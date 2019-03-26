/*
 * Copyright (C) 2017 Glencoe Software, Inc. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

package com.glencoesoftware.omero.ms.pixelbuffer;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Optional;
import java.util.function.BiConsumer;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import Glacier2.CannotCreateSessionException;
import Glacier2.PermissionDeniedException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;
import ome.model.annotations.FileAnnotation;
import ome.model.core.OriginalFile;

/**
 * OMERO thumbnail provider worker verticle. This verticle is designed to be
 * deployed in worker mode and in either a single or multi threaded mode. It
 * acts as a pool of workers to handle blocking thumbnail rendering events
 * dispatched via the Vert.x EventBus.
 * @author Chris Allan <callan@glencoesoftware.com>
 *
 */
public class PixelBufferVerticle extends AbstractVerticle {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(PixelBufferVerticle.class);

    public static final String GET_TILE_EVENT =
            "omero.pixel_buffer.get_tile";

    public static final String GET_FILE_ANNOTATION_EVENT =
            "omero.pixel_buffer.get_file_annotation";

    private static final String GET_OBJECT_EVENT =
            "omero.get_object";

    /** OMERO server host */
    private final String host;

    /** OMERO server port */
    private final int port;

    /** OMERO server Spring application context. */
    private ApplicationContext context;

    /**
     * Default constructor.
     * @param host OMERO server host.
     * @param port OMERO server port.
     */
    public PixelBufferVerticle(
            String host, int port, ApplicationContext context) {
        this.host = host;
        this.port = port;
        this.context = context;
    }

    /* (non-Javadoc)
     * @see io.vertx.core.AbstractVerticle#start()
     */
    @Override
    public void start() {
        log.info("Starting verticle");

        vertx.eventBus().<String>consumer(
                GET_TILE_EVENT, this::getTile);

        vertx.eventBus().<String>consumer(
                GET_FILE_ANNOTATION_EVENT, this::getFileAnnotation);
    }

    private void getTile(Message<String> message) {
        StopWatch t0 = new Slf4JStopWatch("getTile");
        ObjectMapper mapper = new ObjectMapper();
        TileCtx tileCtx;
        try {
            tileCtx = mapper.readValue(message.body(), TileCtx.class);
        } catch (Exception e) {
            String v = "Illegal tile context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            t0.stop();
            return;
        }
        log.debug("Load tile with data: {}", message.body());
            log.debug("Connecting to the server: {}, {}, {}",
                      host, port, tileCtx.omeroSessionKey);

        new TileRequestHandler(context, tileCtx, vertx).getTile(tileCtx.omeroSessionKey, tileCtx.imageId)
        .whenComplete(new BiConsumer<byte[], Throwable>() {
            @Override
            public void accept(byte[] tile, Throwable t) {
                if (t != null) {
                    if (t instanceof ReplyException) {
                        // Downstream event handling failure, propagate it
                        t0.stop();
                        message.fail(
                            ((ReplyException) t).failureCode(), t.getMessage());
                    } else {
                        String s = "Internal error";
                        log.error(s, t);
                        t0.stop();
                        message.fail(500, s);
                    }
                } else if (tile == null) {
                    message.fail(
                            404, "Cannot find Image:" + tileCtx.imageId);
                } else {
                    DeliveryOptions deliveryOptions = new DeliveryOptions();
                    deliveryOptions.addHeader(
                        "filename", String.format(
                            "image%d_z%d_c%d_t%d_x%d_y%d_w%d_h%d.%s",
                            tileCtx.imageId, tileCtx.z, tileCtx.c, tileCtx.t,
                            tileCtx.region.getX(),
                            tileCtx.region.getY(),
                            tileCtx.region.getWidth(),
                            tileCtx.region.getHeight(),
                            Optional.ofNullable(tileCtx.format).orElse("bin")
                        )
                    );
                    message.reply(tile, deliveryOptions);
                }
            };
        });
    }

    private <T> T deserialize(AsyncResult<Message<byte[]>> result)
            throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais =
                new ByteArrayInputStream(result.result().body());
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (T) ois.readObject();
    }

    private void getFileAnnotation(Message<String> message) {
        JsonObject messageBody = new JsonObject(message.body());
        String sessionKey = messageBody.getString("sessionKey");
        Long annotationId = messageBody.getLong("annotationId");
        final JsonObject data = new JsonObject();
        data.put("sessionKey", sessionKey);
        data.put("type", "FileAnnotation");
        data.put("id", annotationId);
        vertx.eventBus().<byte[]>send(
                GET_OBJECT_EVENT, data, fileAnnotationResult -> {
            try {
                if (fileAnnotationResult.failed()) {
                    log.info(fileAnnotationResult.cause().getMessage());
                    message.reply("Failed to get annotation");
                    return;
                }
                FileAnnotation fileAnnotation = deserialize(fileAnnotationResult);
                Long fileId = fileAnnotation.getFile().getId();
                final JsonObject getOriginalFileData = new JsonObject();
                getOriginalFileData.put("sessionKey", sessionKey);
                getOriginalFileData.put("type", "OriginalFile");
                getOriginalFileData.put("id", fileId);
                log.info(getOriginalFileData.toString());
                vertx.eventBus().<byte[]>send(
                        GET_OBJECT_EVENT, getOriginalFileData, originalFileResult -> {
                    try {
                        if (originalFileResult.failed()) {
                            log.info(originalFileResult.cause().getMessage());
                            message.reply("Failed to get annotation");
                            return;
                        }
                        OriginalFile of = deserialize(originalFileResult);
                        log.info(of.getId().toString());
                        log.info(of.getPath());
                        log.info(of.getName());
                        String fullFilePath = (of.getPath() + of.getName());
                        message.reply(fullFilePath);
                    } catch (IOException | ClassNotFoundException e) {
                        log.error("Exception while decoding object in response", e);
                        message.fail(404, "Failed to get OriginalFile");
                        return;
                    }
                });
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                message.fail(404, "Failed to get FileAnnotation");
                return;
            }
        });
    }
}
