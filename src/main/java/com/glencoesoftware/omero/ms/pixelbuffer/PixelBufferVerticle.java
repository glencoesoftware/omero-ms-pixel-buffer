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


import java.util.Optional;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.glencoesoftware.omero.ms.core.OmeroMsAbstractVerticle;
import com.glencoesoftware.omero.ms.core.OmeroRequest;
import com.glencoesoftware.omero.ms.core.PixelsService;

import Glacier2.CannotCreateSessionException;
import Glacier2.PermissionDeniedException;
import brave.ScopedSpan;
import brave.Tracing;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * OMERO thumbnail provider worker verticle. This verticle is designed to be
 * deployed in worker mode and in either a single or multi threaded mode. It
 * acts as a pool of workers to handle blocking thumbnail rendering events
 * dispatched via the Vert.x EventBus.
 * @author Chris Allan <callan@glencoesoftware.com>
 *
 */
public class PixelBufferVerticle extends OmeroMsAbstractVerticle {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(PixelBufferVerticle.class);

    public static final String GET_TILE_EVENT =
            "omero.pixel_buffer.get_tile";

    /** OMERO server pixels service. */
    private final PixelsService pixelsService;

    /** OMERO server host */
    private String host;

    /** OMERO server port */
    private int port;

    /**
     * Default constructor.
     * @param host OMERO server host.
     * @param port OMERO server port.
     */
    public PixelBufferVerticle(PixelsService pixelsService) {
        this.pixelsService = pixelsService;
    }

    /* (non-Javadoc)
     * @see io.vertx.core.AbstractVerticle#start()
     */
    @Override
    public void start() {
        JsonObject omero = config().getJsonObject("omero");
        if (omero == null) {
            throw new IllegalArgumentException(
                "'omero' block missing from configuration");
        }
        host = omero.getString("host");
        port = omero.getInteger("port");

        vertx.eventBus().<String>consumer(
                GET_TILE_EVENT, this::getTile);
    }

    private void getTile(Message<String> message) {
        ObjectMapper mapper = new ObjectMapper();
        TileCtx tileCtx;
        try {
            tileCtx = mapper.readValue(message.body(), TileCtx.class);
        } catch (Exception e) {
            String v = "Illegal tile context";
            log.error(v + ": {}", message.body(), e);
            message.fail(400, v);
            return;
        }
        ScopedSpan span = Tracing.currentTracer().startScopedSpanWithParent(
                "handle_get_tile",
                extractor().extract(tileCtx.traceContext).context());
        span.tag("ctx", message.body());
        tileCtx.injectCurrentTraceContext();
        log.debug("Load tile with data: {}", message.body());
        log.debug("Connecting to the server: {}, {}, {}",
                  host, port, tileCtx.omeroSessionKey);
        try (OmeroRequest request = new OmeroRequest(
                 host, port, tileCtx.omeroSessionKey))
        {
            byte[] tile = request.execute(
                    new TileRequestHandler(pixelsService, tileCtx)::getTile);
            if (tile == null) {
                span.finish();
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
                span.finish();
                message.reply(tile, deliveryOptions);
            }
        } catch (PermissionDeniedException
                | CannotCreateSessionException e) {
            String v = "Permission denied";
            log.debug(v);
            span.error(e);
            message.fail(403, v);
        } catch (IllegalArgumentException e) {
            log.debug("Illegal argument received while retrieving tile", e);
            span.error(e);
            message.fail(400, e.getMessage());
        } catch (Exception e) {
            String v = "Exception while retrieving tile";
            log.error(v, e);
            span.error(e);
            message.fail(500, v);
        }
    }

}
