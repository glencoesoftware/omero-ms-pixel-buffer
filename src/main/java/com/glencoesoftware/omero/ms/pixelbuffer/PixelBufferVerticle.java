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
import org.springframework.context.ApplicationContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import Glacier2.CannotCreateSessionException;
import Glacier2.PermissionDeniedException;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;

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
        log.debug("Load tile with data: {}", message.body());
            log.debug("Connecting to the server: {}, {}, {}",
                      host, port, tileCtx.omeroSessionKey);

            new TileRequestHandler(context, tileCtx, vertx).getTile(tileCtx.omeroSessionKey, tileCtx.imageId)
            .whenComplete((tile, t) -> {
                if(t != null) {
                    //TODO: Do something
                }
                log.info("After TileRequestHandler");
                if (tile == null) {
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
            });
    }

}
