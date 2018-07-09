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

import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.glencoesoftware.omero.ms.core.OmeroWebRedisSessionStore;
import com.glencoesoftware.omero.ms.core.OmeroWebSessionStore;
import com.glencoesoftware.omero.ms.core.OmeroWebSessionRequestHandler;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CookieHandler;

/**
 * Main entry point for the OMERO pixel buffer Vert.x microservice server.
 * @author Chris Allan <callan@glencoesoftware.com>
 *
 */
public class PixelBufferMicroserviceVerticle extends AbstractVerticle {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(PixelBufferMicroserviceVerticle.class);

    /** OMERO.web session store */
    private OmeroWebSessionStore sessionStore;

    /** OMERO server Spring application context. */
    private ApplicationContext context;

    /**
     * Entry point method which starts the server event loop and initializes
     * our current OMERO.web session store.
     */
    @Override
    public void start(Future<Void> future) {
        log.info("Starting verticle");

        ConfigStoreOptions store = new ConfigStoreOptions()
                .setType("file")
                .setFormat("yaml")
                .setConfig(new JsonObject()
                        .put("path", "conf/config.yaml")
                )
                .setOptional(true);
        ConfigRetriever retriever = ConfigRetriever.create(
                vertx, new ConfigRetrieverOptions()
                        .setIncludeDefaultStores(true)
                        .addStore(store));
        retriever.getConfig(ar -> {
            try {
                deploy(ar.result(), future);
            } catch (Exception e) {
                future.fail(e);
            }
        });
    }

    /**
     * Deploys our verticles and performs general setup that depends on
     * configuration.
     * @param config Current configuration
     */
    public void deploy(JsonObject config, Future<Void> future) {
        log.info("Deploying verticle");

        // Set OMERO.server configuration options using system properties
        JsonObject omeroServer = config.getJsonObject("omero.server");
        if (omeroServer == null) {
            throw new IllegalArgumentException(
                    "'omero.server' block missing from configuration");
        }
        omeroServer.forEach(entry -> {
            System.setProperty(entry.getKey(), (String) entry.getValue());
        });

        context = new ClassPathXmlApplicationContext(
                "classpath:ome/config.xml",
                "classpath:ome/services/datalayer.xml",
                "classpath*:beanRefContext.xml");

        // Deploy our dependency verticles
        JsonObject omero = config.getJsonObject("omero");
        if (omero == null) {
            throw new IllegalArgumentException(
                    "'omero' block missing from configuration");
        }
        vertx.deployVerticle(new PixelBufferVerticle(
                omero.getString("host"), omero.getInteger("port"), context),
                new DeploymentOptions()
                        .setWorker(true)
                        .setMultiThreaded(true)
                        .setConfig(config));

        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        // Cookie handler so we can pick up the OMERO.web session
        router.route().handler(CookieHandler.create());

        // OMERO session handler which picks up the session key from the
        // OMERO.web session and joins it.
        JsonObject redis = config.getJsonObject("redis");
        if (redis == null) {
            throw new IllegalArgumentException(
                    "'redis' block missing from configuration");
        }
        sessionStore = new OmeroWebRedisSessionStore(redis.getString("uri"));
        router.route().handler(
                new OmeroWebSessionRequestHandler(config, sessionStore));

        // Pixel buffer request handlers
        router.get(
                "/tile/:imageId/:z/:c/:t")
            .handler(this::getTile);

        int port = config.getInteger("port");
        log.info("Starting HTTP server *:{}", port);
        server.requestHandler(router::accept).listen(port, result -> {
            if (result.succeeded()) {
                future.complete();
            } else {
                future.fail(result.cause());
            }
        });
    }

    /**
     * Exit point method which when the verticle stops, cleans up our current
     * OMERO.web session store.
     */
    @Override
    public void stop() throws Exception {
        sessionStore.close();
    }

    /**
     * Tile retrieval event handler.
     * Responds with a <code>application/octet-stream</code> body on success
     * based on the <code>pixelsId</code>, <code>z</code>, <code>c</code>,
     * and <code>t</code> encoded in the URL or HTTP 404 if the {@link Pixels}
     * does not exist or the user does not have permissions to access it.
     * @param event Current routing context.
     */
    private void getTile(RoutingContext event) {
        log.info("Get tile");
        HttpServerRequest request = event.request();
        TileCtx tileCtx = new TileCtx(
                request.params(), event.get("omero.session_key"));

        final HttpServerResponse response = event.response();
        vertx.eventBus().<byte[]>send(
                PixelBufferVerticle.GET_TILE_EVENT,
                Json.encode(tileCtx), result -> {
            try {
                if (result.failed()) {
                    Throwable t = result.cause();
                    int statusCode = 404;
                    if (t instanceof ReplyException) {
                        statusCode = ((ReplyException) t).failureCode();
                    }
                    response.setStatusCode(statusCode);
                    return;
                }
                byte[] tile = result.result().body();
                String contentType = "application/octet-stream";
                if ("png".equals(tileCtx.format)) {
                    contentType = "image/png";
                }
                if ("tif".equals(tileCtx.format)) {
                    contentType = "image/tiff";
                }
                response.headers().set(
                        "Content-Type", contentType);
                response.headers().set(
                        "Content-Length",
                        String.valueOf(tile.length));
                response.headers().set(
                        "Content-Disposition",
                        String.format(
                                "attachment; filename=\"%s\"",
                                result.result().headers().get("filename")));
                response.write(Buffer.buffer(tile));
            } finally {
                response.end();
                log.debug("Response ended");
            }
        });
    }

}
