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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.glencoesoftware.omero.ms.core.OmeroWebJDBCSessionStore;
import com.glencoesoftware.omero.ms.core.OmeroWebRedisSessionStore;
import com.glencoesoftware.omero.ms.core.OmeroWebSessionStore;
import com.glencoesoftware.omero.ms.core.OmeroWebSessionRequestHandler;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
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

    /** Directory to save zip files for download */
    private String zipDirectory;

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
        retriever.getConfig(new Handler<AsyncResult<JsonObject>>() {
            @Override
            public void handle(AsyncResult<JsonObject> ar) {
            try {
                deploy(ar.result(), future);
            } catch (Exception e) {
                future.fail(e);
            }
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

        zipDirectory = config.getString("zip-download-path");
        new File(zipDirectory).mkdirs();

        // Set OMERO.server configuration options using system properties
        JsonObject omeroServer = config.getJsonObject("omero.server");
        if (omeroServer == null) {
            throw new IllegalArgumentException(
                    "'omero.server' block missing from configuration");
        }
        for(Map.Entry<String, Object> entry : omeroServer) {
            System.setProperty(entry.getKey(), (String) entry.getValue());
        }

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
                omero.getString("host"),
                omero.getInteger("port"),
                omeroServer.getString("omero.data.dir"),
                context),
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
        JsonObject sessionStoreConfig = config.getJsonObject("session-store");
        if (sessionStoreConfig == null) {
            throw new IllegalArgumentException(
                    "'session-store' block missing from configuration");
        }
        String sessionStoreType = sessionStoreConfig.getString("type");
        String sessionStoreUri = sessionStoreConfig.getString("uri");
        if (sessionStoreType.equals("redis")) {
            sessionStore = new OmeroWebRedisSessionStore(sessionStoreUri);
        } else if (sessionStoreType.equals("postgres")) {
            sessionStore = new OmeroWebJDBCSessionStore(
                sessionStoreUri,
                vertx);
        } else {
            throw new IllegalArgumentException(
                "Missing/invalid value for 'session-store.type' in config");
        }

        // Get PixelBuffer Microservice Information
        router.options().handler(this::getMicroserviceDetails);

        router.route().handler(
                new OmeroWebSessionRequestHandler(config, sessionStore, vertx));

        // Pixel buffer request handlers
        router.get(
                "/tile/:imageId/:z/:c/:t")
            .handler(this::getTile);

        router.get(
                "/annotation/:annotationId")
            .handler(this::getFileAnnotation);

        router.get(
                "/file/:fileId")
            .handler(this::getOriginalFile);

        router.get(
                "/zip/:imageId")
            .handler(this::getZippedFiles);

        int port = config.getInteger("port");
        log.info("Starting HTTP server *:{}", port);
        server.requestHandler(router::accept).listen(port,
            new Handler<AsyncResult<HttpServer>>() {
                @Override
                public void handle(AsyncResult<HttpServer> result) {
                    if (result.succeeded()) {
                        future.complete();
                    } else {
                        future.fail(result.cause());
                    }
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
     * Get information about microservice.
     * Confirms that this is a microservice
     * @param event Current routing context.
     */
    private void getMicroserviceDetails(RoutingContext event) {
        log.info("Getting Microservice Details");
        String version = Optional.ofNullable(
            this.getClass().getPackage().getImplementationVersion())
            .orElse("development");
        JsonObject resData = new JsonObject()
                        .put("provider", "PixelBufferMicroservice")
                        .put("version", version)
                        .put("features", new JsonArray());
        event.response()
            .putHeader("content-type", "application-json")
            .end(resData.encodePrettily());
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
            Json.encode(tileCtx), new Handler<AsyncResult<Message<byte[]>>>() {
                @Override
                public void handle(AsyncResult<Message<byte[]>> result) {
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
                }
            }
        );
    }

    private void getFileAnnotation(RoutingContext event) {
        log.info("Get File Annotation");
        HttpServerRequest request = event.request();
        HttpServerResponse response = event.response();
        String sessionKey = event.get("omero.session_key");
        JsonObject data = new JsonObject();
        data.put("sessionKey", sessionKey);
        data.put("annotationId", Long.parseLong(request.getParam("annotationId")));
        vertx.eventBus().<String>send(
                PixelBufferVerticle.GET_FILE_ANNOTATION_EVENT,
                data.toString(), new Handler<AsyncResult<Message<String>>>() {
                    @Override
                    public void handle(AsyncResult<Message<String>> result) {
                        if (result.failed()) {
                            log.error(result.cause().getMessage());
                            response.setStatusCode(404);
                            response.end("Could not get annotation "
                                        + request.getParam("annotationId"));
                            return;
                        }
                        String filePath = result.result().body();
                        log.info(filePath);
                        String[] pathComponents = filePath.split("/");
                        String fileName = pathComponents[pathComponents.length -1];
                        response.headers().set("Content-Type", "application/octet-stream");
                        response.headers().set("Content-Disposition",
                                "attachment; filename=\"" + fileName + "\"");
                        response.sendFile(filePath);
                    }
                });

    }

    private void getOriginalFile(RoutingContext event) {
        log.info("Get Original File");
        HttpServerRequest request = event.request();
        HttpServerResponse response = event.response();
        String sessionKey = event.get("omero.session_key");
        JsonObject data = new JsonObject();
        data.put("sessionKey", sessionKey);
        data.put("fileId", Long.parseLong(request.getParam("fileId")));
        vertx.eventBus().<JsonObject>send(
            PixelBufferVerticle.GET_ORIGINAL_FILE_EVENT,
            data, new Handler<AsyncResult<Message<JsonObject>>>() {
                @Override
                public void handle(AsyncResult<Message<JsonObject>> result) {
                    if (result.failed()) {
                        log.error(result.cause().getMessage());
                        response.setStatusCode(404);
                        response.end("Could not get original file "
                                    + request.getParam("fileId"));
                        return;
                    }
                    JsonObject resultBody = result.result().body();
                    String filePath = resultBody.getString("filePath");
                    String fileName = resultBody.getString("fileName");
                    String mimeType = resultBody.getString("mimeType");
                    log.info(filePath);
                    log.info(fileName);
                    log.info(mimeType);
                    response.headers().set("Content-Type", mimeType);
                    response.headers().set("Content-Disposition",
                            "attachment; filename=\"" + fileName + "\"");
                    response.sendFile(filePath);
                }
            }
        );
    }

    private void getZippedFiles(RoutingContext event) {
        log.info("Get Zipped Files");
        HttpServerRequest request = event.request();
        HttpServerResponse response = event.response();
        String sessionKey = event.get("omero.session_key");
        JsonObject data = new JsonObject();
        data.put("sessionKey", sessionKey);
        data.put("imageId", Long.parseLong(request.getParam("imageId")));
        data.put("zipDirectory", zipDirectory);
        vertx.eventBus().<JsonObject>send(
            PixelBufferVerticle.GET_ZIPPED_FILES_EVENT,
            data, new Handler<AsyncResult<Message<JsonObject>>>() {
                @Override
                public void handle(AsyncResult<Message<JsonObject>> result) {
                    if (result.failed()) {
                        log.error(result.cause().getMessage());
                        response.setStatusCode(404);
                        response.end("Could not get zipped files "
                                    + request.getParam("annotationId"));
                        return;
                    }
                    JsonObject resultBody = result.result().body();
                    String filePath = resultBody.getString("filePath");
                    String fileName = resultBody.getString("fileName");
                    String mimeType = resultBody.getString("mimeType");
                    log.info(filePath);
                    log.info(fileName);
                    log.info(mimeType);
                    response.headers().set("Content-Type", mimeType);
                    response.headers().set("Content-Disposition",
                            "attachment; filename=\"" + fileName + "\"");
                    response.sendFile(filePath, new Handler<AsyncResult<Void>>() {
                        public void handle(AsyncResult<Void> result) {
                            File zipFile = new File(filePath);
                            log.info("Attempting to delete: " + zipFile.getAbsolutePath());
                            if (!zipFile.delete()) {
                                log.error("Failed to delete file " + filePath);
                            }
                        }
                    });
                }
            }
        );
    }

}
