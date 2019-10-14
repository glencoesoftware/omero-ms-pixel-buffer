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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.glencoesoftware.omero.ms.core.OmeroWebJDBCSessionStore;
import com.glencoesoftware.omero.ms.core.OmeroWebRedisSessionStore;
import com.glencoesoftware.omero.ms.core.OmeroWebSessionStore;
import com.glencoesoftware.omero.ms.core.PrometheusSpanHandler;

import brave.ScopedSpan;
import brave.Tracing;
import brave.http.HttpTracing;
import brave.sampler.Sampler;

import com.glencoesoftware.omero.ms.core.OmeroWebSessionRequestHandler;
import com.glencoesoftware.omero.ms.core.OmeroHttpTracingHandler;
import com.glencoesoftware.omero.ms.core.OmeroMsAbstractVerticle;
import com.glencoesoftware.omero.ms.core.OmeroVerticleFactory;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.okhttp3.OkHttpSender;
import io.prometheus.client.vertx.MetricsHandler;

/**
 * Main entry point for the OMERO pixel buffer Vert.x microservice server.
 * @author Chris Allan <callan@glencoesoftware.com>
 *
 */
public class PixelBufferMicroserviceVerticle extends OmeroMsAbstractVerticle {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(PixelBufferMicroserviceVerticle.class);

    /** OMERO.web session store */
    private OmeroWebSessionStore sessionStore;

    /** OMERO server Spring application context. */
    private ApplicationContext context;

    /** VerticleFactory */
    private OmeroVerticleFactory verticleFactory;

    /** Default number of workers to be assigned to the worker verticle */
    private int DEFAULT_WORKER_POOL_SIZE;

    private HttpTracing httpTracing;

    static {
        com.glencoesoftware.omero.ms.core.SSLUtils.fixDisabledAlgorithms();
    }

    /**
     * Entry point method which starts the server event loop and initializes
     * our current OMERO.web session store.
     */
    @Override
    public void start(Promise<Void> prom) {
        log.info("Starting verticle");

        DEFAULT_WORKER_POOL_SIZE =
                Runtime.getRuntime().availableProcessors() * 2;

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
                deploy(ar.result(), prom);
            } catch (Exception e) {
                prom.fail(e);
            }
        });
    }

    /**
     * Deploys our verticles and performs general setup that depends on
     * configuration.
     * @param config Current configuration
     */
    public void deploy(JsonObject config, Promise<Void> prom) {
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

        verticleFactory = (OmeroVerticleFactory)
                context.getBean("omero-ms-verticlefactory");
        vertx.registerVerticleFactory(verticleFactory);

        int workerPoolSize = Optional.ofNullable(
                config.getInteger("worker_pool_size")
                ).orElse(DEFAULT_WORKER_POOL_SIZE);
        vertx.deployVerticle("omero:omero-ms-pixel-buffer-verticle",
                new DeploymentOptions()
                        .setWorker(true)
                        .setInstances(workerPoolSize)
                        .setWorkerPoolName("pixel-buffer-pool")
                        .setWorkerPoolSize(workerPoolSize)
                        .setConfig(config));

        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);

        router.get("/metrics")
            .order(-2)
            .handler(new MetricsHandler());

        try {
            JsonObject httpTracingConfig = config.getJsonObject("http-tracing");
            Boolean tracingEnabled = httpTracingConfig.getBoolean("enabled");
            if(tracingEnabled) {
                String zipkinUrl = httpTracingConfig.getString("zipkin-url");
                OkHttpSender sender = OkHttpSender.create(zipkinUrl);
                AsyncReporter<Span> spanReporter = AsyncReporter.create(sender);
                PrometheusSpanHandler prometheusSpanHandler = new PrometheusSpanHandler();
                Tracing tracing = Tracing.newBuilder()
                    .sampler(Sampler.ALWAYS_SAMPLE)
                    .localServiceName("omero-ms-pixel-buffer")
                    .addFinishedSpanHandler(prometheusSpanHandler)
                    .spanReporter(spanReporter)
                    .build();
                httpTracing = HttpTracing.newBuilder(tracing).build();
                List<String> tags = new ArrayList<String>();
                tags.add("omero.session_key");
                tags.add("omero_ms.request_id");
                Handler<RoutingContext> routingContextHandler =
                        new OmeroHttpTracingHandler(httpTracing, tags);

                /*Set up HttpTracing Routing */
                router.route()
                    .order(-1) // applies before routes
                    .handler(routingContextHandler)
                    .failureHandler(routingContextHandler);
            }
        }
        catch (NullPointerException npe) {
            log.warn("Missing configuration for http tracing");
        }
        catch (Exception e) {
            log.warn("Failed to set up http tracing");
            log.warn(e.toString());
        }

        // Establish a unique identifier for every request
        router.route()
            .handler((event) -> {
            String requestId = UUID.randomUUID().toString();
            event.put("omero_ms.request_id", requestId);
            event.next();
        });

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
                new OmeroWebSessionRequestHandler(config, sessionStore));

        // Pixel buffer request handlers
        router.get(
                "/tile/:imageId/:z/:c/:t")
            .handler(this::getTile);

        int port = config.getInteger("port");
        log.info("Starting HTTP server *:{}", port);
        server.requestHandler(router).listen(port, result -> {
            if (result.succeeded()) {
                prom.complete();
            } else {
                prom.fail(result.cause());
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

        ScopedSpan span =
                Tracing.currentTracer().startScopedSpan("pixel_buffer_ms_verticle_get_tile");
        final HttpServerResponse response = event.response();
        tileCtx.injectCurrentTraceContext();
        vertx.eventBus().<byte[]>request(
                PixelBufferVerticle.GET_TILE_EVENT,
                Json.encode(tileCtx), result -> {
            try {
                if (result.failed()) {
                    Throwable t = result.cause();
                    int statusCode = 404;
                    if (t instanceof ReplyException) {
                        statusCode = ((ReplyException) t).failureCode();
                    }
                    if (statusCode < 1) {
                        log.error("Unexpected failure code {} setting 500 ",
                                  statusCode, t);
                        statusCode = 500;
                    }
                    if (!response.closed()) {
                        response.setStatusCode(statusCode).end();
                    }
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
                if (!response.closed()) {
                    response.end(Buffer.buffer(tile));
                }
            } finally {
                log.debug("Response ended");
                span.finish();
            }
        });
    }

}
