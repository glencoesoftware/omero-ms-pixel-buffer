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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import ome.model.annotations.FileAnnotation;
import ome.model.core.Image;
import ome.model.core.OriginalFile;
import ome.model.fs.Fileset;
import ome.model.fs.FilesetEntry;
import ome.io.nio.FileBuffer;
import ome.io.nio.OriginalFilesService;


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

    public static final String GET_ORIGINAL_FILE_EVENT =
            "omero.pixel_buffer.get_original_file";

    public static final String GET_ZIPPED_FILES_EVENT =
            "omero.pixel_buffer.get_zipped_files";

    private static final String GET_OBJECT_EVENT =
            "omero.get_object";

    public static final String GET_FILE_PATH_EVENT =
            "omero.get_file_path";

    public static final String GET_IMPORTED_IMAGE_FILES_EVENT =
            "omero.get_imported_image_files";

    public static final String GET_ORIGINAL_FILE_PATHS_EVENT =
            "omero.get_original_file_paths";

    /** OMERO server host */
    private final String host;

    /** OMERO server port */
    private final int port;

    /** OMERO server Spring application context. */
    private ApplicationContext context;

    /** Original File Service for getting paths */
    private OriginalFilesService ioService;

    /**
     * Default constructor.
     * @param host OMERO server host.
     * @param port OMERO server port.
     */
    public PixelBufferVerticle(
            String host, int port, String omeroDataDir, ApplicationContext context) {
        this.host = host;
        this.port = port;
        this.context = context;
        this.ioService = new OriginalFilesService(omeroDataDir, true);
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

        vertx.eventBus().<JsonObject>consumer(
                GET_ORIGINAL_FILE_EVENT, this::getOriginalFile);

        vertx.eventBus().<JsonObject>consumer(
                GET_ZIPPED_FILES_EVENT, this::getZippedFiles);
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
                    log.error(fileAnnotationResult.cause().getMessage());
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
                        FileBuffer fBuffer = ioService.getFileBuffer(of, "r");
                        log.info(fBuffer.getPath());
                        message.reply(fBuffer.getPath());
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

    private void getOriginalFile(Message<JsonObject> message) {
        JsonObject messageBody = message.body();
        String sessionKey = messageBody.getString("sessionKey");
        Long fileId = messageBody.getLong("fileId");
        log.debug("Session key: " + sessionKey);
        log.debug("File ID: {}", fileId);

        final JsonObject data = new JsonObject();
        data.put("sessionKey", sessionKey);
        data.put("id", fileId);
        data.put("type", "OriginalFile");
        vertx.eventBus().<byte[]>send(
                GET_FILE_PATH_EVENT, data, filePathResult -> {
            try {
                if (filePathResult.failed()) {
                    log.error(filePathResult.cause().getMessage());
                    message.fail(404, "Failed to get file path");
                    return;
                }
                final String filePath = deserialize(filePathResult);
                vertx.eventBus().<byte[]>send(
                        GET_OBJECT_EVENT, data, getOriginalFileResult -> {
                    try {
                        if (getOriginalFileResult.failed()) {
                            log.error(getOriginalFileResult.cause().getMessage());
                            message.fail(404, "Failed to get original file");
                            return;
                        }
                        OriginalFile of = deserialize(getOriginalFileResult);
                        String fileName = of.getName();
                        String mimeType = of.getMimetype();
                        log.info(fileName);
                        log.info(mimeType);
                        JsonObject response = new JsonObject();
                        response.put("filePath", filePath);
                        response.put("fileName", fileName);
                        response.put("mimeType", mimeType);
                        message.reply(response);
                    } catch (IOException | ClassNotFoundException e) {
                        log.error("Exception while decoding object in response", e);
                        message.fail(404, "Error decoding object");
                    }
                });
            } catch (IOException | ClassNotFoundException e) {
                log.error("Exception while decoding object in response", e);
                message.fail(404, "Error decoding file path object");
            }
        });
    }

    private boolean createZip(String fullZipPath, List<String> filePaths) throws FileNotFoundException {
        FileOutputStream fos = new FileOutputStream(fullZipPath);
        ZipOutputStream zos = new ZipOutputStream(fos);
        try {
        for (String fpath : filePaths) {
                File f = new File(fpath);
                ZipEntry entry = new ZipEntry(f.getName());
                zos.putNextEntry(entry);
                FileInputStream fis = new FileInputStream(f);

                byte[] bytes = new byte[1024];
                int length;
                while((length = fis.read(bytes)) >= 0) {
                    zos.write(bytes, 0, length);
                }
                fis.close();
        }
        zos.close();
        fos.close();
        } catch (IOException e) {
            log.error("Failure during zip: " + e.getMessage());
            return false;
        }
        return true;
    }

    private void getZippedFiles(Message<JsonObject> message) {
        JsonObject messageBody = message.body();
        String sessionKey = messageBody.getString("sessionKey");
        Long imageId = messageBody.getLong("imageId");
        final String zipDirectory = messageBody.getString("zipDirectory");
        log.debug("Session key: " + sessionKey);
        log.debug("Image ID: {}", imageId);

        //Get image
        final JsonObject data = new JsonObject();
        data.put("sessionKey", sessionKey);
        data.put("imageId", imageId);
        vertx.eventBus().<byte[]>send(GET_IMPORTED_IMAGE_FILES_EVENT, data, getImportedFilesResult -> {
            try {
                if (getImportedFilesResult.failed()) {
                    log.error(getImportedFilesResult.cause().getMessage());
                    message.reply("Failed to get image");
                    return;
                }
                List<OriginalFile> originalFiles = deserialize(getImportedFilesResult);
                log.info(String.valueOf(originalFiles.size()));
                JsonObject getOriginalFilePathsData = new JsonObject();
                JsonArray fileIds = new JsonArray();
                for (OriginalFile of : originalFiles) {
                    fileIds.add(of.getId());
                }
                //FilesetEntry fsEntry = fsEntryIter.next();
                final JsonObject filepathData = new JsonObject();
                filepathData.put("sessionKey", sessionKey);
                filepathData.put("originalFileIds", fileIds);
                log.info("Getting OriginalFile Paths");
                vertx.eventBus().<byte[]>send(
                        GET_ORIGINAL_FILE_PATHS_EVENT, filepathData, filePathResult -> {
                    try {
                        if (filePathResult.failed()) {
                            log.error(filePathResult.cause().getMessage());
                            message.fail(404, "Failed to get file path");
                            return;
                        }
                        final List<String> filePaths = deserialize(filePathResult);
                        for (String fp : filePaths) {
                            log.info(fp);
                        }
                        String zipName = "image" + imageId.toString() + ".zip";
                        String zipFullPath = zipDirectory + "/" + zipName;
                        File zipFile = new File(zipFullPath);
                        int fileIndex = 1;
                        while (zipFile.exists()) {
                            log.info("Zip name collision: " + zipFullPath);
                            zipName = "image" + imageId.toString() + "_" + String.valueOf(fileIndex) + ".zip";
                            zipFullPath = zipDirectory + "/" + zipName;
                            zipFile = new File(zipFullPath);
                        }
                        boolean success = createZip(zipFullPath, filePaths);
                        if (success) {
                            JsonObject pathObj = new JsonObject();
                            pathObj.put("filePath", zipFullPath);
                            pathObj.put("fileName", zipName);
                            pathObj.put("mimeType", "application/zip");
                            message.reply(pathObj);
                        }
                        else {
                            message.fail(404, "Error creating zip");
                        }
                    } catch (IOException | ClassNotFoundException e) {
                        log.error("Exception while decoding object in response", e);
                        message.fail(404, "Error decoding file path object");
                    }
                });
            } catch (IOException | ClassNotFoundException e) {

            }
        });
    }
}
