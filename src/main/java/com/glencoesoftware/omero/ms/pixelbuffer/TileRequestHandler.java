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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import loci.common.ByteArrayHandle;
import loci.common.Location;
import loci.formats.FormatException;
import loci.formats.ImageWriter;
import loci.formats.MetadataTools;
import loci.formats.meta.IMetadata;

import ome.xml.model.enums.DimensionOrder;
import ome.xml.model.enums.EnumerationException;
import ome.xml.model.enums.PixelType;
import ome.xml.model.primitives.PositiveInteger;
import omeis.providers.re.data.RegionDef;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import ome.io.nio.PixelBuffer;
import ome.io.nio.PixelsService;
import ome.model.core.Pixels;
import ome.model.core.Image;
import omero.ApiUsageException;
import omero.ServerError;

public class TileRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(TileRequestHandler.class);

    private static final String GET_PIXELS_DESCRIPTION_EVENT = "omero.get_pixels_description";

    /** OMERO server Spring application context. */
    private final ApplicationContext context;

    /** OMERO server pixels service. */
    private final PixelsService pixelsService;

    /** Tile Context */
    private final TileCtx tileCtx;

    /** Handle on vertx instance */
    Vertx vertx;

    /**
     * Default constructor.
     * @param tileCtx {@link TileCtx} object
     */
    public TileRequestHandler(ApplicationContext context,
            TileCtx tileCtx,
            Vertx vertx) {
        log.info("Setting up handler");
        this.context = context;
        pixelsService = (PixelsService) context.getBean("/OMERO/Pixels");
        this.tileCtx = tileCtx;
        this.vertx = vertx;
    }

    public CompletableFuture<byte[]> getTile(String omeroSessionKey, Long imageId) {
        log.info("In TileRequestHandler::getTIle");
        StopWatch t0 = new Slf4JStopWatch("getTile");
        CompletableFuture<byte[]> promise = new CompletableFuture<byte[]>();
        getPixels(omeroSessionKey, tileCtx.imageId)
        .thenAccept(pixels -> {
            log.info("In getPixels callback");
            if (pixels != null) {
                try (PixelBuffer pixelBuffer = pixelsService.getPixelBuffer(pixels, false)) {
                    String format = tileCtx.format;
                    RegionDef region = tileCtx.region;
                    if (tileCtx.resolution != null) {
                        pixelBuffer.setResolutionLevel(tileCtx.resolution);
                    }
                    if (region.getWidth() == 0) {
                        region.setWidth(pixels.getSizeX());
                    }
                    if (region.getHeight() == 0) {
                        region.setHeight(pixels.getSizeY());
                    }
                    int width = region.getWidth();
                    int height = region.getHeight();
                    int bytesPerPixel =
                            pixels.getPixelsType().getBitSize() / 8;
                    int tileSize = width * height * bytesPerPixel;
                    byte[] tile = new byte[tileSize];
                    pixelBuffer.getTileDirect(
                        tileCtx.z, tileCtx.c, tileCtx.t,
                        region.getX(), region.getY(), width, height, tile);

                    log.debug(
                            "Image:{}, z: {}, c: {}, t: {}, resolution: {}, " +
                            "region: {}, format: {}",
                            tileCtx.imageId, tileCtx.z, tileCtx.c, tileCtx.t,
                            tileCtx.resolution, region, format);
                    if (format != null) {
                        IMetadata metadata = createMetadata(pixels);

                        if (format.equals("png") || format.equals("tif")) {
                            promise.complete(writeImage(format, tile, metadata));
                        } else {
                            log.error("Unknown output format: {}", format);
                            promise.complete(null);
                        }
                    }
                    promise.complete(tile);
                }
                catch(IOException | EnumerationException
                        | FormatException e) {
                    promise.completeExceptionally(e);
                }
            } else {
                log.debug("Cannot find Image:{}", tileCtx.imageId);
                promise.complete(null);
            }
        });
        return promise;
    }


    /**
     * Construct a minimal IMetadata instance representing the current tile.
     */
    private IMetadata createMetadata(Pixels pixels)
            throws EnumerationException {
        IMetadata metadata = MetadataTools.createOMEXMLMetadata();
        metadata.setImageID("Image:0", 0);
        metadata.setPixelsID("Pixels:0", 0);
        metadata.setChannelID("Channel:0:0", 0, 0);
        metadata.setChannelSamplesPerPixel(new PositiveInteger(1), 0, 0);
        metadata.setPixelsBigEndian(true, 0);
        metadata.setPixelsSizeX(
                new PositiveInteger(tileCtx.region.getWidth()), 0);
        metadata.setPixelsSizeY(
                new PositiveInteger(tileCtx.region.getHeight()), 0);
        metadata.setPixelsSizeZ(new PositiveInteger(1), 0);
        metadata.setPixelsSizeC(new PositiveInteger(1), 0);
        metadata.setPixelsSizeT(new PositiveInteger(1), 0);
        metadata.setPixelsDimensionOrder(DimensionOrder.XYCZT, 0);
        metadata.setPixelsType(PixelType.fromString(
                pixels.getPixelsType().getValue()), 0);
        return metadata;
    }

    /**
     * Write the tile specified by the given buffer and IMetadata to memory.
     * The output format is determined by the extension (e.g. "png", "tif")
     */
    private byte[] writeImage(String extension, byte[] tile, IMetadata metadata)
            throws FormatException, IOException {
        String id = System.currentTimeMillis() + "." + extension;
        ByteArrayHandle handle = new ByteArrayHandle();
        try (ImageWriter writer = new ImageWriter()) {
            writer.setMetadataRetrieve(metadata);
            Location.mapFile(id, handle);
            writer.setId(id);
            writer.saveBytes(0, tile);

            // trim byte array to written length (not backing array length)
            ByteBuffer bytes = handle.getByteBuffer();
            byte[] file = new byte[(int) handle.length()];
            bytes.position(0);
            bytes.get(file);
            return file;
        } finally {
            Location.mapFile(id, null);
            handle.close();
        }
    }

    protected CompletableFuture<Pixels> getPixels(String omeroSessionKey, Long imageId){
        log.info("In getPixels");
        CompletableFuture<Pixels> promise = new CompletableFuture<Pixels>();

        final JsonObject data = new JsonObject();
        data.put("sessionKey", omeroSessionKey);
        data.put("imageId", imageId);
        vertx.eventBus().<byte[]>send(
                GET_PIXELS_DESCRIPTION_EVENT, data, result -> {
            log.info("In backbone response");
            String s = "";
            try {
                if (result.failed()) {
                    promise.completeExceptionally(result.cause());
                    return;
                }
                ByteArrayInputStream bais =
                        new ByteArrayInputStream(result.result().body());
                ObjectInputStream ois = new ObjectInputStream(bais);
                Pixels pixels = (Pixels) ois.readObject();
                promise.complete(pixels);
            } catch (IOException | ClassNotFoundException e) {
                promise.completeExceptionally(e);
                log.error("Exception while decoding object in response", e);
            }
        });

        return promise;
    }
}
