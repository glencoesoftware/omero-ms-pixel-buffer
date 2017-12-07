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

import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferUShort;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ServiceRegistry;
import javax.imageio.stream.ImageOutputStream;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.sun.media.imageioimpl.plugins.tiff.TIFFImageWriter;
import com.sun.media.imageioimpl.plugins.tiff.TIFFImageWriterSpi;

import ome.io.nio.PixelBuffer;
import ome.io.nio.PixelsService;
import omero.ApiUsageException;
import omero.ServerError;
import omero.model.Image;
import omero.model.Pixels;
import omero.sys.ParametersI;
import omero.util.IceMapper;

import static omero.rtypes.unwrap;

public class TileRequestHandler {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(TileRequestHandler.class);

    /** OMERO server Spring application context. */
    private final ApplicationContext context;

    /** OMERO server pixels service. */
    private final PixelsService pixelsService;

    /** Tile Context */
    private final TileCtx tileCtx;

    /**
     * Mapper between <code>omero.model</code> client side Ice backed objects
     * and <code>ome.model</code> server side Hibernate backed objects. 
     */
    private final IceMapper mapper = new IceMapper();

    /**
     * Default constructor.
     * @param tileCtx {@link TileCtx} object
     */
    public TileRequestHandler(ApplicationContext context, TileCtx tileCtx) {
        log.info("Setting up handler");
        this.context = context;
        pixelsService = (PixelsService) context.getBean("/OMERO/Pixels"); 
        this.tileCtx = tileCtx;
    }

    public byte[] getTile(omero.client client) {
        StopWatch t0 = new Slf4JStopWatch("getTile");
        try {
            Pixels pixels = getPixels(client, tileCtx.imageId);
            if (pixels != null) {
                try (PixelBuffer pixelBuffer = getPixelBuffer(pixels)) {
                    String format = tileCtx.format;
                    if (tileCtx.resolution != null) {
                        pixelBuffer.setResolutionLevel(tileCtx.resolution);
                    }
                    if (tileCtx.region.getWidth() == 0) {
                        tileCtx.region.setWidth(pixels.getSizeX().getValue());
                    }
                    if (tileCtx.region.getHeight() == 0) {
                        tileCtx.region.setHeight(pixels.getSizeY().getValue());
                    }
                    ByteBuffer tileByteBuffer = pixelBuffer.getTile(
                        tileCtx.z, tileCtx.c, tileCtx.t,
                        tileCtx.region.getX(), tileCtx.region.getY(),
                        tileCtx.region.getWidth(), tileCtx.region.getHeight())
                            .getData();

                    log.debug(
                            "Image:{}, z: {}, c: {}, t: {}, resolution: {}, " +
                            "region: {}, format: {}",
                            tileCtx.imageId, tileCtx.z, tileCtx.c, tileCtx.t,
                            tileCtx.resolution, tileCtx.region, format);
                    if (format != null) {
                        ByteArrayOutputStream output =
                                new ByteArrayOutputStream();
                        BufferedImage image =
                                createBufferedImage(pixels, tileByteBuffer);
                        if (image == null) {
                            return null;
                        }

                        if (format.equals("png")) {
                            ImageIO.write(image, "png", output);
                            return output.toByteArray();
                        }
                        if (format.equals("tif")) {
                            writeTiff(image, output);
                            return output.toByteArray();
                        }
                        log.error("Unknown output format: {}", format);
                        return null;
                    }
                    byte[] tile = new byte[tileByteBuffer.capacity()];
                    tileByteBuffer.get(tile);
                    return tile;
                }
            } else {
                log.debug("Cannot find Image:{}", tileCtx.imageId);
            }
        } catch (Exception e) {
            log.error("Exception while retrieving tile", e);
        } finally {
            t0.stop();
        }
        return null;
    }

    protected PixelBuffer getPixelBuffer(Pixels pixels)
            throws ApiUsageException {
        StopWatch t0 = new Slf4JStopWatch("getPixelBuffer");
        try {
            return pixelsService.getPixelBuffer(
                    (ome.model.core.Pixels) mapper.reverse(pixels), false);
        } finally {
            t0.stop();
        }
    }

    /**
     * Retrieves a single {@link Pixels} from the server.
     * @param client OMERO client to use for querying.
     * @param imageId {@link Image} identifier to query for.
     * @return Loaded {@link Pixels} or <code>null</code> if it does not exist.
     * @throws ServerError If there was any sort of error retrieving the pixels.
     */
    protected Pixels getPixels(omero.client client, Long imageId)
            throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(imageId);
        StopWatch t0 = new Slf4JStopWatch("getPixels");
        try {
            return (Pixels) client.getSession().getQueryService().findByQuery(
                "SELECT p FROM Pixels as p " +
                "JOIN FETCH p.pixelsType " +
                "WHERE p.image.id = :id",
                params, ctx
            );
        } finally {
            t0.stop();
        }
    }

    /**
     * Create a buffered image for a given tile
     * @param pixels metadata to use when creating the buffered image
     * @param tileByteBuffer data to create the buffered image with
     * @return See above.
     */
    private BufferedImage createBufferedImage(
            Pixels pixels, ByteBuffer tileByteBuffer) {
        BufferedImage image = null;
        String pixelsType = (String) unwrap(pixels.getPixelsType().getValue());
        if (pixelsType.endsWith("int8")) {
            log.debug(
                "Mapping pixels type {} to BufferedImage.TYPE_BYTE_GRAY",
                pixelsType);
            image = new BufferedImage(
                    tileCtx.region.getWidth(), tileCtx.region.getHeight(),
                    BufferedImage.TYPE_BYTE_GRAY);
            byte[] dataBuffer = ((DataBufferByte) image.getRaster()
                    .getDataBuffer()).getData();
            tileByteBuffer.get(dataBuffer);
            return image;
        }
        if (pixelsType.endsWith("int16")) {
            log.debug(
                "Mapping pixels type {} to BufferedImage.TYPE_USHORT_GRAY",
                pixelsType);
            image = new BufferedImage(
                    tileCtx.region.getWidth(), tileCtx.region.getHeight(),
                    BufferedImage.TYPE_USHORT_GRAY);
            short[] dataBuffer = ((DataBufferUShort) image.getRaster()
                    .getDataBuffer()).getData();
            tileByteBuffer.asShortBuffer().get(dataBuffer);
            return image;
        }
        log.error("Unsupported pixel type: {}", pixelsType);
        return null;
    }

    /**
     * Writes a buffered image to a TIFF output stream.
     * @param image buffered image to write out as a TIFF
     * @param output output stream to write to
     * @throws IOException If there is an error writing to
     * <code>output</code>.
     */
    private void writeTiff(BufferedImage image, OutputStream output)
            throws IOException {
        try (ImageOutputStream ios =
                ImageIO.createImageOutputStream(output)) {
            IIORegistry registry = IIORegistry.getDefaultInstance();
            registry.registerServiceProviders(
                    ServiceRegistry.lookupProviders(
                            TIFFImageWriterSpi.class));
            TIFFImageWriterSpi spi = registry.getServiceProviderByClass(
                    TIFFImageWriterSpi.class);
            TIFFImageWriter writer = new TIFFImageWriter(spi);
            writer.setOutput(ios);
            writer.write(null, new IIOImage(image, null, null), null);
        }
    }
}
