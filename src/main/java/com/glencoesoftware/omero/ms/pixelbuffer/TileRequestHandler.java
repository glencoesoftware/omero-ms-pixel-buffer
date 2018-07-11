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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import loci.common.ByteArrayHandle;
import loci.common.Location;
import loci.formats.FormatException;
import loci.formats.FormatTools;
import loci.formats.ImageWriter;
import loci.formats.MetadataTools;
import loci.formats.meta.IMetadata;

import ome.xml.model.enums.DimensionOrder;
import ome.xml.model.enums.EnumerationException;
import ome.xml.model.enums.PixelType;
import ome.xml.model.primitives.PositiveInteger;

import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

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
                        IMetadata metadata = createMetadata(pixels);

                        if (format.equals("png") || format.equals("tif")) {
                            return writeImage(format, tileByteBuffer, metadata);
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


    /**
     * Construct a minimal IMetadata instance representing the current tile.
     */
    private IMetadata createMetadata(Pixels pixels) throws EnumerationException {
        IMetadata metadata = MetadataTools.createOMEXMLMetadata();
        metadata.setImageID("Image:0", 0);
        metadata.setPixelsID("Pixels:0", 0);
        metadata.setChannelID("Channel:0:0", 0, 0);
        metadata.setChannelSamplesPerPixel(new PositiveInteger(1), 0, 0);
        metadata.setPixelsBigEndian(true, 0);
        metadata.setPixelsSizeX(new PositiveInteger(tileCtx.region.getWidth()), 0);
        metadata.setPixelsSizeY(new PositiveInteger(tileCtx.region.getHeight()), 0);
        metadata.setPixelsSizeZ(new PositiveInteger(1), 0);
        metadata.setPixelsSizeC(new PositiveInteger(1), 0);
        metadata.setPixelsSizeT(new PositiveInteger(1), 0);
        metadata.setPixelsDimensionOrder(DimensionOrder.XYCZT, 0);
        metadata.setPixelsType(PixelType.fromString(pixels.getPixelsType().getValue().getValue()), 0);
        return metadata;
    }

    /**
     * Write the tile specified by the given buffer and IMetadata to memory.
     * The output format is determined by the extension (e.g. "png", "tif")
     */
    private byte[] writeImage(String extension, ByteBuffer tileBuffer, IMetadata metadata)
            throws FormatException, IOException {
        String id = System.currentTimeMillis() + "." + extension;
        ByteArrayHandle handle = new ByteArrayHandle();
        try (ImageWriter writer = new ImageWriter()) {
            writer.setMetadataRetrieve(metadata);
            Location.mapFile(id, handle);
            writer.setId(id);
            writer.saveBytes(0, tileBuffer.array());

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
                "JOIN FETCH p.image " +
                "JOIN FETCH p.pixelsType " +
                "WHERE p.image.id = :id",
                params, ctx
            );
        } finally {
            t0.stop();
        }
    }

}
