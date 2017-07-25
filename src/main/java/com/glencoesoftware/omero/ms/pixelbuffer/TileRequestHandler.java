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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

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
            Pixels pixels = getPixels(client, tileCtx.pixelsId);
            if (pixels != null) {
                try (PixelBuffer pixelBuffer = getPixelBuffer(pixels)) {
                    ByteBuffer tileByteBuffer = pixelBuffer.getTile(
                        tileCtx.z, tileCtx.c, tileCtx.t,
                        tileCtx.region.getX(), tileCtx.region.getY(),
                        tileCtx.region.getWidth(), tileCtx.region.getHeight())
                            .getData();
                    byte[] tile = new byte[tileByteBuffer.capacity()];
                    tileByteBuffer.get(tile);
                    return tile;
                }
            } else {
                log.debug("Cannot find Pixels:{}", tileCtx.pixelsId);
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
     * Retrieves a single {@link Image} from the server.
     * @param client OMERO client to use for querying.
     * @param pixelsId {@link Pixels} identifier to query for.
     * @return Loaded {@link Pixels} or <code>null</code> if it does not exist.
     * @throws ServerError If there was any sort of error retrieving the pixels.
     */
    protected Pixels getPixels(omero.client client, Long pixelsId)
            throws ServerError {
        Map<String, String> ctx = new HashMap<String, String>();
        ctx.put("omero.group", "-1");
        ParametersI params = new ParametersI();
        params.addId(pixelsId);
        StopWatch t0 = new Slf4JStopWatch("getPixels");
        try {
            return (Pixels) client.getSession().getQueryService().findByQuery(
                "SELECT p FROM Pixels as p " +
                "JOIN FETCH p.pixelsType " +
                "WHERE p.id = :id",
                params, ctx
            );
        } finally {
            t0.stop();
        }
    }

}
