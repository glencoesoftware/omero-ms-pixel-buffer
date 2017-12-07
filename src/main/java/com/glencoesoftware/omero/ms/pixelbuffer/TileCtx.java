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

import com.glencoesoftware.omero.ms.core.OmeroRequestCtx;

import io.vertx.core.MultiMap;
import omeis.providers.re.data.RegionDef;

public class TileCtx extends OmeroRequestCtx {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(TileCtx.class);

    /** Image Id*/
    public Long imageId;

    /** z - index */
    public Integer z;

    /** c - index */
    public Integer c;

    /** t - index */
    public Integer t;

    /** Resolution to read */
    public Integer resolution;

    /** Region descriptor (region); X, Y, width, and height pixel offsets */
    public RegionDef region;

    /** Optional region output format ("tif" only at present) */
    public String format;

    /**
     * Constructor for jackson to decode the object from string
     */
    TileCtx() {};

    /**
     * Default constructor.
     * @param params {@link io.vertx.core.http.HttpServerRequest} parameters
     * required for rendering an image region.
     * @param omeroSessionKey OMERO session key.
     */
    TileCtx(MultiMap params, String omeroSessionKey) {
        this.omeroSessionKey = omeroSessionKey;
        imageId = Long.parseLong(params.get("imageId"));
        z = Integer.parseInt(params.get("z"));
        c = Integer.parseInt(params.get("c"));
        t = Integer.parseInt(params.get("t"));
        Integer x = Integer.parseInt(params.get("x"));
        Integer y = Integer.parseInt(params.get("y"));
        Integer width = Integer.parseInt(params.get("w"));
        Integer height = Integer.parseInt(params.get("h"));
        region = new RegionDef(x, y, width, height);
        resolution = Optional.ofNullable(params.get("resolution"))
                .map(Integer::parseInt)
                .orElse(null);
        format = params.get("format");

        log.debug(
                "Image:{}, z: {}, c: {}, t: {}, resolution: {}, " +
                "region: {}, format: {}",
                imageId, z, c, t, resolution, region, format);
    }

}
