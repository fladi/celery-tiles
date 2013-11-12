# -*- coding: utf-8 -*-
###############################################################################
# Copyright (c) 2013, Michael Fladischer <FladischerMichael@fladi.at>
# Copyright (c) 2008, Klokan Petr Pridal
#
#  Permission is hereby granted, free of charge, to any person obtaining a
#  copy of this software and associated documentation files (the "Software"),
#  to deal in the Software without restriction, including without limitation
#  the rights to use, copy, modify, merge, publish, distribute, sublicense,
#  and/or sell copies of the Software, and to permit persons to whom the
#  Software is furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included
#  in all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
#  OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
#  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#  DEALINGS IN THE SOFTWARE.
#******************************************************************************


from __future__ import absolute_import

import warnings
import logging
import subprocess

from osgeo import gdal

from celery import Task

from celery_tiles.utils import GlobalMercator

logger = logging.getLogger(__name__)

class TileRenderer(Task):

    ignore_result = True

    def __init__(self, *args, **kwargs):
        #super(TileRenderer, self).__init__(*args, **kwargs)
        gdal.AllRegister()
        gdal.SetConfigOption("GDAL_PAM_ENABLED", "NO")
        self.mem_drv = gdal.GetDriverByName('MEM')

    def run(self, inputfile, tilefile, tx, ty, tz, tilesize, bands, driver='PNG', optimize=False, overviews=False):
        logger.info('Preparing: %s', tilefile)
        # Initialize necessary GDAL drivers
        if not self.mem_drv:
            raise Exception("The 'MEM' driver was not found, is it available in this GDAL build?")

        out_drv = gdal.GetDriverByName(driver)
        if not out_drv:
            raise Exception("The '%s' driver was not found, is it available in this GDAL build?", driver)

        ds = gdal.Open(inputfile, gdal.GA_ReadOnly)

        if not ds:
            raise Exception("It is not possible to open the input file '%s'." % inputfile)

        logger.debug("Preprocessed file: %s ( %sP x %sL - %s bands)", inputfile, ds.RasterXSize, ds.RasterYSize, ds.RasterCount)
        logger.debug("Input projection: %s", ds.GetProjection())


        mercator = GlobalMercator(tilesize=tilesize)

        b = mercator.TileBounds(tx, ty, tz)

        logger.debug("TileBounds: minx=%f miny=%f maxx=%f maxy=%f", *b)

        rb, wb = self.geo_query(ds, *b)

        # Tile bounds in raster coordinates for ReadRaster query
        rx, ry, rxsize, rysize = rb
        wx, wy, wxsize, wysize = wb

        logger.debug("ReadRaster Extent: rx:%d ry:%d rxsize:%d rysize:%d wx:%d wy:%d wxsize:%d wysize:%d", rx, ry, rxsize, rysize, wx, wy, wxsize, wysize)

        # Query is in 'nearest neighbour' but can be bigger in then the tilesize
        # We scale down the query to the tilesize by supplied algorithm.

        # Tile dataset in memory
        band_list = list(range(1, bands+1))
        dstile = self.mem_drv.Create('', tilesize, tilesize, bands+1)

        # Not implemented yet, therefor always uses reprojection.
        if not overviews:

            # Read data and alpha band
            logger.debug("Reading data band raster: %s", (rx, ry, rxsize, rysize, wxsize, wysize))
            data = ds.ReadRaster(rx, ry, rxsize, rysize, wxsize, wysize, band_list=band_list)
            alphaband = ds.GetRasterBand(1).GetMaskBand()
            logger.debug("Reading alpha band raster: %s", (rx, ry, rxsize, rysize, wxsize, wysize))
            alpha = alphaband.ReadRaster(rx, ry, rxsize, rysize, wxsize, wysize)

            # Create empty buffer to write to
            dsquery = self.mem_drv.Create('', wxsize, wysize, bands+1)

            logger.debug("Writing data band raster: %s", (wx, wy, wxsize, wysize))
            dsquery.WriteRaster(wx, wy, wxsize, wysize, data, band_list=band_list)
            logger.debug("Writing alpha band raster: %s", (wx, wy, wxsize, wysize))
            dsquery.WriteRaster(wx, wy, wxsize, wysize, alpha, band_list=[bands+1])

            dsquery.SetGeoTransform( (0.0, tilesize / float(rxsize), 0.0, 0.0, 0.0, tilesize / float(rysize)) )
            dstile.SetGeoTransform( (0.0, 1.0, 0.0, 0.0, 0.0, 1.0) )

            logger.debug('Reprojecting ...')
            res = gdal.ReprojectImage(dsquery, dstile, None, None, gdal.GRA_NearestNeighbour)

        else:
            tilesizex = int(wxsize/float(wx+wxsize)*tilesize)
            tilesizey = int(wysize/float(wy+wysize)*tilesize)
            logger.debug("Cropped Tilesize: x=%d y=%d", tilesizex, tilesizey)
            logger.debug("Reading data band raster: %s", (rx, ry, rxsize, rysize, wxsize, wysize))
            data = ds.ReadRaster(rx, ry, rxsize, rysize, band_list=band_list, buf_xsize=tilesizex, buf_ysize=tilesizey)
            alphaband = ds.GetRasterBand(1).GetMaskBand()
            logger.debug("Reading alpha band raster: %s", (rx, ry, rxsize, rysize, wxsize, wysize))
            alpha = alphaband.ReadRaster(rx, ry, rxsize, rysize, buf_xsize=tilesizex, buf_ysize=tilesizey)

            # Use the ReadRaster result directly in tiles ('nearest neighbour' query)
            logger.debug("Writing data band raster: %s", (wx, wy, wxsize, wysize))
            dstile.WriteRaster(wx, wy, wxsize, wysize, data, band_list=band_list)
            logger.debug("Writing alpha band raster: %s", (wx, wy, wxsize, wysize))
            dstile.WriteRaster(wx, wy, wxsize, wysize, alpha, band_list=[bands+1])

        del data
        del alpha

        # Write a copy of tile to png/jpg
        logger.info('Rendering: %s', tilefile)
        out_drv.CreateCopy(tilefile, dstile, strict=0)

        del dstile

        if optimize:
            logger.info('Optimizing: %s', tilefile)
            subprocess.call(["pngnq", '-e .png', '-f', tilefile])

        logger.info('Done: %s', tilefile)


    def geo_query(self, ds, minx, miny, maxx, maxy):
        """For given dataset and query in cartographic coordinates
        returns parameters for ReadRaster() in raster coordinates and
        x/y shifts (for border tiles). If the querysize is not given, the
        extent is returned in the native resolution of dataset ds."""

        (ulx, pwx, rotx, uly, roty, pwy) = ds.GetGeoTransform()
        rx = int((minx - ulx) / pwx + 0.001)
        ry = int((maxy - uly) / pwy + 0.001)
        rxsize = int((maxx - minx) / pwx + 0.5)
        rysize = int((miny - maxy) / pwy + 0.5)

        wxsize, wysize = rxsize, rysize

        # Coordinates should not go out of the bounds of the raster
        wx = 0
        # Left border
        if rx < 0:
            rxshift = abs(rx)
            wx = int( wxsize * (float(rxshift) / rxsize) )
            wxsize = wxsize - wx
            rxsize = rxsize - int( rxsize * (float(rxshift) / rxsize) )
            rx = 0
        # Right border
        if rx+rxsize > ds.RasterXSize:
            wxsize = int( wxsize * (float(ds.RasterXSize - rx) / rxsize) )
            rxsize = ds.RasterXSize - rx

        wy = 0
        if ry < 0:
            ryshift = abs(ry)
            wy = int( wysize * (float(ryshift) / rysize) )
            wysize = wysize - wy
            rysize = rysize - int( rysize * (float(ryshift) / rysize) )
            ry = 0
        if ry+rysize > ds.RasterYSize:
            wysize = int( wysize * (float(ds.RasterYSize - ry) / rysize) )
            rysize = ds.RasterYSize - ry

        return (rx, ry, rxsize, rysize), (wx, wy, wxsize, wysize)

