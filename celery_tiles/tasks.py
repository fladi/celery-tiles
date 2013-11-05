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

from .utils import GlobalMercator

logger = logging.getLogger(__name__)

class TileRenderer(Task):

    ignore_result = True

    def __init__(self, *args, **kwargs):
        #super(TileRenderer, self).__init__(*args, **kwargs)
        gdal.AllRegister()
        gdal.SetConfigOption("GDAL_PAM_ENABLED", "NO")
        self.mem_drv = gdal.GetDriverByName('MEM')

    def run(self, inputfile, tilefile, tx, ty, tz, tilesize, bands, driver='PNG', optimize=False):
        # Initialize necessary GDAL drivers
        if not self.mem_drv:
            raise Exception("The 'MEM' driver was not found, is it available in this GDAL build?")

        out_drv = gdal.GetDriverByName(driver)
        if not out_drv:
            raise Exception("The '%s' driver was not found, is it available in this GDAL build?", driver)

        ds = gdal.Open(inputfile, gdal.GA_ReadOnly)

        if not ds:
            raise Exception("It is not possible to open the input file '%s'." % inputfile)

        logger.info("Preprocessed file: %s ( %sP x %sL - %s bands)", inputfile, ds.RasterXSize, ds.RasterYSize, ds.RasterCount)
        logger.info("Input projection: %s", ds.GetProjection())

        mercator = GlobalMercator(tilesize=tilesize)


        b = mercator.TileBounds(tx, ty, tz)

        logger.info("TileBounds: %f %f %f %f", b[0], b[3], b[2], b[1])

        rb, wb = self.geo_query( ds, b[0], b[3], b[2], b[1])

        # Tile bounds in raster coordinates for ReadRaster query
        rx, ry, rxsize, rysize = rb
        wx, wy, wxsize, wysize = wb
        nativesize = wx + wxsize # Pixel size in the raster covering query geo extent
        logger.info("Native Extent: %d", nativesize)

        # Query is in 'nearest neighbour' but can be bigger in then the tilesize
        # We scale down the query to the tilesize by supplied algorithm.

        # Tile dataset in memory
        band_list = list(range(1, bands+1))
        dstile = self.mem_drv.Create('', tilesize, tilesize, bands+1)
        logger.info("ReadRaster Extent: rx:%d ry:%d rxsize:%d rysize:%d wx:%d wy:%d wxsize:%d wysize:%d", rx, ry, rxsize, rysize, wx, wy, wxsize, wysize)
        data = ds.ReadRaster(rx, ry, rxsize, rysize, wxsize, wysize, band_list=band_list)
        alphaband = ds.GetRasterBand(1).GetMaskBand()
        alpha = alphaband.ReadRaster(rx, ry, rxsize, rysize, wxsize, wysize)

        # Use the ReadRaster result directly in tiles ('nearest neighbour' query)
        #dstile.WriteRaster(wx, wy, wxsize, wysize, data, band_list=band_list)
        #dstile.WriteRaster(wx, wy, wxsize, wysize, alpha, band_list=[bands+1])
        dsquery = self.mem_drv.Create('', rxsize, rysize, bands+1)

        dsquery.WriteRaster(wx, wy, wxsize, wysize, data, band_list=band_list)
        dsquery.WriteRaster(wx, wy, wxsize, wysize, alpha, band_list=[bands+1])

        dsquery.SetGeoTransform( (0.0, tilesize / float(rxsize), 0.0, 0.0, 0.0, tilesize / float(rysize)) )
        dstile.SetGeoTransform( (0.0, 1.0, 0.0, 0.0, 0.0, 1.0) )

        logger.info('Reprojecting ...')
        res = gdal.ReprojectImage(dsquery, dstile, None, None, gdal.GRA_NearestNeighbour)

        del data

        # Write a copy of tile to png/jpg
        logger.info('Rendering: %s', tilefile)
        out_drv.CreateCopy(tilefile, dstile, strict=0)

        del dstile

        if optimize:
            logger.info('Optimizing: %s', tilefile)
            subprocess.call(["pngnq", '-e .png', '-f', tilefile])

        logger.info('Done: %s', tilefile)

    def geo_query(self, ds, ulx, uly, lrx, lry):
        """For given dataset and query in cartographic coordinates
        returns parameters for ReadRaster() in raster coordinates and
        x/y shifts (for border tiles). If the querysize is not given, the
        extent is returned in the native resolution of dataset ds."""

        geotran = ds.GetGeoTransform()
        rx= int((ulx - geotran[0]) / geotran[1] + 0.001)
        ry= int((uly - geotran[3]) / geotran[5] + 0.001)
        rxsize= int((lrx - ulx) / geotran[1] + 0.5)
        rysize= int((lry - uly) / geotran[5] + 0.5)

        wxsize, wysize = rxsize, rysize

        # Coordinates should not go out of the bounds of the raster
        wx = 0
        if rx < 0:
            rxshift = abs(rx)
            wx = int( wxsize * (float(rxshift) / rxsize) )
            wxsize = wxsize - wx
            rxsize = rxsize - int( rxsize * (float(rxshift) / rxsize) )
            rx = 0
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

