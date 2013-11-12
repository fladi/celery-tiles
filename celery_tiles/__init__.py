# -*- coding: utf-8 -*-
###############################################################################
# Copyright (c) 2013, Michael Fladischer <FladischerMichael@fladi.at>
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

from osgeo import gdal, osr

import os
import tempfile

from celery_tiles.tasks import TileRenderer

version = "0.2"

def prepare(inputfile, logger, exc, **options):
    gdal.AllRegister()
    # Spatial Reference System of tiles
    out_srs = osr.SpatialReference()
    out_srs.ImportFromEPSG(3857)
    # Set output directory
    if not options.get('output'):
        # Directory with input filename without extension in actual directory
        output = os.path.abspath(os.path.join("%s.tiles" % os.path.splitext(inputfile)[0]))
        logger.info('No output specified, using %s', output)
    else:
        output = options.get('output')
    if os.path.exists(output):
        if not options.get('resume'):
            raise exc('Output %s already exists and resume is not enabled, aborting!' % output)
    else:
        if not options.get('dry_run'):
            os.makedirs(output)

    out_drv = gdal.GetDriverByName(options.get('format'))
    if not out_drv:
        raise exc("The '%s' driver was not found, is it available in this GDAL build?", options.get('format'))

    # Open the input file
    in_ds = gdal.Open(inputfile, gdal.GA_ReadOnly)

    if not in_ds:
        raise exc("It is not possible to open the input file '%s'." % inputfile)

    logger.info("Input file: %s ( %sP x %sL - %s bands)", inputfile, in_ds.RasterXSize, in_ds.RasterYSize, in_ds.RasterCount)

    if (in_ds.GetGeoTransform() == (0.0, 1.0, 0.0, 0.0, 0.0, 1.0)) and (in_ds.GetGCPCount() == 0):
        raise exc("Input file %s is not georeferenced!" % inputfile)

    # Read metadata from the input file
    if in_ds.RasterCount == 0:
        raise exc("Input file '%s' has no raster band" % inputfile)

    if in_ds.GetRasterBand(1).GetRasterColorTable():
        # TODO: Process directly paletted dataset by generating VRT in memory
        raise exc("Please convert %s to RGB/RGBA with gdal_translate." % inputfile)

    # Get NODATA value
    in_nodata = []
    for i in range(1, in_ds.RasterCount+1):
        if in_ds.GetRasterBand(i).GetNoDataValue() != None:
            in_nodata.append( in_ds.GetRasterBand(i).GetNoDataValue() )

    logger.info("NODATA: %s", in_nodata)

    # Here we should have RGBA input dataset opened in in_ds

    logger.info("Preprocessed file: %s ( %sP x %sL - %s bands)", inputfile, in_ds.RasterXSize, in_ds.RasterYSize, in_ds.RasterCount)

    # Spatial Reference System of the input raster
    in_srs = None
    if options.get('srs'):
        in_srs = osr.SpatialReference()
        in_srs.SetFromUserInput(options.get('srs'))
        in_srs_wkt = in_srs.ExportToWkt()
    else:
        in_srs_wkt = in_ds.GetProjection()
        if not in_srs_wkt and in_ds.GetGCPCount() != 0:
            in_srs_wkt = in_ds.GetGCPProjection()
        if in_srs_wkt:
            in_srs = osr.SpatialReference()
            in_srs.ImportFromWkt(in_srs_wkt)

    if not in_srs:
        raise exc("Input file %s has unknown SRS. Use --srs=ESPG:xyz (or similar) to provide source reference system." % inputfile)

    logger.info("Input SRS: %s", in_srs.ExportToProj4())
    logger.info("Output SRS: %s", out_srs.ExportToProj4())

    out_ds = None

    # Are the reference systems the same? Reproject if necessary.
    if (in_srs.ExportToProj4() != out_srs.ExportToProj4()) or (in_ds.GetGCPCount() != 0):
        logger.info("Creating AutoCreateWarpedVRT.")

        # Generation of VRT dataset in tile projection, default 'nearest neighbour' warping
        out_ds = gdal.AutoCreateWarpedVRT( in_ds, in_srs_wkt, out_srs.ExportToWkt() )

        # TODO: HIGH PRIORITY: Correction of AutoCreateWarpedVRT according the max zoomlevel for correct direct warping!!!

        #logger.info("Warping of the raster by AutoCreateWarpedVRT (result saved into 'tiles.vrt')")
        #out_ds.GetDriver().CreateCopy("tiles.vrt", out_ds)

        # Note: in_srs and in_srs_wkt contain still the non-warped reference system!!!

        # Correction of AutoCreateWarpedVRT for NODATA values
        if in_nodata != []:
            tempfilename = tempfile.mktemp('-bands.vrt') # TODO
            out_ds.GetDriver().CreateCopy(tempfilename, out_ds)
            # open as a text file
            s = open(tempfilename).read()
            # Add the warping options
            s = s.replace("""<GDALWarpOptions>""","""<GDALWarpOptions>
<Option name="INIT_DEST">NO_DATA</Option>
<Option name="UNIFIED_SRC_NODATA">YES</Option>""")
            # replace BandMapping tag for NODATA bands....
            for i in range(len(in_nodata)):
                s = s.replace("""<BandMapping src="%i" dst="%i"/>""" % ((i+1),(i+1)),"""<BandMapping src="%i" dst="%i">
<SrcNoDataReal>%i</SrcNoDataReal>
<SrcNoDataImag>0</SrcNoDataImag>
<DstNoDataReal>%i</DstNoDataReal>
<DstNoDataImag>0</DstNoDataImag>
</BandMapping>""" % ((i+1), (i+1), in_nodata[i], in_nodata[i])) # Or rewrite to white by: , 255 ))
            # save the corrected VRT
            open(tempfilename,"w").write(s)
            # open by GDAL as out_ds
            out_ds = gdal.Open(tempfilename) #, gdal.GA_ReadOnly)
            # delete the temporary file
            os.unlink(tempfilename)

            # set NODATA_VALUE metadata
            out_ds.SetMetadataItem('NODATA_VALUES','%i %i %i' % (in_nodata[0],in_nodata[1],in_nodata[2]))

            #logger.info("Modified warping result saved into 'tiles1.vrt'")
            #open("tiles1.vrt","w").write(s)

        # -----------------------------------
        # Correction of AutoCreateWarpedVRT for Mono (1 band) and RGB (3 bands) files without NODATA:
        # equivalent of gdalwarp -dstalpha
        if in_nodata == [] and out_ds.RasterCount in [1,3]:
            tempfilename = tempfile.mktemp('-alpha.vrt')
            out_ds.GetDriver().CreateCopy(tempfilename, out_ds)
            # open as a text file
            s = open(tempfilename).read()
            # Add the warping options
            s = s.replace("""<BlockXSize>""","""<VRTRasterBand dataType="Byte" band="%i" subClass="VRTWarpedRasterBand">
<ColorInterp>Alpha</ColorInterp>
</VRTRasterBand>
<BlockXSize>""" % (out_ds.RasterCount + 1))
            s = s.replace("""</GDALWarpOptions>""", """<DstAlphaBand>%i</DstAlphaBand>
</GDALWarpOptions>""" % (out_ds.RasterCount + 1))
            s = s.replace("""</WorkingDataType>""", """</WorkingDataType>
<Option name="INIT_DEST">0</Option>""")
            # save the corrected VRT
            open(tempfilename,"w").write(s)
            # open by GDAL as out_ds
            out_ds = gdal.Open(tempfilename) #, gdal.GA_ReadOnly)
            # delete the temporary file
            os.unlink(tempfilename)

    if not out_ds:
        out_ds = in_ds

    # Here we should have a raster (out_ds) in the correct Spatial Reference system

    # Get alpha band (either directly or from NODATA value)
    alphaband = out_ds.GetRasterBand(1).GetMaskBand()
    if (alphaband.GetMaskFlags() & gdal.GMF_ALPHA) or out_ds.RasterCount==4 or out_ds.RasterCount==2:
        # TODO: Better test for alpha band in the dataset
        dataBandsCount = out_ds.RasterCount - 1
    else:
        dataBandsCount = out_ds.RasterCount
    logger.info("dataBandsCount: %s", dataBandsCount)

    # Read the georeference

    out_gt = out_ds.GetGeoTransform()

    # Report error in case rotation/skew is in geotransform (possible only in 'raster' profile)
    if (out_gt[2], out_gt[4]) != (0,0):
        # TODO: Do the warping in this case automaticaly
        raise exc("Georeference of the raster in input file %s contains rotation or skew. Such raster is not supported. Please use gdalwarp first." % inputfile)

    # Here we expect: pixel is square, no rotation on the raster

    # Output Bounds - coordinates in the output SRS
    ominx = out_gt[0]
    omaxx = out_gt[0]+out_ds.RasterXSize*out_gt[1]
    omaxy = out_gt[3]
    ominy = out_gt[3]+out_ds.RasterYSize*out_gt[5]
    # Note: maybe round(x, 14) to avoid the gdal_translate behaviour, when 0 becomes -1e-15

    logger.info("Bounds (output srs): minX:%d minY:%d maxX:%d maxY:%d", ominx,ominy, omaxx, omaxy)

    # Calculating ranges for tiles in different zoom levels
    mercator = GlobalMercator(tilesize=options.get('tilesize')) # from globalmaptiles.py

    logger.info('Bounds (latlong): minX:%f minY:%f maxX:%f maxY:%f', *mercator.MetersToLatLon( ominx, ominy) + mercator.MetersToLatLon( omaxx, omaxy))

    # Get the minimal zoom level (map covers area equivalent to one tile)
    tminz = mercator.ZoomForPixelSize( out_gt[1] * max( out_ds.RasterXSize, out_ds.RasterYSize) / float(options.get('tilesize')))

    # Get the maximal zoom level (closest possible zoom level up on the resolution of raster)
    tmaxz = mercator.ZoomForPixelSize( out_gt[1] )

    logger.info('MinZoomLevel: %d (res:%f)', tminz,  mercator.Resolution( tminz ))
    logger.info('MaxZoomLevel: %d (res:%f)', tmaxz,  mercator.Resolution( tmaxz ))

    # Generate table with min max tile coordinates for all zoomlevels
    tminmax = {k: 0 for k in range(tminz, tmaxz+1)}
    for tz in range(tminz, tmaxz+1):
        tminx, tminy = mercator.MetersToTile( ominx, ominy, tz )
        tmaxx, tmaxy = mercator.MetersToTile( omaxx, omaxy, tz )
        # crop tiles extending world limits (+-180,+-90)
        tminx, tminy = max(0, tminx), max(0, tminy)
        tmaxx, tmaxy = min(2**tz-1, tmaxx), min(2**tz-1, tmaxy)
        logger.info("tiles at zoom %d: %d", tz, (tmaxy-tminy+1)*(tmaxx-tminx+1))
        tminmax[tz] = (tminx, tminy, tmaxx, tmaxy)

    # TODO: Maps crossing 180E (Alaska?)

    if options.get('dry_run'):
        logger.info("This is only a dry-run, stopping before any tasks are dispatched ...")

    workerfile = os.path.abspath("%s.worker" % inputfile)
    out_ds.GetDriver().CreateCopy(workerfile, out_ds)
    tr = TileRenderer()
    for tz in range (tmaxz, tminz-1, -1):
        tminx, tminy, tmaxx, tmaxy = tminmax[tz]
        for tx in range(tminx, tmaxx+1):
            tiledir = os.path.abspath(os.path.join(output, str(tz), str(tx)))
            if not os.path.exists(tiledir) and not options.get('dry_run'):
                logger.debug("Creating tile directory: %s", tiledir)
                os.makedirs(tiledir)
            for ty in range(tmaxy, tminy-1, -1):
                tilefile = os.path.join(tiledir, "%s.%s" % (ty, options.get('format').lower()))
                if options.get('resume') and os.path.exists(tilefile):
                    logger.debug("Skip existing tile: %s", tilefile)
                    continue
                args = (workerfile, tilefile, tx, ty, tz, options.get('tilesize'), dataBandsCount)
                kwargs = {'driver': options.get('format')}
                logger.debug("Task: %s, %s", repr(args), repr(kwargs))
                if not options.get('dry_run'):
                    tr.apply_async(args, kwargs)


