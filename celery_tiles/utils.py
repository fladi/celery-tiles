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

from osgeo import gdal, osr

import os
import math


class GlobalMercator(object):
    """
    TMS Global Mercator Profile
    ---------------------------

    Functions necessary for generation of tiles in Spherical Mercator projection,
    EPSG:3857 (EPSG:gOOglE, Google Maps Global Mercator), EPSG:3785, OSGEO:41001.

    Such tiles are compatible with Google Maps, Microsoft Virtual Earth, Yahoo Maps,
    UK Ordnance Survey OpenSpace API, ...
    and you can overlay them on top of base maps of those web mapping applications.

    Pixel and tile coordinates are in TMS notation (origin [0,0] in bottom-left).

    What coordinate conversions do we need for TMS Global Mercator tiles::

         LatLon      <->       Meters      <->     Pixels    <->       Tile

     WGS84 coordinates   Spherical Mercator  Pixels in pyramid  Tiles in pyramid
         lat/lon            XY in metres     XY pixels Z zoom      XYZ from TMS
        EPSG:4326           EPSG:3857
         .----.              ---------               --                TMS
        /      \     <->     |       |     <->     /----/    <->      Google
        \      /             |       |           /--------/          QuadTree
         -----               ---------         /------------/
       KML, public         WebMapService         Web Clients      TileMapService

    What is the coordinate extent of Earth in EPSG:3857?

      [-20037508.342789244, -20037508.342789244, 20037508.342789244, 20037508.342789244]
      Constant 20037508.342789244 comes from the circumference of the Earth in meters,
      which is 40 thousand kilometers, the coordinate origin is in the middle of extent.
      In fact you can calculate the constant as: 2 * math.pi * 6378137 / 2.0
      $ echo 180 85 | gdaltransform -s_srs EPSG:4326 -t_srs EPSG:3857
      Polar areas with abs(latitude) bigger then 85.05112878 are clipped off.

    What are zoom level constants (pixels/meter) for pyramid with EPSG:3857?

      whole region is on top of pyramid (zoom=0) covered by 256x256 pixels tile,
      every lower zoom level resolution is always divided by two
      initialResolution = 20037508.342789244 * 2 / 256 = 156543.03392804062

    What is the difference between TMS and Google Maps/QuadTree tile name convention?

      The tile raster itself is the same (equal extent, projection, pixel size),
      there is just different identification of the same raster tile.
      Tiles in TMS are counted from [0,0] in the bottom-left corner, id is XYZ.
      Google placed the origin [0,0] to the top-left corner, reference is XYZ.
      Microsoft is referencing tiles by a QuadTree name, defined on the website:
      http://msdn2.microsoft.com/en-us/library/bb259689.aspx

    The lat/lon coordinates are using WGS84 datum, yeh?

      Yes, all lat/lon we are mentioning should use WGS84 Geodetic Datum.
      Well, the web clients like Google Maps are projecting those coordinates by
      Spherical Mercator, so in fact lat/lon coordinates on sphere are treated as if
      the were on the WGS84 ellipsoid.

      From MSDN documentation:
      To simplify the calculations, we use the spherical form of projection, not
      the ellipsoidal form. Since the projection is used only for map display,
      and not for displaying numeric coordinates, we don't need the extra precision
      of an ellipsoidal projection. The spherical projection causes approximately
      0.33 percent scale distortion in the Y direction, which is not visually noticable.

    How do I create a raster in EPSG:3857 and convert coordinates with PROJ.4?

      You can use standard GIS tools like gdalwarp, cs2cs or gdaltransform.
      All of the tools supports -t_srs 'epsg:3857'.

      For other GIS programs check the exact definition of the projection:
      More info at http://spatialreference.org/ref/user/google-projection/
      The same projection is degined as EPSG:3785. WKT definition is in the official
      EPSG database.

      Proj4 Text:
        +proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0
        +k=1.0 +units=m +nadgrids=@null +no_defs

      Human readable WKT format of EPGS:3857:
         PROJCS["Google Maps Global Mercator",
             GEOGCS["WGS 84",
                 DATUM["WGS_1984",
                     SPHEROID["WGS 84",6378137,298.257223563,
                         AUTHORITY["EPSG","7030"]],
                     AUTHORITY["EPSG","6326"]],
                 PRIMEM["Greenwich",0],
                 UNIT["degree",0.0174532925199433],
                 AUTHORITY["EPSG","4326"]],
             PROJECTION["Mercator_1SP"],
             PARAMETER["central_meridian",0],
             PARAMETER["scale_factor",1],
             PARAMETER["false_easting",0],
             PARAMETER["false_northing",0],
             UNIT["metre",1,
                 AUTHORITY["EPSG","9001"]]]
    """

    MAXZOOMLEVEL = 32

    def __init__(self, tilesize=256):
        "Initialize the TMS Global Mercator pyramid"
        # 20037508.342789244
        self.tilesize = tilesize
        self.originShift = math.pi * 6378137.0
        self.initialResolution = 2 * self.originShift / self.tilesize

    def LatLonToMeters(self, lat, lon ):
        "Converts given lat/lon in WGS84 Datum to XY in Spherical Mercator EPSG:3857"

        mx = lon * self.originShift / 180.0
        my = math.log( math.tan((90 + lat) * math.pi / 360.0 )) / (math.pi / 180.0)

        my = my * self.originShift / 180.0
        return mx, my

    def MetersToLatLon(self, mx, my ):
        "Converts XY point from Spherical Mercator EPSG:3857 to lat/lon in WGS84 Datum"

        lon = (mx / self.originShift) * 180.0
        lat = (my / self.originShift) * 180.0

        lat = 180 / math.pi * (2 * math.atan( math.exp( lat * math.pi / 180.0)) - math.pi / 2.0)
        return lat, lon

    def PixelsToMeters(self, px, py, zoom):
        "Converts pixel coordinates in given zoom level of pyramid to EPSG:3857"

        res = self.Resolution( zoom )
        mx = px * res - self.originShift
        my = py * res - self.originShift
        return mx, my

    def MetersToPixels(self, mx, my, zoom):
        "Converts EPSG:3857 to pyramid pixel coordinates in given zoom level"

        res = self.Resolution( zoom )
        px = (mx + self.originShift) / res
        py = (my + self.originShift) / res
        return px, py

    def PixelsToTile(self, px, py):
        "Returns a tile covering region in given pixel coordinates"

        tx = int( math.ceil( px / float(self.tilesize) ) - 1 )
        ty = int( math.ceil( py / float(self.tilesize) ) - 1 )
        return tx, ty

    def PixelsToRaster(self, px, py, zoom):
        "Move the origin of pixel coordinates to top-left corner"

        mapSize = self.tilesize << zoom
        return px, mapSize - py

    def MetersToTile(self, mx, my, zoom):
        "Returns tile for given mercator coordinates"

        px, py = self.MetersToPixels( mx, my, zoom)
        return self.PixelsToTile( px, py)

    def TileBounds(self, tx, ty, zoom):
        "Returns bounds of the given tile in EPSG:3857 coordinates"

        minx, miny = self.PixelsToMeters( tx*self.tilesize, ty*self.tilesize, zoom )
        maxx, maxy = self.PixelsToMeters( (tx+1)*self.tilesize, (ty+1)*self.tilesize, zoom )
        return ( minx, miny, maxx, maxy )

    def TileLatLonBounds(self, tx, ty, zoom):
        "Returns bounds of the given tile in latitude/longitude using WGS84 datum"

        bounds = self.TileBounds( tx, ty, zoom)
        minLat, minLon = self.MetersToLatLon(bounds[0], bounds[1])
        maxLat, maxLon = self.MetersToLatLon(bounds[2], bounds[3])

        return ( minLat, minLon, maxLat, maxLon)

    def Resolution(self, zoom):
        "Resolution (meters/pixel) for given zoom level (measured at Equator)"

        return self.initialResolution / (2**zoom)

    def ZoomForPixelSize(self, pixelSize):
        "Maximal scaledown zoom of the pyramid closest to the pixelSize."

        for i in range(self.MAXZOOMLEVEL):
            if pixelSize > self.Resolution(i):
                if i!=0:
                    return i-1
                else:
                    return 0 # We don't want to scale up

def prepare(inputfile, logger, exc, **options):
    gdal.AllRegister()
    # Spatial Reference System of tiles
    out_srs = osr.SpatialReference()
    out_srs.ImportFromEPSG(3857)
    if not options.get('dry_run'):
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
        return

    workerfile = os.path.abspath("%s.worker" % inputfile)
    out_ds.GetDriver().CreateCopy(workerfile, out_ds)
    tr = TileRenderer()
    for tz in range (tmaxz, tminz-1, -1):
        tminx, tminy, tmaxx, tmaxy = tminmax[tz]
        for tx in range(tminx, tmaxx+1):
            tiledir = os.path.abspath(os.path.join(output, str(tz), str(tx)))
            if not os.path.exists(tiledir):
                os.makedirs(tiledir)
            for ty in range(tmaxy, tminy-1, -1):
                tilefile = os.path.join(tiledir, "%s.%s" % (ty, options.get('format').lower()))
                if options.get('resume') and os.path.exists(tilefile):
                    continue
                args = (workerfile, tilefile, tx, ty, tz, options.get('tilesize'), dataBandsCount)
                kwargs = {'driver': options.get('format')}
                logger.debug("Task: %s, %s", repr(args), repr(kwargs))
                tr.apply_async(args, kwargs)


