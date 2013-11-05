celery-tiles
============

celery-tiles is a distributed tile renderer using celery tasks. It provides a
CLI interface and Django command integration to validate and reproject raster
input that can be read by GDAL (see [GDAL inputs
formats](http://www.gdal.org/formats_list.html)).

The target [SRS](https://en.wikipedia.org/wiki/Spatial_reference_system) for the
reprojection is [EPSG:3857](http://spatialreference.org/ref/sr-org/6864/),
making the tiles useable for map viewers like GoogleMaps or OpenLayers.

Requirements
------------

Mandatory:

* GDAL (>= 1.9.2)
* Celery (>= 3.0.23)

Optional:

* pngnq (>= 1.0)

How it works
------------

The CLI or Django command expects the path to a file readable by GDAL. If this
file exists, spatial validations are applied and a
[VRT](http://www.gdal.org/ogr/drv_vrt.html) that is reprojected to EPSG:3857 is
created. The maximum and minimum zoom levels are calculated and the necessary
tiles for each zoom level are calculated. For each tile a celery task is
dispached that carries the parameters necessary to render a single tile.

Each worker then starts picking up the tasks and renders the tile from the VRT
file. The inputfile is only passed as an absolut path, so there should be no
problem for the worker to access the VRT file that was created by the CLI or
Django command. If the celery workers are distributed across several nodes they
need a way to access the input file over shared storage.

If the option `--output=<dir>` is used, the tiles are stored in the given
directory. If it is not specified, the tiles are stored in a directory that is
in the same path as the input file and has the extention `.tiles`.

The tiles for the input file `foo/bar.vrt` would be stored in
`foo/bar.vrt.tiles/`.

The directory structure beneath the output directory follows the
[TMS](http://en.wikipedia.org/wiki/Tile_Map_Service) convention:

    <zoom>/<x>/<y>.<ext>

Using distributed celery workers
--------------------------------

In order to utilize celery workers that reside on distributed systems, it is
neccessary to give them access to the inputfile and prefferably to the output
directory where all tiles are to be stored.

This has been tested with [GlusterFS](http://www.gluster.org/) but it should
also be possible to provide access with
[NFS](https://en.wikipedia.org/wiki/Network_File_System),
[OCFS2](https://en.wikipedia.org/wiki/Ocfs2) or
[Ceph](https://en.wikipedia.org/wiki/Ceph_%28storage%29).

Output
------

The default output format is
[PNG](https://en.wikipedia.org/wiki/Portable_Network_Graphics) but other formats
supported by GDAL are also possible. If the output format is PNG, optimizations
are applied to the tile by passing it through `pngnq`, which, most of the time,
can decrease the size of the tile image by a significant amount. no optimization
is done for other output formats.

CLI
---

celery_tiles <inputfile>

Django
------

django-admin tiles <inputfile>

Acknowledgment
--------------

Most of the validation and rendering code is heavily based on `gdal2tiles.py`
that ships with the GDAL distribution and was written by [Klokan Petr
Pridal](http://www.klokan.cz/projects/gdal2tiles/) <klokan@klokan.cz>.

TODO
----

* Allow different resampling algorithms
* Provide a way to track progress
* Improve documentation
* Error handling for failed tasks
