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

import sys
import os
import logging
import warnings
import tempfile

from django.core.management.base import BaseCommand, CommandError
from optparse import make_option

# TODO:
#RESAMPLER = ('average','near','bilinear','cubic','cubicspline','lanczos')

from osgeo import gdal
from osgeo import osr

from ...utils import GlobalMercator, prepare
from ...tasks import TileRenderer

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    option_list = BaseCommand.option_list + (
        make_option('-o', '--output',
            action='store',
            dest='output',
            type='string',
            default=None,
            help='Directory where generated tiles should be stored.',
        ),
        #make_option('-r', '--resampling',
        #    action='store',
        #    dest='resampling',
        #    type='choice',
        #    choices=RESAMPLER,
        #    default='near',
        #    help='Resampling method to use.',
        #),
        make_option('-e', '--resume',
            action='store_true',
            dest='resume',
            help='Resume tile generation, omitting already existing tiles.',
        ),
        make_option('-n', '--dry-run',
            action='store_true',
            dest='dry_run',
            help='Do not create the actual tiles, only do validation for the input file and calculate number of neccessary tasks.',
        ),
        make_option('-f', '--format',
            action='store',
            dest='format',
            type='choice',
            choices=('PNG','GIF','JPEG'),
            default='PNG',
            help='Output format for tile images.',
        ),
        make_option('-t', '--tilesize',
            action='store',
            dest='tilesize',
            type='int',
            default=256,
            help='Size (quadratic) for each tile image in pixels.',
        ),
        make_option('-s', '--srs',
            action='store',
            dest='srs',
            type='string',
            help='GDAL input SRS.',
        ),
    )
    help = 'Fans out celery tasks to generate TMS tile images from GDAL input for EPSG:3857.'

    def handle(self, inputfile, **options):
        prepare(inputfile, logger, CommandError, **options)

