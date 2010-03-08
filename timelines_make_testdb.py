#!/usr/bin/env python
import os
import sys
import time
import re
import tempfile
import mx.DateTime
import numpy as np
from itertools import count, izip
from Ska.Shell import bash_shell
import Ska.DBI
import Ska.Table
from Chandra.Time import DateTime

import timelines_test


def get_options():
    from optparse import OptionParser
    parser = OptionParser(usage="usage: %prog [options] [cmd_set_arg1 ...]")
    parser.set_defaults()
#    parser.add_option("--tstart",
#                      help="tstart for mp yanks")
#    parser.add_option("--tstop",
#                      help="tstop for mp yanks")
    parser.add_option("--outdir",
                      default=".",
                      help="Destination directory for links")
    parser.add_option("--load_rdb",
                      help="load segment rdb file")
    parser.add_option("--verbose",
                      action="store_true",
                      default=False,
                      help="verbose")
    opt, args = parser.parse_args()
    return opt, args

def main():
    opt, args = get_options()
    outdir = opt.outdir
    dbfilename = os.path.join(outdir, 'test.db3')
    (outdir, load_dir, mp_dir) = timelines_test.data_setup( opt.load_rdb, 
                                                                 outdir=outdir )
    if not os.path.exists(dbfilename):
        timelines_test.make_table( dbfilename )

    timelines_test.populate_states( outdir, load_dir, mp_dir, dbfilename, opt.verbose)

if __name__ == '__main__':
    main()



         





