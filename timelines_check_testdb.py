#!/usr/bin/env python
import os
import timelines_test
# Matplotlib setup
# Use Agg backend for command-line (non-interactive) operation
import matplotlib
if __name__ == '__main__':
    matplotlib.use('Agg')
        

def get_options():
    from optparse import OptionParser
    parser = OptionParser(usage="usage: %prog [options] [cmd_set_arg1 ...]")
    parser.set_defaults()
    parser.add_option("--outdir",
                      default=".",
                      help="Destination directory for links")
    parser.add_option("--load_rdb")
    parser.add_option("--run_start_time")
    parser.add_option("--oflsdir")
    parser.add_option("--verbose",
                      action="store_true",
                      default=False,
                      help="verbose")
    opt, args = parser.parse_args()
    return opt, args

#class options(object):
#    load_rdb = 'source_rdb/july_fixed.rdb'
##    load_rdb = 'test/nov21_right/loads/2009:322:13:15:00.000.rdb'
##    outdir = 'test/nov21_right/'
#    outdir = 'july_fixed'
#    oflsdir = None
#    run_start_time = None
#    verbose = True


def main():
    opt, args = get_options()
    outdir = opt.outdir
    dbfilename = os.path.join(outdir, 'test.db3')
    model = timelines_test.run_model( opt, dbfilename )
    timelines_test.cmp_states( opt, dbfilename )


if __name__ == '__main__':
    main()




