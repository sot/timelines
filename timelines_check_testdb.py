#!/usr/bin/env python
import os
import timelines_test


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


def main():
    opt, args = get_options()
    outdir = opt.outdir
    dbfilename = os.path.join(outdir, 'test.db3')
#    timelines_test.run_model( opt, dbfilename )
    print dbfilename
    timelines_test.cmp_states( opt, dbfilename )


if __name__ == '__main__':
    main()




