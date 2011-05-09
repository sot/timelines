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


def main():
    """
    Run timelines_test.run_model over the states from the test.db3 found in --outdir
    Run timelines_test.cmp_states to compare the states from test.db3 to the sybase tables
    """
    
    opt, args = get_options()
    outdir = opt.outdir
    dbfilename = os.path.join(outdir, 'test.db3')
    import Ska.DBI
    dbh = Ska.DBI.DBI(dbi='sqlite', server=dbfilename)
    model = timelines_test.run_model( opt, dbh)

    #timelines_test.cmp_states( opt, dbfilename )
    #timelines_test.cmp_timelines( opt, dbfilename )

if __name__ == '__main__':
    main()




