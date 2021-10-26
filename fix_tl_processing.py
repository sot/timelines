#!/usr/bin/env python

import Ska.DBI


def get_options():
    from optparse import OptionParser
    parser = OptionParser()
    parser.set_defaults()
    parser.add_option("--dbi",
                      default='sqlite',
                      help="Database interface (sqlite|sybase)")
    parser.add_option("--server",
                      default='db_base.db3',
                      help="DBI server (<filename>|sybase)")
    opt, args = parser.parse_args()
    return opt, args


def repair(dbh):
    """
    Make any necessary edits to the tl_processing table.  This is called in
    update_load_seg_db.py before loads are inserted and timelines are determined.

    It is not expected that any edits will need to be created, but this mechanism
    is scripted into the suite to allow the possibility.

    If, at some point, we need to manually override mapping of replan commands
    to a specific directory instead of the one determined by parse_cmd_load_gen.pl,
    we could use something like the commented-out code in this routine.
    """
    dbh.verbose = True
    # dbh.execute("delete from tl_processing where dir = '/2008/FEB1808/oflsb/'")
    # dbh.execute("""insert into tl_processing
    # ( year, dir, file )
    # values
    # (2008, '/2008/FEB1808/oflsb/', 'C048_0802.sum')
    # """)
    dbh.verbose = False


def main():
    opt, args = get_options()
    dbh = Ska.DBI.DBI(dbi=opt.dbi, server=opt.server)
    repair(dbh)


if __name__ == "__main__":

    main()
