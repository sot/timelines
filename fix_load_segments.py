#!/usr/bin/env python

import Ska.DBI
from Chandra.Time import DateTime

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

def repair( dbh ):
    dbh.verbose = True
    dbh.execute("delete from load_segments where load_segment = 'CL304:0504' and year = 2003;")

    dbh.execute("update load_segments set datestop='2008:353:05:00:00.000' where load_segment = 'CL352:1208' and year = 2008;")
    dbh.execute("update load_segments set fixed_by_hand=1 where load_segment = 'CL352:1208' and year = 2008;")

    # CL110:1404 was run, not CL110:1409
    dbh.execute("delete from load_segments where load_segment = 'CL110:1409' and year = 2003;")
    # and delete the real one so the re-insert always works
    dbh.execute("delete from load_segments where load_segment = 'CL110:1404' and year = 2003;")
    cmd_idx = int(DateTime('2003:110:14:07:09.439').secs)
    dbh.execute("""insert into load_segments values 
                  (%d, 'CL110:1404', 2003, '2003:110:14:07:09.439', '2003:112:00:00:31.542', 130, 1);""" % cmd_idx)

    # CL188:0402 is missing
    dbh.execute("delete from load_segments where load_segment = 'CL188:0402' and year = 2009;")
    cmd_idx = int(DateTime('2009:188:04:00:00.000').secs)
    dbh.execute("""insert into load_segments values 
                  (%d, 'CL188:0402', 2009, '2009:188:04:00:00.000', '2009:188:20:57:33.571', 129, 1);""" % cmd_idx)
    dbh.verbose = False

def main():
    opt, args = get_options()
    dbh = Ska.DBI.DBI(dbi=opt.dbi, server=opt.server)
    repair(dbh)


if __name__ == "__main__":

    main()
