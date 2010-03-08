#!/usr/bin/env python

import os
import sys
import glob
import re
import mx.DateTime
import logging
from logging.handlers import SMTPHandler
import numpy as np
from itertools import count, izip

import Ska.Table
from Ska.DBI import DBI
from Chandra.Time import DateTime

sys.path.append(os.path.join(os.environ['SKA'], 'share', 'timelines'))
from timelines import update_loads_db, update_timelines_db, rdb_to_db_schema

log = logging.getLogger()
log.setLevel(logging.DEBUG)

# emails...
#smtp_handler = logging.handlers.SMTPHandler('localhost', 
#                                           'jeanconn@head.cfa.harvard.edu',
#                                           'jeanconn@head.cfa.harvard.edu',
#                                           'load segment update')
#smtp_handler.setLevel(logging.WARN)
#log.addHandler(smtp_handler)


def get_options():
    from optparse import OptionParser
    parser = OptionParser(usage='update_load_seg_db.py [options]')
    parser.set_defaults()
    parser.add_option("--dbi",
                      default='sqlite',
                      help="Database interface (sqlite|sybase)")
    parser.add_option("--server",
                      default='db_base.db3',
                      help="DBI server (<filename>|sybase)")
    parser.add_option("--dryrun",
                      action='store_true',
                      help="Do not perform real database updates")
    parser.add_option("--test",
                      action='store_true',
                      help="In test mode, allow changes to db before 2009...")
    parser.add_option("-v", "--verbose",
                      action='store_true',
                      )
    parser.add_option("--loadseg_rdb_dir",
                      default=os.path.join(os.environ['SKA'], 'data', 'arc', 'iFOT_events', 'load_segment'),
                      help="directory containing iFOT rdb files of load segments")
    
    (opt,args) = parser.parse_args()
    return opt, args


        
def main():
    """
    Command Load Segment Table Updater
    
    Read RDB table from SKA arc iFOT events area and update load_segments table
    Meant to be run as a cront task with no arguments.

    Details:
    Reads most recent RDB file from arc data iFOT events load_segments directory.
    Checks loads in that file for overlap and prolonged separations
    Removes outdated table entries
    Inserts new and newly modified entries

    Note that dryrun mode does not show timelines which *would* be updated,
    as an update to the load_segments table must happen prior to get_timelines()

    """

    (opt,args) = get_options()
    dbh = DBI(dbi=opt.dbi, server=opt.server)
    ch = logging.StreamHandler()
    ch.setLevel(logging.WARN)
    if opt.verbose:
        ch.setLevel(logging.DEBUG)
    log.addHandler(ch)
    if opt.dryrun:
        log.info("LOAD_SEG INFO: Running in dryrun mode")
    loadseg_dir = opt.loadseg_rdb_dir
    # get the loads from the arc ifot area
    all_rdb_files = glob.glob(os.path.join(loadseg_dir, "*"))
    rdb_file = max(all_rdb_files)
    log.debug("LOAD_SEG DEBUG: Updating from %s" % rdb_file)
    orig_rdb_loads = Ska.Table.read_ascii_table(rdb_file, datastart=3)
    ifot_loads = rdb_to_db_schema( orig_rdb_loads )
    if len(ifot_loads):
        update_loads_db( ifot_loads, dbh=dbh, test=opt.test, dryrun=opt.dryrun )    
        
        db_loads = dbh.fetchall("""select * from load_segments 
                                   where datestart >= '%s' and datestart <= '%s'
                                   order by datestart   
                                  """ % ( ifot_loads[0]['datestart'],
                                          ifot_loads[-1]['datestop'],
                                        )
                                    )    
        update_timelines_db( loads = db_loads, dbh=dbh, dryrun=opt.dryrun, test=opt.test )

    log.removeHandler(ch)


if __name__ == "__main__":
    main()
    



 
