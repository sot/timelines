#!/usr/bin/env python
"""
Copy timelines and load_segments tables from sybase to local sqlite db and create
timeline_loads view.
"""

import os
import time
import mx.DateTime
import Ska.DBI
from Chandra.Time import DateTime

REAL_SKA = '/proj/sot/ska'

def get_options():
    from optparse import OptionParser
    parser = OptionParser(usage="usage: %prog [options] [cmd_set_arg1 ...]")
    parser.set_defaults()
    parser.add_option("--tstart",
                      default="1999:001:00:00:00.000",
                      help="only grab timelines and load_segments after tstart")
    nowdate=DateTime(time.time(), format='unix').mxDateTime
    futuredate=nowdate + mx.DateTime.DateTimeDeltaFromDays(30)
    parser.add_option("--tstop",
                      default=DateTime(futuredate).date,
                      help="only grab timelines and load_segments before tstop")
    parser.add_option("--server",
                      default='db_base.db3',
                      help="DBI server (<filename>|sybase)")
    opt, args = parser.parse_args()
    return opt, args

def main():
    opt, args = get_options()

    syb = Ska.DBI.DBI(dbi='sybase', numpy=False, verbose=True)
    db = Ska.DBI.DBI(dbi='sqlite', server=opt.server, numpy=False, verbose=False)

    for drop in ('VIEW timeline_loads', 'TABLE timelines', 'TABLE load_segments',
                 'TABLE cmd_fltpars', 'TABLE cmd_intpars', 'TABLE cmds',
                 'TABLE cmd_states',
                 'TABLE tl_processing', 'TABLE tl_built_loads', 'TABLE tl_obsids'):
        try:
            db.execute('DROP %s' % drop)
        except:
            print '%s not found' % drop

    # use local versions of the files in this project and the REAL_SKA versions of others
    for sqldef in ('load_segments', 'timelines', 'timeline_loads', 'tl_dep',
                   os.path.join(REAL_SKA, 'data', 'cmd_states', 'cmd_fltpars')
                   os.path.join(REAL_SKA, 'data', 'cmd_states', 'cmd_intpars')
                   os.path.join(REAL_SKA, 'data', 'cmd_states', 'cmds')
                   os.path.join(REAL_SKA, 'data', 'cmd_states', 'cmd_states')
                   ):
        cmd = file(sqldef + '_def.sql').read()
        db.execute(cmd, commit=True)





    timelines = syb.fetchall("""select * from timelines 
                                where datestart >= '%s' 
                                and datestart <= '%s'""" % ( DateTime(opt.tstart).date,
                                                           DateTime(opt.tstop).date ))
    load_segments = syb.fetchall("""select * from load_segments
                                    where datestart >= '%s' 
                                    and datestart <= '%s'""" % ( DateTime(opt.tstart).date,
                                                               DateTime(opt.tstop).date ))

    for ls in load_segments:
        print 'Inserting ls %d:%s' % (ls['year'], ls['load_segment'])
        db.insert(ls, 'load_segments')

    for tl in timelines:
        print 'Insert tl %s %s %d' % (tl['datestart'], tl['datestop'], tl['id'])
        db.insert(tl, 'timelines')

    db.commit()

if __name__ == '__main__':
    main()
