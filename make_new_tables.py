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

SKA = os.environ['SKA']

def get_options():
    from optparse import OptionParser
    parser = OptionParser(usage="usage: %prog [options] [cmd_set_arg1 ...]")
    parser.set_defaults()
    parser.add_option("--tstart",
                      help="only grab timelines and load_segments after tstart")
    parser.add_option("--tstop",
                      help="only grab timelines and load_segments before tstop")
    parser.add_option("--dbi",
                      default='sqlite')
    parser.add_option("--server",
                      default='db_base.db3',
                      help="DBI server (<filename>|sybase)")
    parser.add_option("--user",
                      default='aca_test')
    parser.add_option("--database",
                      default='aca_tstdb')
    parser.add_option("--verbose",
                      action="store_true")
    parser.add_option('--wipe',
                      action="store_true")
    opt, args = parser.parse_args()
    if opt.user == 'aca_ops' or opt.database == 'aca':
        raise ValueError("make_new_tables should not be used on flight tables (user != aca_ops, database != aca)")
    return opt, args

def main():
    opt, args = get_options()

    syb = Ska.DBI.DBI(dbi='sybase', user='aca_read', server='sybase', database='aca',
                      numpy=False, verbose=opt.verbose)
    db = Ska.DBI.DBI(dbi=opt.dbi, server=opt.server, user=opt.user, database=opt.database,
                     numpy=False, verbose=opt.verbose)

    for truncate in (
        'TABLE cmd_states',
                     'TABLE cmd_intpars', 'TABLE cmd_fltpars', 'TABLE cmds',
                     'TABLE timelines', 'TABLE load_segments',
                     'TABLE tl_processing', 'TABLE tl_built_loads', 'TABLE tl_obsids'):

        try:
            db.execute('TRUNCATE %s' % truncate)
        except:
            if opt.verbose:
                print '%s not found' % truncate
                     

    for drop in ('VIEW timeline_loads',
                 'TRIGGER fkd_load_segment_id' ,
                 'TRIGGER fkd_timeline_id_cmds' ,
                 'TRIGGER fkd_timeline_id_cmd_fltpars' ,
                 'TRIGGER fkd_timeline_id_cmd_intpars' ,
                 'TRIGGER fkd_cmd_id_cmd_fltpars',
                 'TRIGGER fkd_cmd_id_cmd_intpars',
                 'TABLE cmd_states',
                 'TABLE cmd_intpars', 'TABLE cmd_fltpars', 'TABLE cmds',
                 'TABLE timelines', 'TABLE load_segments',
                 'TABLE tl_processing', 'TABLE tl_built_loads', 'TABLE tl_obsids'):

        try:
            db.execute('DROP %s' % drop)
        except:
            if opt.verbose:
                print '%s not found' % drop
            

    if opt.wipe:
        # just delete
        return
    # use local versions of the files in this project and the SKA versions of others
    for sqldef in ('load_segments', 'timelines', 'timeline_loads', 'tl_dep',
                   os.path.join(SKA, 'data', 'cmd_states', 'cmds'),
                   os.path.join(SKA, 'data', 'cmd_states', 'cmd_fltpars'),
                   os.path.join(SKA, 'data', 'cmd_states', 'cmd_intpars'),
                   os.path.join(SKA, 'data', 'cmd_states', 'cmd_states'),
                   ):
        cmd = file(sqldef + '_def.sql').read()
        db.execute(cmd, commit=True)

    # and force the use of a few triggers to simulate some foreign-key constraints
    #trigger_cmds = file('sqlite_triggers.sql').read()
    #db.execute(trigger_cmds, commit=True)
    from Ska.Shell import bash
    if (opt.dbi == 'sqlite'):
        bash("sqlite3 %s < sqlite_triggers.sql" % opt.server, )
    

    if opt.tstart:

        ls_query = """select * from load_segments
                      where datestart >= '%s' """ % ( DateTime(opt.tstart).date)
        if opt.tstop:
            ls_query += " and datestart <= '%s'" % DateTime(opt.tstop).date
        
        load_segments = syb.fetchall(ls_query)

        t_query = """select * from timelines
                      where datestart >= '%s' """ % ( DateTime(opt.tstart).date)
        if opt.tstop:
            t_query += " and datestart <= '%s'" % DateTime(opt.tstop).date
        
        timelines = syb.fetchall(t_query)

        for ls in load_segments:
            #        print 'Inserting ls %d:%s' % (ls['year'], ls['load_segment'])
            db.insert(ls, 'load_segments')

        for tl in timelines:
            #        print 'Insert tl %s %s %d' % (tl['datestart'], tl['datestop'], tl['id'])
            db.insert(tl, 'timelines')

    


if __name__ == '__main__':
    main()
