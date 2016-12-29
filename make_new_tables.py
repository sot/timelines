#!/usr/bin/env python
"""
Copy timelines and load_segments tables from sybase to local sqlite db and create
timeline_loads view.
"""

import os
import time
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
    parser.add_option("--cmds",
                      action="store_true",
                      help="Add cmds, cmd_intpars, and cmd_fltpars to output database")
    parser.add_option("--cmd-states",
                      action="store_true",
                      help="Add cmd_states to output database")
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
    

    if not opt.tstart:
        # Nothing more to do
        return

    datecols = ['datestart', 'datestart']
    tables = ['load_segments', 'timelines']

    if opt.cmds:
        tables.extend(['cmds', 'cmd_fltpars', 'cmd_intpars'])
        datecols.extend(['time', None, None])

    if opt.cmd_states:
        tables.append('cmd_states')
        datecols.append('datestart')

    for table, datecol in zip(tables, datecols):
        query = 'select * from {}'.format(table)

        if datecol:
            date = DateTime(opt.tstart)
            datetime = getattr(date, 'date' if datecol == 'datestart' else 'secs')
            query += ' where {} >= {!r}'.format(datecol, datetime)

            if opt.tstop:
                start = DateTime(opt.tstop)
                datetime = getattr(date, 'date' if datecol == 'datestart' else 'time')
                query += ' and datestart <= {!r}'.format(datecol, datetime)

        print(query)
        vals = syb.fetchall(query)

        print('Writing output {} table'.format(table))
        for val in vals:
            db.insert(val, table, commit=(opt.dbi == 'sybase'))

        db.commit()


if __name__ == '__main__':
    main()
