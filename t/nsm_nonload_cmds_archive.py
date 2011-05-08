#!/usr/bin/env python

import Ska.DBI
import Chandra.cmd_states as cmd_states
from Chandra.cmd_states import generate_cmds, cmd_set, interrupt_loads

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
    parser.add_option("--user",
                      default='aca_ops')
    parser.add_option("--database",
                      default='aca')
    opt, args = parser.parse_args()
    return opt, args

opt, args = get_options()

db = Ska.DBI.DBI(dbi=opt.dbi, server=opt.server,
                 user=opt.user, database=opt.database)

# Drop all existing non-load commands
for table in ('cmd_intpars', 'cmd_fltpars', 'cmds'):
    db.execute('DELETE FROM %s WHERE timeline_id is NULL' % table)

cmds = []

# Normal sun mode day 2008:225
cmds += generate_cmds('2008:225:10:00:00', cmd_set('nsm')) 
cmds += generate_cmds('2008:227:20:00:00', cmd_set('manvr', 0.734142,-0.111682,0.558589,0.369515 ))
cmds += generate_cmds('2008:227:21:25:00', cmd_set('manvr', 0.784368,-0.0804672,0.535211,0.303053))
cmds += generate_cmds('2008:227:22:15:00', cmd_set('manvr', 0.946291,-0.219412,0.0853751,0.221591))

# Normal sun mode '2008:292:23:24:56'
cmds += generate_cmds('2008:292:21:24:00', cmd_set('nsm'))
cmds += generate_cmds('2008:292:21:24:00', cmd_set('obsid', 0))
cmds += generate_cmds('2008:294:22:20:00', cmd_set('manvr', 0.608789,0.335168,0.717148,0.0523189))
cmds += generate_cmds('2008:295:02:35:00', cmd_set('manvr', 0.542935,0.333876,0.766926,0.0746532))
cmds += generate_cmds('2008:295:03:25:00', cmd_set('manvr', 0.866838,0.243633,0.362662,0.240231))

# NSM in 2004
cmds += generate_cmds('2004:315:16:41:00', cmd_set('nsm'))


# Add StopScience and VidBoard power down at approximate time of
# CAP_1134_AUG2609A_Load_Termination_and_ACIS_Safing
cap_cmds = (dict(dur=1.025),
            dict(cmd='ACISPKT',
                 tlmsid='AA00000000',
                 ),
            dict(cmd='ACISPKT',
                 tlmsid='WSVIDALLDN',
                 ),
            )  
cmds += generate_cmds('2009:240:10:40:00.000', cap_cmds )


# ACIS Reboot; Contrived Power commands to match telemetry
cmds += generate_cmds('2010:025:09:11:00.000', ( dict(cmd='ACISPKT', tlmsid='WSPOW00306'),
                                                 dict(cmd='ACISPKT', tlmsid='AA00000000')
                                                 ))
cmds += generate_cmds('2010:025:09:11:01.025', ( dict(cmd='ACISPKT', tlmsid='WSVIDALLDN'), 
                                                 ))

cmds += generate_cmds('2010:025:14:00:00.000', ( dict(cmd='ACISPKT', tlmsid='WSPOW0EC3E'),
                                                 dict(cmd='ACISPKT', tlmsid='AA00000000')
                                                 ))
cmds += generate_cmds('2010:025:14:00:01.025', ( dict(cmd='ACISPKT', tlmsid='XTZ0000005'), 
                                                 ))

cmds += generate_cmds('2010:026:00:30:00.000', ( dict(cmd='ACISPKT', tlmsid='WSPOW01f1f'),
                                                 ))

cmds += generate_cmds('2010:026:01:36:41.000', ( dict(cmd='ACISPKT', tlmsid='WSPOW00707'), 
                                                 ))

# Day 97 CAP made a strange state at 80W that looks like a fep=3,vid_board=on,clocking=off
cmds += generate_cmds('2010:097:11:45:00.000', ( dict(cmd='ACISPKT', tlmsid='WSPOW00707'),
                                                 dict(cmd='ACISPKT', tlmsid='AA00000000')
                                                 ))



# SCS107s
dates = """
2001:360:06:30:00

2002:331:00:00:00
2002:314:03:32:52
2002:250:17:04:10
2002:236:02:11:29
2002:230:18:43:00
2002:198:12:37:40
2002:143:09:58:00
2002:134:18:10:00
2002:111:05:07:07
2002:109:14:15:00
2002:107:12:54:22
2002:092:22:00:00
2002:077:14:07:09
2002:024:04:17:00
2002:011:03:17:00

2003:336:17:31:10
2003:326:01:24:54
2003:306:20:52:34
2003:301:13:02:04
2003:299:19:22:00
2003:297:13:34:00
2003:213:11:38:00
2003:173:18:48:41
2003:149:22:01:44
2003:128:20:08:06
2003:120:23:37:13
2003:107:20:36:00

2004:315:16:41:00
2004:312:19:54:46
2004:257:21:38:00
2004:215:18:21:00
2004:213:12:01:55
2004:210:19:24:29
2004:208:21:59:00
2004:208:03:54:00
2004:181:19:17:00
2004:021:11:51:00
2004:009:14:29:11

2005:016:14:44:00
2005:257:01:06:00
2005:251:03:44:43
2005:236:22:38:00
2005:234:19:40:20
2005:214:01:28:49
2005:134:13:24:19
2005:016:14:44:00

2006:347:22:45:39
2006:344:08:59:38
2006:340:16:16:00
2006:080:01:32:00

2008:292:21:24:00
2008:225:10:00:00
"""
for date in dates.split('\n'):
    date = date.strip()
    if date:
        print date
        cmds += generate_cmds(date, cmd_set('scs107'))
        cmd_states.interrupt_loads(date, db, current_only=True)


cmd_states.insert_cmds_db(cmds, None, db)
