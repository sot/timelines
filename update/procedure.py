
from Ska.Shell import bash, bash_shell
import re
import Ska.DBI
dbh = Ska.DBI.DBI(dbi='sybase', server='sybase', database='aca_tstdb', user='aca_test')

#bash("./make_old_tables.py --verbose 2 --dbi sybase --server sybase --user aca_test --database aca_tstdb")

#bash("sh dump_cmd_tables.sh")

#bash("sh load_tables.sh")




#cl = bash("sqsh -U aca_test -S sybase -D aca_tstdb -P '400-ZET' -C 'sp_helpconstraint tl_built_loads'")
#constr = ''.join(cl)
#ml = re.search("tl_built_\w+", constr)
#constraint = ml.group(0)
#
#
#
#
#
#
#dbh.execute("alter table tl_built_loads drop constraint %s" % constraint)
#
#dbh.execute("alter table tl_built_loads modify load_scs int not null")
#
#dbh.execute("alter table tl_built_loads add primary key (year, load_segment, file, load_scs, sumfile_modtime)")
#
#dbh.execute("alter table cmd_states add dither varchar(4)")
#
#dbh.execute("""create index cmd_flt_id on cmd_fltpars(cmd_id)""")
#dbh.execute("""create index cmd_int_id on cmd_intpars(cmd_id)""")

#dbh.execute("""insert into load_segments (id, load_segment, year, datestart, datestop,
#load_scs, fixed_by_hand) values (419114466, 'CL103:2002', 2011,
#'2011:103:20:40:00.000', '2011:103:22:57:00.000', 129, 1)""")

#dbh.execute("""insert into timelines (id, load_segment_id, dir,
#datestart, datestop,
#replan, incomplete, fixed_by_hand)
#values (419114466, 419114466, '/2011/APR1111/oflsa/',
#'2011:103:20:40:00.000','2011:103:22:57:00.000',
#0,0,1)""")


#import os
#import sys
#import re
#import mx.DateTime
#import numpy as np
#import shutil
#from Chandra.Time import DateTime
#from Ska.Shell import bash_shell
#err = sys.stderr
#
#import difflib
#
#
#outdir = 'trans'
#
## when to begin simulation
#start_time = mx.DateTime.Date(2011,6,1,0,0,0)
#
#tls = dbh.fetchall("""select id from timelines where datestart >= '%s'"""
#                          % (DateTime(start_time).date))
#for tl in tls:
#    print tl
#    dbh.execute("""delete from cmd_intpars where timeline_id = %d"""
#                % (tl.id))
#    dbh.execute("""delete from cmd_fltpars where timeline_id = %d"""
#                % (tl.id))
#    dbh.execute("""delete from cmds where timeline_id = %d"""
#                % (tl.id))
#    dbh.execute("""delete from timelines where id = %d"""
#                % (tl.id))
#dbh.execute("""delete from load_segments where datestart >= '%s'"""
#            % (DateTime(start_time).date))
#dbh.execute("""delete from cmd_states where datestart >= '%s'"""
#            % (DateTime(start_time).date))
#
## use a clone of the load_segments "time machine"
#
#tm = 'iFOT_time_machine'
#repo_dir = '/proj/sot/ska/data/arc/%s/' % tm
#if not os.path.exists(tm):
#    c_output = bash_shell("hg clone %s" % repo_dir)
#load_rdb = os.path.join(tm, 'load_segment.rdb')
#
## make a local copy of nonload_cmds_archive, modified in test directory by interrupt
#shutil.copyfile('t/nsm_nonload_cmds_archive.py', '%s/nonload_cmds_archive.py' % outdir)
#bash_shell("chmod 755 %s/nonload_cmds_archive.py" % outdir)
#
#
## days between simulated cron task to update timelines
#step = 1
#
#for day_step in np.arange( 0, 28, step):
#    ifot_time = start_time + mx.DateTime.DateTimeDeltaFromDays(day_step)
#    # mercurial python stuff doesn't seem to work.
#    # grab version of iFOT_time_machine just before simulated date
#
#    os.chdir(tm)
#    u_output = bash_shell("""hg update --date "%s%s" """ % (
#        '<', ifot_time.strftime()))
#    os.chdir("..")
#
#
#    # but do some extra work to skip running the whole process on days when
#    # there are no updates from the load segments table
#    if os.path.exists(os.path.join(outdir, 'last_loads.rdb')):
#        last_rdb_lines = open(os.path.join(outdir, 'last_loads.rdb')).readlines()
#        new_rdb_lines = open(load_rdb).readlines()
#        differ = difflib.context_diff(last_rdb_lines, new_rdb_lines)
#        # cheat and just get lines that begin with + or !
#        # (new or changed)
#        change_test = re.compile('^(\+|\!)\s.*')
#        difflines = filter(change_test.match, [line for line in differ])
#        if len(difflines) == 0:
#            err.write("Skipping %s, no additions\n" % ifot_time)
#            continue
#        else:
#            for line in difflines:
#                err.write("%s" % line)
#
#    err.write("Processing %s\n" % ifot_time)
#
#    ## and copy that load_segment file to testing/working directory
#    shutil.copyfile(load_rdb, '%s/loads/%s_loads.rdb' % (outdir, DateTime(ifot_time).date))
#
#    # and copy that load_segment file to the 'last' file
#    shutil.copyfile(load_rdb, '%s/last_loads.rdb' % (outdir))
#
#    load_seg_dir = '%s/loads/' % outdir
#    update_load_seg = os.path.join('./update_load_seg_db.py')
#    load_seg_cmd_str = ( """%s --dbi sybase --server sybase \
#                               --user aca_test --database aca_tstdb \
#                               --test --verbose --loadseg_rdb_dir '%s' """ %
#                         ( update_load_seg, load_seg_dir ))
#    err.write(load_seg_cmd_str + "\n")
#    load_seg_output = bash(load_seg_cmd_str)
#    print "\n".join(load_seg_output)

#

