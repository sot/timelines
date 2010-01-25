#!/usr/bin/env python
import os
import sys
import time
import re
import tempfile
import mx.DateTime
import numpy as np
from itertools import count, izip
from Ska.Shell import bash_shell
import Ska.DBI
import Ska.Table
from Chandra.Time import DateTime


MP_DIR = '/data/mpcrit1/mplogs/'


def load_to_dir( week ):
    """
    Given a week from the load segment rdb file in MMMDDYY(V) (NOV2108A) format
    return the likely directory (/2008/NOV2108/oflsa/)
    """

    dir = None
    match = re.match('((\w{3})(\d{2})(\d{2}))(\w{1})', week)
    if match:
        dir = "/20%s/%s/ofls%s/" % (match.group(4), match.group(1), match.group(5).lower())
    return dir


def test_data_setup( opt ):    
    """
    Read a load segment rdb file
    Figure out which MP directories will be needed
    Make a link tree to those in the output directory
    Copy over the rdb to another directory in that output directory
    Return the directories
    """
    
    if opt.load_rdb is None:
        raise ValueError('load_rdb should be supplied as option')
    outdir = opt.outdir
    if outdir is None:
        outdir = tempfile.mkdtemp()
    if opt.tstart:
        tstart = DateTime(opt.tstart)
    if opt.tstop:
        tstop = DateTime(opt.tstop)
    if opt.load_rdb:
        # cheat, and use the db to get the smallest range that contains
        # all the MP dirs that are listed in the rdb file in the LOADSEG.LOAD_NAME
        # field
        loads = Ska.Table.read_ascii_table(opt.load_rdb, datastart=3)
        db = Ska.DBI.DBI(dbi='sybase', numpy=True, verbose=opt.verbose)
        sumfile_modtimes = []
        for week in np.unique(loads['LOADSEG.LOAD_NAME']):
            week_dir = load_to_dir(week)
            time_query = """select sumfile_modtime from tl_processing 
                            where dir = "%s" """ % ( week_dir )
            time_matches = db.fetchall(time_query)
            sumfile_modtimes.append(time_matches[0]['sumfile_modtime'])
        start = min(sumfile_modtimes)
        stop = max(sumfile_modtimes)
        opt.tstart = DateTime( start, format='unix').date
        opt.tstop = DateTime( stop, format='unix').date
        if opt.verbose:
            print "Fetching mp dirs from %s to %s " % (
                opt.tstart, opt.tstop)
        match_query = """select * from tl_processing
                         where sumfile_modtime >= %s
                         and sumfile_modtime <= %s """ % ( start, stop)
        matches = db.fetchall(match_query)
        match_files = matches.dir
    else:
        # otherwise, hope the user gave a tstart and tstop and just find the directories
        # that match that range
        import glob
        all_files = glob.glob(os.path.join(MP_DIR, '????', '???????', 'ofls?', 'mps', 'C???_????.sum'))
        match_files = []
        for file in all_files:
            s = os.stat(file)
            if ((s.st_mtime >= tstart.unix) and (s.st_mtime < tstop.unix )):
                match_files.append(file)
    
    for m in match_files:
        dirmatch = re.search('(\d{4})\/(\w{7})/(ofls\w)', m)
        if dirmatch:
            year = dirmatch.group(1)
            week = dirmatch.group(2)
            rev = dirmatch.group(3)
            if not os.path.exists(os.path.join(outdir, 'mp', year, week)):
                os.makedirs( os.path.join( outdir, 'mp', year, week ))
            oldname = os.path.join('/data/mpcrit1/mplogs/', year, week, rev)
            newname = os.path.join( outdir, 'mp', year, week, rev )
            if not os.path.exists(newname):
                os.symlink(oldname, newname)
                if opt.verbose:
                    print "Linking %s -> %s" % (oldname, newname )

    # copy over load file
    from shutil import copy
    if not os.path.exists(os.path.join(outdir, 'loads')):
        os.mkdir(os.path.join(outdir, 'loads'))
    copy( opt.load_rdb, os.path.join(outdir, 'loads'))

    return ( outdir, os.path.join(outdir, 'loads'), os.path.join(outdir, 'mp'))

def make_table( dbfilename, opt=None):
    """ 
    Initialize tables
    """

    if not os.path.exists( dbfilename ):
        # tstart = tstop to get no import of tables
        make_new_tables = os.path.join(os.environ['SKA'], 'share', 'timelines', 'make_new_tables.py')
        bash_shell( '%s --server %s --tstart %s --tstop %s' % 
                    ( make_new_tables, dbfilename , opt.tstop, opt.tstop ))


def make_states( outdir, load_dir, mp_dir, dbfilename, opt=None ):
    """
    From the available directories and load segment file
    Populate a load segments table and a timelines table
    Insert the nonload commands
    Insert a handful of states from the real database to seed the commands states
    Build states
    """

    parse_cmd = os.path.join(os.environ['SKA'], 'share', 'timelines', 'parse_cmd_load_gen.pl')
    bash_shell( '%s --touch_file %s --mp_dir %s --server %s' %
                ( parse_cmd, os.path.join( outdir, 'clg_touchfile'), mp_dir, dbfilename ))

    update_load_seg = os.path.join(os.environ['SKA'], 'share', 'timelines', 'update_load_seg_db.py')
    bash_shell( '%s --test --server %s --loadseg_rdb_dir %s --verbose' %
                ( update_load_seg, dbfilename, load_dir ))
#    bash_shell( './broken_load_seg_db.py --server %s --loadseg_rdb_dir %s ' %
#                ( dbfilename, load_dir ))


    # update_cmd_states backs up to the first NPNT before the given tstart...
    # backing up doesn't work for this, because we don't have timelines to make 
    # states before the working tstart... 
    # so copy over states from the real db, starting at the start of this
    # timelines timerange until we hit first NPNT state
    # and use the time just after that state as the start of update_cmd_states
    testdb = Ska.DBI.DBI(dbi='sqlite', server=dbfilename, verbose=opt.verbose )
    first_timeline = testdb.fetchone("select datestart from timelines")

    acadb = Ska.DBI.DBI(dbi='sybase', user='aca_read', numpy=True, verbose=True)
    es_query = """select * from cmd_states
                  where datestop > '%s'
                  order by datestart asc""" % first_timeline['datestart'] 
    db_states = acadb.fetch(es_query)

    from itertools import izip, count
    for state, scount in izip(acadb.fetch(es_query), count()):
        testdb.insert( state, 'cmd_states', replace=True )
        if state['pcad_mode'] == 'NPNT':
            break
    
    print "Copied %d states from real table into test db" % (scount+1) 
    cmd_state_mxstart = DateTime( state['datestop'] ).mxDateTime + mx.DateTime.TimeDelta( seconds=1 ) 
    cmd_state_datestart = DateTime(cmd_state_mxstart).date
#    bash_shell( "/proj/gads6/jeanproj/cmd_states/update_cmd_states.py --datestart '%s' --server %s" %
#                (cmd_state_datestart, dbfilename))
    print "Adding Nonload Commands"
    nonload_cmds = os.path.join(os.environ['SKA'], 'share', 'cmd_states', 'nonload_cmds_archive.py')
    bash_shell("%s --server %s " % (nonload_cmds, dbfilename ))

    print "Updating States from %s" % cmd_state_datestart
    update_cmd_states = os.path.join(os.environ['SKA'], 'share', 'cmd_states', 'update_cmd_states.py')
    bash_shell( "%s --datestart '%s' --server %s" %
                (update_cmd_states, cmd_state_datestart, dbfilename))

    return dbfilename


def write_states( states, outfile):
    """Write states recarray to file states.dat"""
    
    out = open(outfile, 'w')
    fmt = {'power': '%.1f',
           'pitch': '%.2f',
           'tstart': '%.2f',
           'tstop': '%.2f',
           'ra': '%.2f',
           'dec' : '%.2f',
           'roll' : '%.2f',
           'q1' : '%.2f',
           'q2' : '%.2f',
           'q3' : '%.2f',
           'q4' : '%.2f',
           }
    newcols = list(states.dtype.names)
    newcols = [ x for x in newcols if (x != 'tstart') & (x != 'tstop')]
    newstates = np.rec.fromarrays([states[x] for x in newcols], names=newcols)
    import Ska.Numpy
    Ska.Numpy.pprint(newstates, fmt, out)
    out.close()
    

def cmp_states(opt, dbfile ):


    loads = Ska.Table.read_ascii_table(opt.load_rdb, datastart=3)
    datestart = loads[0]['TStart (GMT)']
    datestop = loads[-1]['TStop (GMT)']
    
    db = Ska.DBI.DBI(dbi='sqlite', server=dbfile, numpy=True, verbose=opt.verbose )
    test_states = db.fetchall("""select * from cmd_states
                            where datestart >= '%s'
                            and datestart <= '%s'
                            order by datestart""" % (datestart, datestop ))
    write_states( test_states, os.path.join(opt.outdir, 'test_states.dat'))

    acadb = Ska.DBI.DBI(dbi='sybase', user='aca_read', numpy=True, verbose=True)    
    acadb_states = acadb.fetchall("""select * from cmd_states
                            where datestart >= '%s'
                            and datestart <= '%s'
                            order by datestart""" % (datestart, datestop ))
    write_states(acadb_states, os.path.join(opt.outdir, 'acadb_states.dat'))


                  

#    pass
#    if len(db_states) > 0:
#         # Get states columns that are not float type. descr gives list of (colname, type_descr)
#        match_cols = [x[0] for x in states.dtype.descr if 'f' not in x[1]]
#
#        # Find mismatches: direct compare or where pitch or attitude differs by > 1 arcsec
#        for i_diff, db_state, state in izip(count(), db_states, states):
#            for x in match_cols:
#                if db_state[x] != state[x]:
#                    print x
#                    print db_state[x]
#                    print state[x]
#                    break
#    else:
#        return
#          # made it with no mismatches so no action required
#
#    else:
#        i_diff = 0
#
#    print "mismatch at %d" % i_diff

#    # Mismatch occured at i_diff.  Drop cmd_states after states[i_diff].datestart
#    cmd = "DELETE FROM cmd_states WHERE datestart >= '%s'" % states[i_diff].datestart
#    logging.info('udpate_states_db: ' + cmd)
#    db.execute(cmd)
#
#    # Insert new states[i_diff:] into cmd_states 
#    logging.info('udpate_states_db: inserting states[%d:%d] to cmd_states' %
#                  (i_diff, len(states)+1))
#    for state in states[i_diff:]:
#        db.insert(dict((x, state[x]) for x in state.dtype.names), 'cmd_states', commit=True)
#    db.commit()

#def cmp_cmds( db ):
#
#    test_cmds = db.fetchall('select * from cmds order by date')
#    real_cmds = acadb.fetchall("""select * from cmds
#                                 where date >= '%s'
#                                 and date <= '%s'
#                                 order by date""" % ( test_cmds[0]['date'],
#                                                         test_cmds[-1]['date'] ))
#    
#    # Get states columns that are not float type. descr gives list of (colname, type_descr)
##    match_cols = [x[0] for x in test_cmds.dtype.descr if 'f' not in x[1]]
#    match_cols = [ 'timeline_id', 'date', 'cmd', 'tlmsid', 'msid', 'vcdu', 'step', 'scs']
#    
#     # Find mismatches: direct compare or where pitch or attitude differs by > 1 arcsec
#    for i_diff, real_cmd, cmd in izip(count(), real_cmds, test_cmds):
#        for x in match_cols:
#            if real_cmd[x] != cmd[x]:
#                print x
#                print real_cmd[x]
#                print cmd[x]
#                break
#    else:
#        # made it with no mismatches so no action required
#        return
#
#    print "mismatch at %d" % i_diff



def run_model(opt, dbfile):

    import Ska.Table

    loads = Ska.Table.read_ascii_table(opt.load_rdb, datastart=3)
    datestart = loads[0]['TStart (GMT)']
    datestop = loads[-1]['TStop (GMT)']

    db = Ska.DBI.DBI(dbi='sqlite', server=dbfile, numpy=True, verbose=opt.verbose )
    # Add psmc dirs
#    psmc_dir = os.path.join(os.environ['SKA'], 'share', 'psmc')
#    sys.path.append(psmc_dir)
    import Chandra.cmd_states as cmd_states
    import Ska.TelemArchive.fetch
    import numpy as np
    states = db.fetchall("""select * from cmd_states
                            where datestart >= '%s'
                            and datestart <= '%s'
                            order by datestart""" % (datestart, datestop ))
    colspecs = ['1pdeaat', '1pin1at',
                'tscpos', 'aosares1',
                '1de28avo', '1deicacu',
                '1dp28avo', '1dpicacu',
                '1dp28bvo', '1dpicbcu']

    print "Fetching from %s to %s" % ( states[0]['datestart'], states[-2]['datestart'])
    colnames, vals = Ska.TelemArchive.fetch.fetch( start=states[0]['datestart'],
                                                   stop=states[-2]['datestart'],
                                                   dt=32.8,
                                                   colspecs=colspecs )

    tlm = np.rec.fromrecords( vals, names=colnames )
    import psmc_check
    plots_validation = psmc_check.make_validation_plots( opt, tlm, db )
    valid_viols = psmc_check.make_validation_viols(plots_validation)
    import characteristics
    MSID = dict(dea='1PDEAAT', pin='1PIN1AT')
    YELLOW = dict(dea=characteristics.T_dea_yellow, pin=characteristics.T_pin_yellow)
    MARGIN = dict(dea=characteristics.T_dea_margin, pin=characteristics.T_pin_margin)
    proc = dict(run_user=os.environ['USER'],
                run_time=time.ctime(),
                errors=[],
                dea_limit=YELLOW['dea'] - MARGIN['dea'],
                pin_limit=YELLOW['pin'] - MARGIN['pin'],
                )

    pred = dict(plots=None, viols=None, times=None, states=None, temps=None)  
    psmc_check.write_index_rst(opt, proc, plots_validation, valid_viols=valid_viols, 
                               plots=pred['plots'], viols=pred['viols'])
    psmc_check.rst_to_html(opt, proc)
    
    return dict(opt=opt, states=pred['states'], times=pred['times'],
                temps=pred['temps'], plots=pred['plots'],
                viols=pred['viols'], proc=proc, 
                plots_validation=plots_validation)





#t0 = '1999:001:00:00:00.000'
#
#
#def tester( dbfilename, dirs, load_rdb_text, datestart, testdir=None):
#    if testdir==None:
#        testdir = tempfile.mkdtemp()
#    dbfilename = os.path.join( testdir, dbfilename )
#    mpdir = os.path.join(testdir, 'mp')
#    if not os.path.exists(mpdir):
#        os.mkdir(mpdir)
#    for dir in dirs:
#        dirmatch = re.match('(\d{4})\/(\w{7})', dir)
#        if dirmatch:
#            year = dirmatch.group(1)
#            week = dirmatch.group(2)
#            if not os.path.exists(os.path.join(mpdir, year)):
#                os.mkdir( os.path.join( mpdir, year))
#            oldname = os.path.join('/data/mpcrit1/mplogs/', year, week)
#            newname = os.path.join( mpdir, year, week )
#            if not os.path.exists(newname):
#                os.symlink(oldname, newname)
#    if not os.path.exists(os.path.join(testdir, 'load_rdb')):
#        os.mkdir(os.path.join(testdir, 'load_rdb'))
#    f=open(os.path.join(testdir, 'load_rdb', 'test.rdb'), 'w')
#    f.write(load_rdb_text)
#    f.close()
#    

         





