import os
import sys
import time
import re
import tempfile
import difflib
import shutil
import mx.DateTime
import numpy as np
from itertools import count, izip
from Ska.Shell import bash_shell, bash
import Ska.DBI
import Ska.Table
from Chandra.Time import DateTime
from functools import partial

import update_load_seg_db

err = sys.stderr
MP_DIR = '/data/mpcrit1/mplogs/'


#### actual tests.
def test_loads():
    """
    Build testing states and test against fiducial data

    Return testing generators for each list of loads to be checked
    """

    # load segment files and states to test against
    good = [
        dict(loads='t/2008:048:08:07:00.000.rdb',
                  states='t/2008:048:08:07:00.000.dat'),
             dict(loads='t/2010:023:15:15:00.000.rdb',
                  states='t/2010:023:15:15:00.000.dat'),
             dict(loads='t/2009:164:04:11:15.022.rdb',
                  states='t/2009:164:04:11:15.022.dat'),
             dict(loads='t/2009:193:20:30:02.056.rdb',
                  states='t/2009:193:20:30:02.056.dat'),
             dict(loads='t/2009:214:22:44:19.592.rdb',
                  states='t/2009:214:22:44:19.592.dat'),
             dict(loads='t/july_fixed.rdb',
                  states='t/july_fixed.dat'),
             dict(loads='t/2009:248:12:39:44.351.rdb',
                  states='t/2009:248:12:39:44.351.dat'),
             dict(loads='t/2009:274:22:25:44.351.rdb',
                  states='t/2009:274:22:25:44.351.dat'),
             dict(loads='t/cut_cl110_1409.rdb',
                  states='t/cut_cl110_1409.dat'),
             dict(loads='t/cut_cl304_0504.rdb',
                  states='t/cut_cl304_0504.dat'),
             dict(loads='t/cut_cl352_1208.rdb',
                  states='t/cut_cl352_1208.dat'),
        ]

    for ftest in good:
        load_rdb = ftest['loads']
        state_file = ftest['states']
        err.write("Checking %s\n" % load_rdb )
        f = partial( check_loads, load_rdb, state_file, True)
        f.description = "%s does not match %s states" % (load_rdb, state_file)
        yield(f,)

    # *INCORRECT* fiducial states to check failure
    #bad = [dict(loads='t/july_broken.rdb',
    #            states='t/july_broken.dat')]
    #
    # I don't have a good way to make up broken states with hetg/letg
    # columns handy, so disabling this test for timelines 0.05
    bad = []

    for ftest in bad:
        load_rdb = ftest['loads']
        state_file = ftest['states']
        err.write("Checking %s\n" % load_rdb )
        f = partial( check_loads, load_rdb, state_file, False)
        f.description = "%s unexpectedly matches %s states" % (load_rdb, state_file)
        yield(f,)

def make_states( load_rdb ):
    """
    Setup db and states in a temp directory for quick nose testing

    :param load_rdb: file with load segments
    :rtype: (outdir, dbfilename)
    """

    outdir = re.sub(r'\.rdb$', '', load_rdb)
    if os.path.exists(outdir):
        clean_states(outdir)
    os.makedirs(outdir)
        
    (outdir, load_dir, mp_dir) = data_setup( load_rdb, outdir=outdir )
    dbfilename = os.path.join( outdir, 'test.db3')
    make_table( dbfilename )
    populate_states( outdir, load_dir, mp_dir, dbfilename )
    err.write("Made Test States in %s\n" % outdir )
    return (outdir, dbfilename)

def clean_states( outdir ):
    """
    completely delete testing directory

    :param outdir: testing directory
    """
    shutil.rmtree( outdir )
    err.write("Deleted output directory %s\n" % outdir )

def check_loads( load_rdb, state_file, expect_match=True):
    """
    Setup testing db for a list of load segments
    Write out the "states" created.
    Diff against fiducial list of states

    :param load_rdb: load segment rdb file
    :param state_file: file with fiducial states
    """

    # make new states from load segments
    (outdir, dbfilename) = make_states(load_rdb )
    test_states = text_states( load_rdb, dbfilename)
    ts = open( os.path.join(outdir, 'test_states.dat'), 'w')
    ts.writelines(test_states)
    ts.close()
    # retrieve fiducial states
    if os.path.exists(state_file):
        fl = open(state_file, 'r')
        fiducial_states = fl.readlines()
    else:
        fiducial_states = ''
    # compare
    differ = difflib.context_diff( fiducial_states, test_states )
    difflines = [ line for line in differ ]
    if len(difflines):
        df = open(os.path.join(outdir, 'diffs.txt'), 'w')
        df.writelines(difflines)
        df.close()
        err.write("Diff in dir %s\n" % outdir )
    if expect_match:
        assert len(difflines) == 0 
        err.write("Checked Loads from %s\n" % load_rdb )
        clean_states(outdir)    
    else:
        assert len(difflines) != 0 
        err.write("Expected mismatch in Loads from %s\n" % load_rdb )
        clean_states(outdir)    

def test_load_to_dir():
    good = [dict(weekabr='NOV2408C',
                 week='/2008/NOV2408/oflsc/'),
            dict(weekabr='feb0110c',
                 week='/2010/FEB0110/oflsc/')]

    for ftest in good:
        weekabr = ftest['weekabr']
        week = ftest['week']
        err.write("Checking load_to_dir \n" )
        assert week == load_to_dir(weekabr)

def load_to_dir( week ):
    """
    Given a week from the load segment rdb file in MMMDDYY(V) (NOV2108A) format
    return the likely directory (/2008/NOV2108/oflsa/)
    """

    dir = None
    match = re.match('((\w{3})(\d{2})(\d{2}))(\w{1})', week)
    if match:
        dir = "/20%s/%s/ofls%s/" % (match.group(4), match.group(1).upper(), match.group(5).lower())
    return dir


def data_setup( load_rdb, outdir=tempfile.mkdtemp(), 
                     verbose=False):    
    """
    Read a load segment rdb file
    Figure out which MP directories will be needed
    Make a link tree to those in the output directory
    Copy over the rdb to another directory in that output directory
    Return the directories

    :param load_rdb: rdb file with load segments from arc/get_iFOT_events
    :param outdir: output directory
    :param verbose:

    :rtype: tuple (<output dir>, <mock MP dir>, <mock load seg rdb dir>)

    """
    
    # cheat, and use the real db to get the smallest range that contains
    # all the MP dirs that are listed in the rdb file in the LOADSEG.LOAD_NAME
    # field
    loads = Ska.Table.read_ascii_table(load_rdb, datastart=3)
    db = Ska.DBI.DBI(dbi='sybase', user='aca_read', database='aca', numpy=True, verbose=verbose)
    # get the modification times of the likely directories
    sumfile_modtimes = []
    for load in loads:
        mx_date = DateTime(load['TStart (GMT)']).mxDateTime
        cl = load['LOADSEG.NAME']
        replan_query = """select tp.sumfile_modtime as sumfile_modtime, *
                         from tl_processing as tp, tl_built_loads as tb
                         where tp.file = tb.file
                         and tp.sumfile_modtime = tb.sumfile_modtime
                         and tb.year = %d and tb.load_segment <= "%s"
                         and replan = 0
                         order by tp.sumfile_modtime desc
                         """ % ( mx_date.year, cl )
        replan_match = db.fetchone(replan_query)
        if replan_match:
            sumfile_modtimes.append(replan_match['sumfile_modtime'])
        max_query = """select tp.sumfile_modtime as sumfile_modtime, *
                         from tl_processing as tp, tl_built_loads as tb
                         where tp.file = tb.file
                         and tp.sumfile_modtime = tb.sumfile_modtime
                         and tb.year = %d and tb.load_segment = "%s"
                         order by tp.sumfile_modtime desc
                         """ % ( mx_date.year, cl )
        time_match = db.fetchone(max_query)
        if time_match:
            sumfile_modtimes.append(time_match['sumfile_modtime'])

    # and then fetch the range of products built during this time
    start = min(sumfile_modtimes)
    stop = max(sumfile_modtimes)
    tstart = DateTime( start, format='unix').date
    tstop = DateTime( stop, format='unix').date
#    if verbose:
    print "Fetching mp dirs from %s to %s " % (
        tstart, tstop)
    match_query = """select * from tl_processing
                     where sumfile_modtime >= %s
                     and sumfile_modtime <= %s """ % ( start, stop)
    matches = db.fetchall(match_query)
    match_files = matches.dir

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
                if verbose:
                    print "Linking %s -> %s" % (oldname, newname )

    # copy over load file
    from shutil import copy
    if not os.path.exists(os.path.join(outdir, 'loads')):
        os.mkdir(os.path.join(outdir, 'loads'))

    copy( load_rdb, os.path.join(outdir, 'loads'))

    return ( outdir, os.path.join(outdir, 'loads'), os.path.join(outdir, 'mp'))

def make_table( dbfilename, tstart=None, tstop=None):
    """ 
    Initialize tables

    Use the make_new_tables script to initialize new tables and copy over rows from the 
    "real" sybase tables as needed.

    :param dbfilename: sqlite destination db file
    :param tstart: start of range to copy from sybase
    :param tstart: stop of range to copy from sybase

    """
    if not os.path.exists( dbfilename ):
        make_new_tables = os.path.join('./make_new_tables.py')
        cmd = "%s --server %s " % ( make_new_tables, dbfilename )
        if tstart:
            cmd += " --tstart %s " % DateTime(tstart).date
        if tstop:
            cmd += " --tstop %s " % DateTime(tstop).date
        bash(cmd)

def populate_states( outdir, load_seg_dir, mp_dir, dbfilename, nonload_cmd_file=None, verbose=False ):
    """
    From the available directories and load segment file
    Populate a load segments table and a timelines table
    Insert the nonload commands
    Insert a handful of states from the real database to seed the commands states
    Build states

    :param outdir: output/source/working directory
    :param load_seg_dir: dir with load segment rdb files
    :param mp_dir:  mock or real MP directory
    :param dbfilename: sqlite db file for test database
    :param verbose:

    """

    parse_cmd = os.path.join('./parse_cmd_load_gen.pl')
    parse_cmd_str = ( '%s --touch_file %s --mp_dir %s --server %s' %
                      ( parse_cmd, os.path.join( outdir, 'clg_touchfile'), mp_dir, dbfilename ))
    #err.write(parse_cmd_str + "\n")
    bash(parse_cmd_str)
    #    update_load_seg_db.main(load_seg_dir, test=True, server=dbfilename,
    #                     verbose=True, dryrun=False)
    update_load_seg = os.path.join('./update_load_seg_db.py')
    load_seg_cmd_str = ( "%s --test --server %s --loadseg_rdb_dir '%s'" %
                            ( update_load_seg, dbfilename, load_seg_dir ))
    #err.write(load_seg_cmd_str + "\n")
    load_seg_output = bash(load_seg_cmd_str)

    # update_cmd_states backs up to the first NPNT before the given tstart...
    # backing up doesn't work for this, because we don't have timelines to make 
    # states before the working tstart... 
    # so copy over states from the real db, starting at the start of this
    # timelines timerange until we hit first NPNT state
    # and use the time just after that state as the start of update_cmd_states
    testdb = Ska.DBI.DBI(dbi='sqlite', server=dbfilename, verbose=verbose )
    first_timeline = testdb.fetchone("select datestart from timelines")

    acadb = Ska.DBI.DBI(dbi='sybase', user='aca_read', database='aca', numpy=True, verbose=verbose)
    es_query = """select * from cmd_states
                  where datestop > '%s'
                  order by datestart asc""" % first_timeline['datestart'] 
    db_states = acadb.fetch(es_query)

    from itertools import izip, count
    for state, scount in izip(acadb.fetch(es_query), count()):
        testdb.insert( state, 'cmd_states', replace=True )
        if state['pcad_mode'] == 'NPNT':
            break
    

    cmd_state_mxstart = DateTime( state['datestop'] ).mxDateTime + mx.DateTime.TimeDelta( seconds=1 ) 
    cmd_state_datestart = DateTime(cmd_state_mxstart).date

    #fix_timelines = os.path.join(os.environ['SKA'], 'share', 'timelines', 'fix_timelines.py')
    #bash("%s --server %s " % (fix_timelines, dbfilename ))

    #print "Adding Nonload Commands"
    if nonload_cmd_file is None:
        nonload_cmd_file = os.path.join(os.environ['SKA'], 'share', 'cmd_states', 'nonload_cmds_archive.py')
    bash("%s --server %s " % (nonload_cmd_file, dbfilename ))

    update_cmd_states = os.path.join(os.environ['SKA'], 'share', 'cmd_states', 'update_cmd_states.py')
    #err.write("Running %s --datestart '%s' --server %s --mp_dir %s \n" %
    #          (update_cmd_states, cmd_state_datestart, dbfilename, mp_dir))              
    bash( "%s --datestart '%s' --server %s --mp_dir %s" %
          (update_cmd_states, cmd_state_datestart, dbfilename, mp_dir))

    return dbfilename

def string_states( states):
    """
    Write states recarray out to a string

    :param states: recarray of states
    :rtype: string

    """
    
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
    newcols = sorted(list(states.dtype.names))
    newcols = [ x for x in newcols if (x != 'tstart') & (x != 'tstop')]
    newstates = np.rec.fromarrays([states[x] for x in newcols], names=newcols)
    import Ska.Numpy
    statestring = Ska.Numpy.pformat(newstates, fmt)
    return statestring.splitlines(True)
    
def text_states( load_rdb, dbfile ):
    """
    Fetch states and return as a string

    :param load_rdb: rdb file with load segments
    :param dbfile: database with states
    :rtype: string
    """

    loads = Ska.Table.read_ascii_table(load_rdb, datastart=3)
    datestart = loads[0]['TStart (GMT)']
    datestop = loads[-1]['TStop (GMT)']

    db = Ska.DBI.DBI(dbi='sqlite', server=dbfile, numpy=True )
    test_states = db.fetchall("""select * from cmd_states
                            where datestart >= '%s'
                            and datestart <= '%s'
                            order by datestart""" % (datestart, datestop ))
    test_lines = string_states( test_states )
    return test_lines
def text_cmds( load_rdb, dbfile):
    loads = Ska.Table.read_ascii_table(load_rdb, datastart=3)
    datestart = loads[0]['TStart (GMT)']
    datestop = loads[-1]['TStop (GMT)']

    db = Ska.DBI.DBI(dbi='sqlite', server=dbfile, numpy=False )
    timelines = db.fetchall("""select * from timelines
                                    where datestart >= '%s'
                                    and datestop <= '%s'
                                    order by datestart""" % (datestart, datestop))
    cmds = db.fetchall("""select * from cmds
                            where date >= '%s'
                            and date <= '%s'
                            and timeline_id is NULL
                            order by date""" % (datestart, datestop ))
    for tl in timelines:
        tl_cmds = db.fetchall("""select * from cmds
                                 where timeline_id = %d
                                 and date >= '%s'
                                 and date <= '%s'
                                 order by date""" % (tl['id'], tl['datestart'], tl['datestop']))
        cmds.extend(tl_cmds)
    cmds = sorted(cmds, key=lambda x: x['date'])
    test_lines = ["id\ttimeline_id\tdate\tcmd\ttlmsid\tmsid\tscs\n"]
    for cmd in cmds:
        test_lines.append("%d\t%s\t%s\t%s\t%s\t%s\t%s\n" %
                          (cmd['id'], cmd['timeline_id'], cmd['date'],
                          cmd['cmd'], cmd['tlmsid'], cmd['msid'], cmd['scs']))
    return test_lines

    

def cmp_states(opt, dbfile ):
    """
    Compare sqlite testing database in dbfile with sybase
    Writes out diff

    :param opt: cmd line passthrough that includes opt.outdir and opt.load_rdb
    :param dbfile: sqlite3 filename
    :rtype: None, diff written to opt.outdir/diff.html

    """
    loads = Ska.Table.read_ascii_table(opt.load_rdb, datastart=3)
    datestart = loads[0]['TStart (GMT)']
    datestop = loads[-1]['TStop (GMT)']
    
    db = Ska.DBI.DBI(dbi='sqlite', server=dbfile, numpy=True, verbose=opt.verbose )
    test_states = db.fetchall("""select * from cmd_states
                            where datestart >= '%s'
                            and datestop < '%s'
                            order by datestart""" % (datestart, datestop ))
    test_lines = string_states( test_states )
    ts = open(os.path.join(opt.outdir, 'test_states.dat'), 'w')
    ts.writelines(test_lines)
    ts.close()

    acadb = Ska.DBI.DBI(dbi='sybase', user='aca_read', database='aca', numpy=True, verbose=opt.verbose)    
    acadb_states = acadb.fetchall("""select * from cmd_states
                            where datestart >= '%s'
                            and datestop < '%s'
                            order by datestart""" % (datestart, datestop ))
    db_lines = string_states(acadb_states)
    ds = open(os.path.join(opt.outdir, 'acadb_states.dat'), 'w')
    ds.writelines(db_lines)
    ds.close()


    differ = difflib.context_diff( db_lines, test_lines )
    difflines = [ line for line in differ ]
    if len(difflines):
        diff_out = open(os.path.join(opt.outdir, 'diff.html'), 'w')
        htmldiff = difflib.HtmlDiff()
        htmldiff._styles = htmldiff._styles.replace('Courier', 'monospace')
        diff = htmldiff.make_file( db_lines, test_lines, context=True )
        diff_out.writelines(diff)


def string_timelines(timelines):
    """
    Write timelines recarray out to a string

    :param timelines: recarray of timelines
    :rtype: string

    """

    timeline_strings = []
    for t in timelines:
        tstring = "%s\t%s\t%s\n" % (t['datestart'], t['datestop'], t['dir'])
        timeline_strings.append(tstring)
    return timeline_strings

def cmp_timelines(opt, dbfile ):
    """
    Compare sqlite timelines testing database in dbfile with sybase
    Writes out diff

    :param opt: cmd line passthrough that includes opt.outdir and opt.load_rdb
    :param dbfile: sqlite3 filename
    :rtype: None, diff written to opt.outdir/diff_timelines.html

    """

    loads = Ska.Table.read_ascii_table(opt.load_rdb, datastart=3)
    datestart = loads[0]['TStart (GMT)']
    datestop = loads[-1]['TStop (GMT)']
    
    db = Ska.DBI.DBI(dbi='sqlite', server=dbfile, numpy=True, verbose=opt.verbose )
    test_timelines = db.fetchall("""select * from timelines
                            where datestart >= '%s'
                            and datestop < '%s'
                            order by datestart""" % (datestart, datestop ))
    test_lines = string_timelines( test_timelines )
    ts = open(os.path.join(opt.outdir, 'test_timelines.dat'), 'w')
    ts.writelines(test_lines)
    ts.close()

    acadb = Ska.DBI.DBI(dbi='sybase', user='aca_read', database='aca', numpy=True, verbose=opt.verbose)    
    acadb_timelines = acadb.fetchall("""select * from timelines
                            where datestart >= '%s'
                            and datestop < '%s'
                            order by datestart""" % (datestart, datestop ))
    db_lines = string_timelines( acadb_timelines)
    ds = open(os.path.join(opt.outdir, 'acadb_timelines.dat'), 'w')
    ds.writelines(db_lines)
    ds.close()


    differ = difflib.context_diff( db_lines, test_lines )
    difflines = [ line for line in differ ]
    if len(difflines):
        diff_out = open(os.path.join(opt.outdir, 'diff_timelines.html'), 'w')
        htmldiff = difflib.HtmlDiff()
        htmldiff._styles = htmldiff._styles.replace('Courier', 'monospace')
        diff = htmldiff.make_file( db_lines, test_lines, context=True )
        diff_out.writelines(diff)

def run_model(opt, dbfile):
    """
    Run the psmc twodof model for given states from a sqlite dbfile

    :param opt: cmd line options opt.load_rdb, opt.verbose, opt.outdir
    :param dbfile: sqlite3 filename
    :rtype: dict of just about everything
    """

    import Ska.Table

    loads = Ska.Table.read_ascii_table(opt.load_rdb, datastart=3)
    datestart = loads[0]['TStart (GMT)']
    datestop = loads[-1]['TStop (GMT)']

    db = Ska.DBI.DBI(dbi='sqlite', server=dbfile, numpy=True, verbose=opt.verbose )
    # Add psmc dirs
    psmc_dir = os.path.join(os.environ['SKA'], 'share', 'psmc')
    sys.path.append(psmc_dir)
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

def test_get_built_load():

    dbh = Ska.DBI.DBI(dbi='sybase', user='aca_read', database='aca', numpy=True, verbose=False)

    good = [dict(load={'datestart': '2010:052:01:59:26.450',
                         'datestop': '2010:052:12:20:06.101',
                         'fixed_by_hand': 0,
                         'id': 383104832,
                         'load_scs': 128,
                         'load_segment': 'CL052:0101',
                         'year': 2010},
                 built_load={'file': 'C044_2301.sum',
                               'first_cmd_time': '2010:052:01:59:26.450',
                               'last_cmd_time': '2010:052:12:20:06.101',
                               'load_scs': 128,
                               'load_segment': 'CL052:0101',
                               'sumfile_modtime': 1266030114.0,
                               'year': 2010})]
    for ftest in good:
        load = ftest['load']
        built_load = ftest['built_load']
        err.write("Checking get_built_loads\n")
        assert built_load == update_load_seg_db.get_built_load( load, dbh)


def test_get_processing():

    dbh = Ska.DBI.DBI(dbi='sybase', user='aca_read', database='aca', numpy=True, verbose=False)
 
    good = [dict(built_load={'file': 'C044_2301.sum',
                               'first_cmd_time': '2010:052:01:59:26.450',
                               'last_cmd_time': '2010:052:12:20:06.101',
                               'load_scs': 128,
                               'load_segment': 'CL052:0101',
                               'sumfile_modtime': 1266030114.0,
                               'year': 2010},
                 proc={'bcf_cmd_count': None,
                         'continuity_cmds': None,
                         'dir': '/2010/FEB1310/oflsb/',
                         'execution_tstart': '2010:044:03:01:27.000',
                         'file': 'C044_2301.sum',
                         'planning_tstart': '2010:044:23:00:00.000',
                         'planning_tstop': '2010:052:12:23:00.000',
                         'processing_tstart': '2010:044:23:00:00.000',
                         'processing_tstop': '2010:052:12:23:00.000',
                         'replan': 0,
                         'replan_cmds': None,
                         'sumfile_modtime': 1266030114.0,
                         'year': 2010})]

    for ftest in good:
        built_load = ftest['built_load']
        proc = ftest['proc']
        err.write("Checking get_processing \n")
        assert proc == update_load_seg_db.get_processing( built_load, dbh)

def test_weeks_for_load():

    dbh = Ska.DBI.DBI(dbi='sybase', user='aca_read', database='aca', numpy=True, verbose=False)

    good = [dict(load= {'datestart': '2010:052:01:59:26.450',
                          'datestop': '2010:052:12:20:06.101',
                          'fixed_by_hand': 0,
                          'id': 383104832,
                          'load_scs': 128,
                          'load_segment': 'CL052:0101',
                          'year': 2010},
                 new_timelines= [{'datestart': '2010:052:01:59:26.450',
                                    'datestop': '2010:052:12:20:06.101',
                                    'dir': '/2010/FEB1310/oflsb/',
                                    'incomplete': 0,
                                    'load_segment_id': 383104832,
                                    'replan': 0}])]

    for ftest in good:
        load = ftest['load']
        new_timelines=ftest['new_timelines']
        err.write("Checking weeks_for_load \n" )
        assert new_timelines == update_load_seg_db.weeks_for_load( load, dbh)

def test_nsm_2010(outdir='t/nsm_2010', cmd_state_ska='/proj/sot/ska'):
        
    err.write("Running nsm 2010 simulation \n" )
    # Simulate timelines and cmd_states around day 150 NSM
    dbfilename = os.path.join(outdir, 'test.db3')
    if os.path.exists(outdir):
        clean_states(outdir)
    os.makedirs(outdir)

    # use a clone of the load_segments "time machine"
    tm = 'iFOT_time_machine'
    repo_dir = '/proj/sot/ska/data/arc/%s/' % tm
    if not os.path.exists(tm):
        c_output = bash_shell("hg clone %s" % repo_dir)
    load_rdb = os.path.join(tm, 'load_segment.rdb')


    # make a local copy of nonload_cmds_archive, modified in test directory by interrupt
    shutil.copyfile('t/nsm_nonload_cmds_archive.py', '%s/nonload_cmds_archive.py' % outdir)
    bash_shell("chmod 755 %s/nonload_cmds_archive.py" % outdir)

    # when to begin simulation
    start_time = mx.DateTime.Date(2010,5,30,2,0,0)
    # load interrupt time
    int_time = mx.DateTime.Date(2010,5,30,3,0,0)
    # hours between simulated cron task to update timelines
    step = 12

    for hour_step in np.arange( 0, 72, step):
        ifot_time = start_time + mx.DateTime.DateTimeDelta(0,hour_step)
        # mercurial python stuff doesn't seem to work.
        # grab version of iFOT_time_machine just before simulated date
        
        os.chdir(tm)
        u_output = bash_shell("""hg update --date "%s%s" """ % (
            '<', ifot_time.strftime()))
        os.chdir("..")

        # and copy that load_segment file to testing/working directory
        shutil.copyfile(load_rdb, '%s/%s_loads.rdb' % (outdir, DateTime(ifot_time).date))

        # set up rest of testing components around that load_segment file
        (outdir, load_dir, mp_dir) = data_setup( load_rdb,
                                                 outdir=outdir )
        if not os.path.exists(dbfilename):
            make_table( dbfilename )

        # run the interrupt commanding before running the rest of the commands
        # if the current time is the first simulated cron pass after the
        # actual insertion of the interrupt commands
        dtime = ifot_time - int_time
        if dtime.hours >= 0 and dtime.hours < step:
            print "Performing interrupt"
            bash_shell("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
                       + " --dbi=sqlite --cmd-set nsm "
                       + " --date '%s'" % DateTime(int_time).date
                       + " --interrupt "
                       + " --archive-file %s/nonload_cmds_archive.py " % outdir
                       + " --server %s/test.db3" % outdir )

            bash_shell("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
                       + " --dbi=sqlite --cmd-set scs107 "
                       + " --date '%s'" % DateTime(int_time).date
                       + " --interrupt "
                       + " --archive-file %s/nonload_cmds_archive.py " % outdir
                       + " --server %s/test.db3" % outdir )


        # run the rest of the cron task pieces: parse_cmd_load_gen.pl,
        # update_load_seg_db.py, update_cmd_states.py
        populate_states( outdir, load_dir, mp_dir, dbfilename,
                         "%s/nonload_cmds_archive.py" % outdir)

        prefix = DateTime(ifot_time).date + '_'
        text_files = output_text_files( load_rdb, dbfilename, outdir, prefix=prefix)
        for type in ('timelines', 'states'):
            f = partial( text_compare, text_files[type], 't/%sfid_%s.dat' % (prefix, type), outdir, type)
            f.description = "%s doesn't match %s for %s" % (text_files[type],
                                                            't/%sfid_%s.dat' % (prefix, type), type)
            yield(f,)

    try:
        clean_states(outdir)
    except OSError:
        err.write("Passed all tests, but failed to delete dir %s\n" % outdir)

def output_text_files( load_rdb, dbfilename, outdir, prefix=''):
    
    dbh = Ska.DBI.DBI(dbi='sqlite',server=dbfilename)
    timelines = dbh.fetchall("select * from timelines")
    test_timelines = string_timelines(timelines)
    tfile = os.path.join( outdir,  prefix+'test_timelines.dat')
    tl = open( tfile, 'w')
    tl.writelines(test_timelines)
    tl.close()
    test_states = text_states( load_rdb, dbfilename )
    sfile = os.path.join( outdir, prefix+'test_states.dat')
    ts = open( sfile, 'w')
    ts.writelines(test_states)
    ts.close()
    test_cmds = text_cmds( load_rdb, dbfilename )
    cfile =  os.path.join( outdir, prefix+'test_cmds.dat')
    tc = open( cfile, 'w')
    tc.writelines(test_cmds)
    tc.close()
    return { 'timelines': tfile,
             'states': sfile,
             'cmds': cfile }


def text_compare( testfile, fidfile, outdir, label):
    err.write("Comparing %s and %s \n" % (testfile, fidfile))

    # test lines
    test_lines = open(testfile, 'r').readlines() 

    # retrieve fiducial data
    if os.path.exists(fidfile):
        fiducial_lines = open(fidfile, 'r').readlines()
    else:
        fiducial_lines = ''

    
#    # compare
    differ = difflib.context_diff( fiducial_lines, test_lines )
    difflines = [ line for line in differ ]

    if len(difflines):
        err.write("Error on %s, diff in %s \n" % (testfile,
                                                  os.path.join(outdir, 'diff_%s.html' % label)))
        diff_out = open(os.path.join(outdir, 'diff_%s.html' % label), 'w')
        htmldiff = difflib.HtmlDiff()
        htmldiff._styles = htmldiff._styles.replace('Courier', 'monospace')
        diff = htmldiff.make_file( fiducial_lines, test_lines, context=True )
        diff_out.writelines(diff)
        assert False
        


def test_sosa(outdir='t/sosa_new_plus_cmds', cmd_state_ska=os.environ['SKA']):
        
    err.write("Running sosa 2010 simulation \n" )
    # Simulate timelines and cmd_states around day 150 NSM
    dbfilename = os.path.join(outdir, 'test.db3')
    if os.path.exists(outdir):
        clean_states(outdir)
    os.makedirs(outdir)

    # make a local copy of nonload_cmds_archive, (will be modified in test directory by interrupt)
    shutil.copyfile('t/sosa_nonload_cmds_archive.py', '%s/nonload_cmds_archive.py' % outdir)
    bash_shell("chmod 755 %s/nonload_cmds_archive.py" % outdir)

    load_dir = os.path.join(outdir, 'loads')
    os.makedirs(load_dir)
    shutil.copy('t/sosa.rdb', load_dir)
    load_rdb = os.path.join(load_dir, 'sosa.rdb')

    shutil.copytree('t/sosa_mp', os.path.join(outdir, 'mp'))
    mp_dir = os.path.join(outdir, 'mp')
    
    # set up rest of testing components around that load_segment file
    if not os.path.exists(dbfilename):
        make_table( dbfilename )

    populate_states( outdir, load_dir, mp_dir, dbfilename,
                     "%s/nonload_cmds_archive.py" % outdir)

    
    # write out the timelines, states, and cmds 
    prefix = 'sosa_pre_'
    test_text = output_text_files(load_rdb, dbfilename, outdir, prefix=prefix)
    for type in ('timelines', 'states', 'cmds'):
        f = partial( text_compare, test_text[type], 't/%sfid_%s.dat' % (prefix, type), outdir, type)
        f.description = "%s doesn't match %s for %s" % (test_text[type],
                                                        't/%sfid_%s.dat' % (prefix, type), type)
        yield(f,)

    shutil.copyfile( dbfilename, os.path.join(outdir, 'db_pre_interrupt.db3'))
    

    # a time for the interrupt (chosen in the middle of a segment)
    int_time = '2010:278:20:00:00.000'

    # run the interrupt commanding before running the rest of the commands
    # if the current time is the first simulated cron pass after the
    # actual insertion of the interrupt commands
    #print "Performing SCS107 interrupt"
    interrupt_cmd = ("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
               + " --dbi=sqlite --cmd-set scs107 "
               + " --date '%s'" % DateTime(int_time).date
               + " --interrupt_observing "
               + " --archive-file %s/nonload_cmds_archive.py " % outdir
               + " --server %s/test.db3" % outdir )
    #err.write(interrupt_cmd + "\n")
    bash_shell(interrupt_cmd)

    # run the rest of the cron task pieces: parse_cmd_load_gen.pl,
    # update_load_seg_db.py, update_cmd_states.py
    populate_states( outdir, load_dir, mp_dir, dbfilename,
                     "%s/nonload_cmds_archive.py" % outdir)
    
    # write out the timelines and states after the tasks are complete
    prefix = 'sosa_post_'
    test_text = output_text_files(load_rdb, dbfilename, outdir, prefix=prefix)
    for type in ('timelines', 'states', 'cmds'):
        f = partial( text_compare, test_text[type], 't/%sfid_%s.dat' % (prefix, type), outdir, type)
        f.description = "%s doesn't match %s for %s" % (test_text[type],
                                                        't/%sfid_%s.dat' % (prefix, type), type)
        yield(f,)
        
    shutil.copyfile( dbfilename, os.path.join(outdir, 'db_post_interrupt.db3'))


    # if we didn't fail an assert, remove the testing directory
    try:
        clean_states(outdir)
    except OSError:
        err.write("Passed all tests, but failed to delete dir %s" % outdir)
