import os
import sys
import time
import re
import tempfile
import difflib
import shutil
import numpy as np
import mercurial.ui
import mercurial.hg
import mercurial.commands
from itertools import count, izip
from Ska.Shell import bash_shell, bash
import Ska.DBI
import Ska.Table
import asciitable
from Chandra.Time import DateTime
from functools import partial
from shutil import copy
import tables
import pytest
from astropy.table import Table
from kadi import events
from Ska.astro import sph_dist
#from Ska.Numpy import pprint

from mica.archive import obspar
import update_load_seg_db

err = sys.stderr
MP_DIR = '/data/mpcrit1/mplogs/'
SKA = os.environ['SKA']
verbose = True
DBI = 'sybase'
cleanup = True


def time_machine_update(date):
    """
    Clone the arc ifot time machine as needed and
    update to the version before the requested date.
    """
    hg_time_str = "<{}".format(time.strftime("%c", time.gmtime(DateTime(date).unix)))
    hg_ui = mercurial.ui.ui()
    # use a clone of the arc "time machine"
    tm = 'iFOT_time_machine'
    repo_dir = '/proj/sot/ska/data/arc/%s/' % tm
    # just make the time machine repo in the current directory
    if not os.path.exists(tm):
        mercurial.hg.clone(hg_ui, repo_dir, tm)
    repo = mercurial.hg.repository(hg_ui, tm)
    mercurial.commands.update(hg_ui, repo, date=hg_time_str)


def pprint(recarray, fmt=None, cols=None, out=sys.stdout):
    """
    Print a nicely-formatted version of ``recarray`` to ``out`` file-like object.
    If ``fmt`` is provided it should be a dict of ``colname:fmt_spec`` pairs where
    ``fmt_spec`` is a format specifier (e.g. '%5.2f').

    :param recarray: input record array
    :param fmt: dict of format specifiers (optional)
    :param cols:
    :param out: output file-like object

    :rtype: None
    """

    # Define a dict of pretty-print functions for each column in fmt
    if fmt is None:
        pprint = {}
    else:
        pprint = dict((colname, lambda x: fmt[colname] % x) for colname in fmt)

    colnames = cols
    if colnames == None:
        colnames = recarray.dtype.names

    # Pretty-print all columns and turn into another recarray made of strings
    str_recarray = []
    for row in recarray:
        str_recarray.append([pprint.get(x, str)(row[x]) for x in colnames])
    str_recarray = np.rec.fromrecords(str_recarray, names=colnames)

    # Parse the descr fields of str_recarray recarray to get field width
    colfmt = {}
    for descr in str_recarray.dtype.descr:
        colname, coldescr = descr
        collen = max(int(re.search(r'\d+', coldescr).group()), len(colname))
        colfmt[colname] = '%-' + str(collen) + 's'
    
    # Finally print everything to out
    print >>out, ' '.join(colfmt[x] % x for x in colnames)
    for row in str_recarray:
        print >>out, ' '.join(colfmt[x] % row[x] for x in colnames)


def find_mp_files(loads):
    """
    Find the MP directories that correspond to the list of loads supplied.
    Uses flight/sybase.aca database tables of the parsed files.

    :param loads: list or recarray of load segments
    :rtype: list of directories
    """
    

    db = Ska.DBI.DBI(dbi='sybase', user='aca_read', database='aca', numpy=True)
    # get the modification times of the likely directories
    sumfile_modtimes = []
    for load in loads:
        tstart_date = DateTime(load['TStart (GMT)'])
        cl = load['LOADSEG.NAME']
        replan_query = """select tp.sumfile_modtime as sumfile_modtime, *
            from tl_processing as tp, tl_built_loads as tb
                         where tp.file = tb.file
                         and tp.sumfile_modtime = tb.sumfile_modtime
                         and tb.year = %d and tb.load_segment <= "%s"
                         and replan = 0
                         order by tp.sumfile_modtime desc
                         """ % ( int(tstart_date.frac_year), cl )
        replan_match = db.fetchone(replan_query)
        if replan_match:
            sumfile_modtimes.append(replan_match['sumfile_modtime'])
        max_query = """select tp.sumfile_modtime as sumfile_modtime, *
                         from tl_processing as tp, tl_built_loads as tb
                         where tp.file = tb.file
                         and tp.sumfile_modtime = tb.sumfile_modtime
                         and tb.year = %d and tb.load_segment = "%s"
                         order by tp.sumfile_modtime desc
                         """ % ( int(tstart_date.frac_year), cl )
        time_match = db.fetchone(max_query)
        if time_match:
            sumfile_modtimes.append(time_match['sumfile_modtime'])

    # and then fetch the range of products built during this time
    start = min(sumfile_modtimes)
    stop = max(sumfile_modtimes)
    tstart = DateTime( start, format='unix').date
    tstop = DateTime( stop, format='unix').date
    #    if verbose:
    err.write("Fetching mp dirs from %s to %s \n" % (
        tstart, tstop))
    match_query = """select * from tl_processing
                     where sumfile_modtime >= %s
                     and sumfile_modtime <= %s """ % ( start, stop)
    matches = db.fetchall(match_query)
    match_files = matches.dir
    return match_files


def text_compare( testfile, fidfile, outdir, label):
    """
    Diff two files and write html diff to directory.

    :param testfile: file generated by test
    :param fidfile: fiducial file for comparison
    :param outdir: directory for html diff if any
    :param label: label for html diff
    :rtype: boolean, True if files match

    """

    err.write("Comparing %s and %s \n" % (testfile, fidfile))

    # test lines
    test_lines = open(testfile, 'r').readlines() 

    # retrieve fiducial data
    if os.path.exists(fidfile):
        fiducial_lines = open(fidfile, 'r').readlines()
    else:
        fiducial_lines = ''

    # compare
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
        return False

    return True


class Scenario(object):
    """
    Test scenario object to hold database and directory setup for a test.
    """

    def __init__(self, outdir, dbi=DBI):
        self.outdir = outdir # where to put everything
        self.h5file = os.path.join(self.outdir, 'cmd_states.h5')
        self.dbi = dbi # which dbi
        self.db_initialized = False # have we created the tables
        self.first_npnt_state = None # have we copied over the states before first NPNT and have a npnt state
        self.run_at_time = None # if scenario used more than once in time, what is the pretend run time
        self.load_rdb = None # load segment rdb file from iFOT
        self.mp_dir = None # directory with load products tree
        self.load_dir = None # directory with load segment rdb files
        if os.path.exists(outdir):
            shutil.rmtree(outdir)
            err.write("Pre-cleaned by deleting dir %s\n" % outdir)
        os.makedirs(outdir)
        self.dbfile = os.path.join(outdir, 'test.db3')

    def data_setup(self):
        """
        Read a load segment rdb file (from self.load_rdb)
        Figure out which MP directories will be needed
        Make a link tree to those in the output directory (self.outdir)
        Define self.mp_dir and self.load_dir in self.outdir
        Copy over the rdb to another directory in that output directory

        """

        loads = Ska.Table.read_ascii_table(self.load_rdb, datastart=3)
        outdir = self.outdir
        # cheat, and use the real db to get the smallest range that contains
        # all the MP dirs that are listed in the rdb file in the LOADSEG.LOAD_NAME
        # field
        match_files = find_mp_files(loads)

        # gin up a directory tree in outdir
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
                        err.write("Linking %s -> %s \n" % (oldname, newname ))

        # copy over load file
        if not os.path.exists(os.path.join(outdir, 'loads')):
            os.mkdir(os.path.join(outdir, 'loads'))
        copy( self.load_rdb, os.path.join(outdir, 'loads'))
        self.load_dir = os.path.join(outdir, 'loads')
        self.mp_dir = os.path.join(outdir, 'mp')
        return

    def db_handle(self):
        if hasattr(self, 'dbh'):
            return self.dbh
        if self.dbi == 'sybase':
            self.dbh = Ska.DBI.DBI(dbi='sybase', server='sybase',
                                   user='aca_test', database='aca_tstdb')
        else:
            self.dbh = Ska.DBI.DBI(dbi='sqlite', server=self.dbfile)
        return self.dbh

    def db_cmd_str(self):
        if hasattr(self, 'db_str'):
            return self.db_str
        if self.dbi == 'sybase':
            self.db_str = " --dbi sybase --server sybase --user aca_test --database aca_tstdb "
        else:
            self.db_str = ' --server %s ' % self.dbfile
        return self.db_str

    def db_wipe(self):
        self.db_setup(wipe=True)

    def cleanup(self):
        if not cleanup:
            err.write("Cleanup not run due to cleanup=False")
            return
        self.db_wipe()
        self.dbh.conn.close()
        try:
            shutil.rmtree( self.outdir )
            err.write("Deleted output directory %s\n" % self.outdir )
        except OSError:
            err.write("Passed all tests, but failed to delete dir %s\n" % self.outdir)


    def db_setup(self, wipe=False, tstart=None, tstop=None):
        """ 
        Initialize tables or delete them

        Use the make_new_tables script to initialize new tables and copy over rows from the 
        "real" sybase tables as needed.

        :param tstart: start of range to copy from sybase
        :param tstart: stop of range to copy from sybase
        :param wipe: just delete the tables
        """
        make_new_tables = os.path.join('./make_new_tables.py')

        if wipe:
            make_new_tables += " --wipe "

        testdb = self.db_handle()
        db_str = self.db_cmd_str()

        cmd = "%s %s" % (make_new_tables, db_str)

        if tstart:
            cmd += " --tstart %s " % DateTime(tstart).date
        if tstop:
            cmd += " --tstop %s " % DateTime(tstop).date

        err.write("%s \n" % cmd)
        bash(cmd)

        self.db_initialized = True


    def consistent_with_data(self):
        load_rdb = self.load_rdb
        loads = Ska.Table.read_ascii_table(load_rdb, datastart=3)
        check_datestart = self.first_npnt_state['datestop']
        check_datestop = loads[-1]['TStop (GMT)']

        db_states = Table(self.db_handle().fetchall(
                'select * from cmd_states where datestart > "{}" and datestart  < "{}"'.format(
                    check_datestart, check_datestop)))
        # should probably group these
        for obsid in np.unique(db_states['obsid']):

            idxs = np.flatnonzero(db_states['obsid'] == obsid)
            if (0 in idxs) or ((len(db_states) - 1) in idxs):
                continue # skip the first one and the last one
            start_obs = db_states[idxs][0]['datestart']
            stop_obs = db_states[idxs][-1]['datestop']
            k_obs = events.obsids.filter(obsid=obsid)[0]
            assert abs(DateTime(k_obs.start).secs - DateTime(start_obs).secs) < 5
            assert abs(DateTime(k_obs.stop).secs - DateTime(stop_obs).secs) < 5
        for idx, state in enumerate(db_states):
            if idx == 0:
                continue
            if state['pcad_mode'] == 'NPNT' and db_states[idx - 1]['pcad_mode'] != 'NPNT':
                manvr = events.manvrs.filter(obsid=state['obsid'])[0]
                assert sph_dist(manvr.stop_ra, manvr.stop_dec, state['ra'], state['dec']) < .5
            if state['pcad_mode'] == 'NPNT' and state['clocking'] == 1 and state['obsid'] < 40000:
                obsinfo = obspar.get_obspar(state['obsid'])
                assert obsinfo['num_ccd_on'] == state['ccd_count']
        err.write("Obsid times, CCD counts, Pointings consistent between kadi and states\n")
        return True


        
    def check_hdf5_sql_consistent(self):
        """Check that all HDF5 string and int values match corresponding SQL
        values.
        """
        load_rdb = self.load_rdb
        loads = Ska.Table.read_ascii_table(load_rdb, datastart=3)
        check_datestart = self.first_npnt_state['datestop']
        check_datestop = loads[-1]['TStop (GMT)']

        h5 = tables.openFile(self.h5file, mode='r')
        db = self.db_handle()
        sql_rows = db.fetchall('select * from cmd_states')
        h5_rows = h5.root.data[:]
        # just check string and int cols
        colnames = ('datestart', 'datestop', 'obsid', 'power_cmd', 'si_mode', 'pcad_mode',
                    'vid_board', 'clocking', 'fep_count', 'ccd_count', 'trans_keys',
                    'hetg', 'letg', 'dither')
        sql_ok = sql_rows['datestart'] >= check_datestart
        h5_ok = h5_rows['datestart'] >= check_datestart
        for name in colnames:
            match = np.all(sql_rows[sql_ok][name] == h5_rows[h5_ok][name])
            if not match:
                err.write('FAILED MATCH for {}\n'.format(name))
                err.write('SQL: {}\n'.format(sql_rows[name]))
                err.write('HDF5: {}\n'.format(h5_rows[name]))
                return False

        err.write('All HDF5 string and int values match corresponding SQL values\n')
        h5.close()
        return True

    def populate_states( self,
                         nonload_cmd_file=None, verbose=False ):
        """
        From the available directories and load segment file
        Populate a load segments table and a timelines table
        Insert the nonload commands
        Insert a handful of states from the real database to seed the commands states
        Build states

        :param nonload_cmd_file: a nonload_cmd_file for the test
        :param verbose: verbosity True/Fals

        """

        outdir = self.outdir
        mp_dir = self.mp_dir
        load_seg_dir = self.load_dir
        db_str = self.db_cmd_str()
        testdb = self.db_handle()

        parse_cmd = os.path.join('./parse_cmd_load_gen.pl')
        parse_cmd_str = ( '%s %s --touch_file %s --mp_dir %s ' %
                          ( parse_cmd, db_str, os.path.join( outdir, 'clg_touchfile'), mp_dir))

        err.write(parse_cmd_str + "\n")
        bash(parse_cmd_str)

        update_load_seg = os.path.join('./update_load_seg_db.py')
        load_seg_cmd_str = ( "%s %s --test --loadseg_rdb_dir '%s'" %
                                ( update_load_seg, db_str, load_seg_dir ))
        err.write(load_seg_cmd_str + "\n")
        load_seg_output = bash(load_seg_cmd_str)

        # update_cmd_states backs up to the first NPNT before the given tstart...
        # backing up doesn't work for this, because we don't have timelines to make 
        # states before the working tstart... 
        # so copy over states from the real db, starting at the start of this
        # timelines timerange until we hit first NPNT state
        # and use the time just after that state as the start of update_cmd_states
        first_timeline = testdb.fetchone("select datestart from timelines")
        last_timeline = testdb.fetchone("select datestop from timelines order by datestart desc")

        if self.first_npnt_state is None:
            acadb = Ska.DBI.DBI(dbi='sybase', user='aca_read', database='aca',
                                numpy=True, verbose=verbose)
            es_query = """select * from cmd_states
                          where datestop > '%s' and datestart < '%s'
                          order by datestart asc""" % (first_timeline['datestart'], last_timeline['datestop'])
            db_states = acadb.fetchall(es_query)
            last_state = None
            for idx, state in enumerate(db_states):
                if state['pcad_mode'] != 'NPNT' and db_states[idx - 1]['pcad_mode'] == 'NPNT':
                    break
                testdb.insert(state, 'cmd_states' )
                last_state = state

            cmd_state_start = DateTime(last_state['datestop']).secs + .001
            cmd_state_datestart = DateTime(cmd_state_start).date
            self.first_npnt_state = last_state

        else:
            # after the database has been set up the first time,
            # use run_at_time - 10 days (approximate the update_cmd_states.py default)
            # unless the timelines don't go back a full ten days
            if self.run_at_time is None:
                raise ValueError("self.run_at_time must be defined")
            run_start = DateTime(self.run_at_time) - 10
            npnt_start = DateTime(self.first_npnt_state['datestop'])
            cmd_state_start = max(npnt_start.secs, run_start.secs) + .001
            cmd_state_datestart = DateTime(cmd_state_start).date


        #fix_timelines = os.path.join(os.environ['SKA'], 'share', 'timelines', 'fix_timelines.py')
        #bash("%s --server %s " % (fix_timelines, dbfilename ))

        if nonload_cmd_file is None:
            nonload_cmd_file = os.path.join(os.environ['SKA'], 'share',
                                            'cmd_states', 'nonload_cmds_archive.py')

        # insert nonload cmds
        err.write("%s %s \n" % (nonload_cmd_file, db_str))
        bash("%s %s " % (nonload_cmd_file, db_str ))

        # Delete any cmds after region of interest
        late_cmds = testdb.fetchall("select * from cmds where date > '{}'".format(
                last_timeline['datestop']))
        for cmd in late_cmds:
            testdb.execute("delete from cmd_fltpars where cmd_id = {}".format(cmd['id']))
            testdb.execute("delete from cmd_intpars where cmd_id = {}".format(cmd['id']))
        testdb.execute("delete from cmds where date > '{}'".format(last_timeline['datestop']))

        update_cmd_states = os.path.join(os.environ['SKA'], 'share',
                                         'cmd_states', 'update_cmd_states.py')
        if os.path.exists(self.h5file):
            os.unlink(self.h5file)
        cmd_state_cmd = "%s %s --datestart '%s' --mp_dir %s --h5file %s" % (
            update_cmd_states, db_str, cmd_state_datestart, mp_dir, self.h5file)

        err.write("Running %s\n" % cmd_state_cmd)
        out = bash(cmd_state_cmd)
        err.write('\n'.join(out) + '\n\n')
        err.write("Updated Test States in %s\n" % outdir )
        return

    def output_text_files(self, prefix='', get_all=False):

        load_rdb = self.load_rdb
        outdir = self.outdir
        dbh = self.db_handle()
        
        if not get_all:
            loads = Ska.Table.read_ascii_table(load_rdb, datastart=3)
            datestart = loads[0]['TStart (GMT)']
            datestop = loads[-1]['TStop (GMT)']

            timelines = dbh.fetchall("""select * from timelines
                                        where datestart >= '%s'
                                        and datestop <= '%s'
                                        order by datestart,load_segment_id""" % (datestart, datestop))
            test_states = dbh.fetchall("""select * from cmd_states
                                     where datestart >= '%s'
                                     and datestop <= '%s'
                                     order by datestart""" % (datestart, datestop ))
        else:
            timelines = dbh.fetchall("""select * from timelines
                                        order by datestart, load_segment_id""")
            test_states = dbh.fetchall("""select * from cmd_states
                                     order by datestart""")


        tfile = os.path.join( outdir,  prefix+'test_timelines.dat')
        pprint(timelines, cols=['datestart','datestop','dir'], out=open(tfile, 'w'))

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


        newcols = sorted(list(test_states.dtype.names))
        newcols = [ x for x in newcols if (x != 'tstart') & (x != 'tstop')]
        newstates = np.rec.fromarrays([test_states[x] for x in newcols], names=newcols)

        sfile = os.path.join( outdir, prefix+'test_states.dat')
        pprint(newstates, fmt=fmt, out=open(sfile, 'w'))

        # get the commands not associated with a timeline for the interval 
        # of interest
        null_cmds = dbh.fetchall("""select * from cmds
                              where date >= '%s'
                              and date <= '%s'
                              and timeline_id is NULL
                              order by date""" % (timelines[0]['datestart'], 
                                                  timelines[-1]['datestop'] ))

        try:
            null_cmds = null_cmds.tolist()
        except AttributeError:
            pass

        # start off with the null commands and then insert all of the
        # timeline-associated commands by timeline id
        cmds = null_cmds
        for tl in timelines:
            tl_cmds = dbh.fetchall("""select * from cmds
                                     where timeline_id = %d
                                     and date >= '%s'
                                     and date <= '%s'
                                     order by date""" % (tl['id'], tl['datestart'], tl['datestop']))
            if len(tl_cmds):
                colnames = tl_cmds.dtype.names
                cmds.extend(tl_cmds.tolist())
        cmds = sorted(cmds, key=lambda x: x[3])
        cmds = np.rec.fromrecords( cmds, names=colnames)

        cfile =  os.path.join( outdir, prefix+'test_cmds.dat')        
        pprint(cmds, out=open(cfile, 'w'))

        self.text_files =  { 'timelines': tfile,
                             'states': sfile,
                             'cmds': cfile }

        return self.text_files





def check_load(load_rdb, state_file, expect_match=True):
    """
    Setup testing db for a list of load segments
    Write out the "states" created.
    Diff against fiducial list of states

    :param load_rdb: load segment rdb file
    :param state_file: file with fiducial states
    """

    # make new states from load segments
    s = make_states(load_rdb)
    text_files = s.output_text_files()
    assert s.consistent_with_data()
    assert s.check_hdf5_sql_consistent()
    for etype in ['states',]:
        match = text_compare(text_files[etype], state_file, s.outdir, etype)
        assert match
    s.cleanup()


def make_states(load_rdb, dbi=DBI, outdir=None):
    """
    Setup db and states in a temp directory for quick nose testing

    :param load_rdb: file with load segments
    :rtype: (outdir, dbfilename)
    """
    if not outdir:
        outdir = re.sub(r'\.rdb$', '', load_rdb)
    s = Scenario(outdir, dbi=dbi)
    s.load_rdb = load_rdb
    s.data_setup()
    s.db_setup()
    s.populate_states()

    return s




### actual tests.
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
        f = partial( check_load, load_rdb, state_file, True)
        f.description = "Confirm %s matches %s states" % (load_rdb, state_file)
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

def test_first_sosa_update():
    # for a plain update, sosa or not, nothing should be deleted
    # and new entries should be inserted
    db_loads = asciitable.read('t/pre_sosa_db_loads.txt')
    want_load_rdb = asciitable.read('t/first_sosa.rdb')
    want_loads = update_load_seg_db.rdb_to_db_schema(want_load_rdb)
    to_delete, to_insert = update_load_seg_db.find_load_seg_changes(want_loads, db_loads, exclude=['id']) 
    assert len(to_delete) == 0
    assert len(to_insert) == len(want_loads[want_loads['datestart'] >= '2011:335:13:44:41.368'])
    assert to_insert[0]['datestart'] == '2011:335:13:44:41.368'

def test_load_update_weird():
    # for a difference in the past on any column, the entry
    # should be replaced
    db_loads = asciitable.read('t/pre_sosa_db_loads.txt')
    db_loads[5]['load_segment'] = 'CL324:0120'
    want_load_rdb = asciitable.read('t/first_sosa.rdb')
    want_loads = update_load_seg_db.rdb_to_db_schema(want_load_rdb)
    to_delete, to_insert = update_load_seg_db.find_load_seg_changes(want_loads, db_loads, exclude=['id']) 
    assert len(to_delete) == len(db_loads[db_loads['datestart'] >= '2011:324:01:05:40.930'])
    assert len(to_insert) == len(want_loads[want_loads['datestart'] >= '2011:324:01:05:40.930'])
    assert to_delete[0]['datestart'] == '2011:324:01:05:40.930'
    assert to_insert[0]['datestart'] == '2011:324:01:05:40.930'

def test_load_update_truncate():
    # if an old entry is now truncated, it and all after should be replaced
    db_loads = asciitable.read('t/pre_sosa_db_loads.txt')
    want_load_rdb = asciitable.read('t/first_sosa.rdb')
    want_loads = update_load_seg_db.rdb_to_db_schema(want_load_rdb)
    want_loads[10]['datestop'] = '2011:332:00:00:00.000'
    to_delete, to_insert = update_load_seg_db.find_load_seg_changes(want_loads, db_loads, exclude=['id']) 
    assert len(to_delete) == len(db_loads[db_loads['datestart'] >= want_loads[10]['datestart']])
    assert len(to_insert) == len(want_loads[want_loads['datestart'] >= want_loads[10]['datestart']])
    assert to_delete[0]['datestart'] == want_loads[10]['datestart']
    assert to_insert[0]['datestart'] == want_loads[10]['datestart']

def test_load_update_sosa_truncate():
    # if an old entry is now truncated, it and all after should be replaced
    # but the first entry of a sosa pair should not be removed if it is before the difference
    db_loads = asciitable.read('t/post_sosa_db_loads.txt')
    want_load_rdb = asciitable.read('t/second_sosa.rdb')
    want_loads = update_load_seg_db.rdb_to_db_schema(want_load_rdb)
    want_loads[12]['datestop'] = '2011:338:00:00:00.000'
    to_delete, to_insert = update_load_seg_db.find_load_seg_changes(want_loads, db_loads, exclude=['id']) 
    assert len(to_delete) == 11
    assert len(to_insert) == 11
    assert to_delete[0]['datestart'] == want_loads[12]['datestart']
    assert to_insert[0]['datestart'] == want_loads[12]['datestart']



def test_nsm_2010(outdir='t/nsm_2010', cmd_state_ska=SKA):

    # Simulate timelines and cmd_states around day 150 NSM

    err.write("Running nsm 2010 simulation \n" )

    s = Scenario(outdir)
    s.db_setup()
    db_str = s.db_cmd_str()

    # use a clone of the load_segments "time machine"
    tm = 'iFOT_time_machine'
    load_rdb = os.path.join(tm, 'load_segment.rdb')
    
    # make a local copy of nonload_cmds_archive, modified in test directory by interrupt
    shutil.copyfile('t/nsm_nonload_cmds_archive.py', '%s/nonload_cmds_archive.py' % outdir)
    bash_shell("chmod 755 %s/nonload_cmds_archive.py" % outdir)

    # when to begin simulation
    start_time = DateTime('2010:150:02:00:00.000')
    # load interrupt time
    int_time = DateTime('2010:150:03:00:00.000')
    # hours between simulated cron task to update timelines
    step = 12

    for hour_step in np.arange( 0, 72, step):
        ifot_time = start_time.secs + hour_step * 3600
        time_machine_update(ifot_time)

        # and copy that load_segment file to testing/working directory
        shutil.copyfile(load_rdb, '%s/%s_loads.rdb' % (outdir, DateTime(ifot_time).date))

        s.load_rdb = load_rdb
        s.run_at_time = ifot_time
        s.data_setup()


        # run the interrupt commanding before running the rest of the commands
        # if the current time is the first simulated cron pass after the
        # actual insertion of the interrupt commands
        dtime = ifot_time - int_time.secs
        if dtime / 3600. >= 0 and dtime / 3600. < step:
            err.write( "Performing interrupt \n")
            nsm_cmd = ("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
                       + " --cmd-set nsm "
                       + " --date '%s'" % DateTime(int_time).date
                       + " --interrupt "
                       + " --archive-file %s/nonload_cmds_archive.py " % outdir
                       + db_str)
            err.write(nsm_cmd + "\n")
            bash_shell(nsm_cmd)

            scs_cmd = ("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
                       + " --cmd-set scs107 "
                       + " --date '%s'" % DateTime(int_time).date
                       + " --interrupt "
                       + " --archive-file %s/nonload_cmds_archive.py " % outdir
                       + db_str)
            err.write(scs_cmd + "\n")
            bash_shell(scs_cmd)

        prefix = DateTime(ifot_time).date + '_'
        # run the rest of the cron task pieces: parse_cmd_load_gen.pl,
        # update_load_seg_db.py, update_cmd_states.py

        s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)
        text_files = s.output_text_files(prefix=prefix)
        for etype in ['states','timelines']:
            fidfile = os.path.join('t', "%sfid_%s.dat" % (prefix, etype))
            match = text_compare(text_files[etype], fidfile, s.outdir, etype)
            if match:
                assert True
            else:
                assert False
        
    s.cleanup()




def test_sosa_scs107(outdir='t/sosa_scs107', cmd_state_ska=os.environ['SKA']):
        
    err.write("Running sosa scs 107 simulation \n" )

    s = Scenario(outdir)
    s.db_setup()
    db_str = s.db_cmd_str()

    # make a local copy of nonload_cmds_archive, (will be modified in test directory by interrupt)
    shutil.copyfile('t/sosa_nonload_cmds_archive.py', '%s/nonload_cmds_archive.py' % outdir)
    bash_shell("chmod 755 %s/nonload_cmds_archive.py" % outdir)

    load_dir = os.path.join(outdir, 'loads')
    os.makedirs(load_dir)
    shutil.copy('t/sosa.rdb', load_dir)
    s.load_rdb = os.path.join(load_dir, 'sosa.rdb')
    s.load_dir = os.path.join(outdir, 'loads')
    s.mp_dir = os.path.join(outdir, 'mp') 

    shutil.copytree('t/sosa_mp', os.path.join(outdir, 'mp'))
    mp_dir = os.path.join(outdir, 'mp')
    
    prefix = 'sosa_pre_'
    s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)
    text_files = s.output_text_files(prefix=prefix)
    for etype in ['states','timelines']:
        fidfile = os.path.join('t', "%sfid_%s.dat" % (prefix, etype))
        match = text_compare(text_files[etype], fidfile, s.outdir, etype)
        if match:
            assert True
        else:
            assert False


    #shutil.copyfile( dbfilename, os.path.join(outdir, 'db_pre_interrupt.db3'))
    

    # a time for the interrupt (chosen in the middle of a segment)
    int_time = '2010:278:20:00:00.000'

    # run the interrupt commanding before running the rest of the commands
    # if the current time is the first simulated cron pass after the
    # actual insertion of the interrupt commands
    #print "Performing SCS107 interrupt"
    scs_cmd = ("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
               + " --cmd-set scs107 "
               + " --date '%s'" % DateTime(int_time).date
               + " --interrupt "
               + " --observing-only "
               + " --archive-file %s/nonload_cmds_archive.py " % outdir
               + db_str)
    err.write(scs_cmd + "\n")
    bash_shell(scs_cmd)


    prefix = 'sosa_post_'
    s.run_at_time = '2010:283:20:40:02.056'
    s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)
    text_files = s.output_text_files(prefix=prefix)
    for etype in ['states','timelines']:
        fidfile = os.path.join('t', "%sfid_%s.dat" % (prefix, etype))
        match = text_compare(text_files[etype], fidfile, s.outdir, etype)
        if match:
            assert True
        else:
            assert False
    s.cleanup()


def test_sosa_nsm(outdir='t/sosa_nsm', cmd_state_ska=os.environ['SKA']):
        
    err.write("Running sosa nsm simulation \n" )

    s = Scenario(outdir)
    s.db_setup()
    db_str = s.db_cmd_str()

    # make a local copy of nonload_cmds_archive, (will be modified in test directory by interrupt)
    shutil.copyfile('t/sosa_nonload_cmds_archive.py', '%s/nonload_cmds_archive.py' % outdir)
    bash_shell("chmod 755 %s/nonload_cmds_archive.py" % outdir)

    load_dir = os.path.join(outdir, 'loads')
    os.makedirs(load_dir)
    shutil.copy('t/sosa.rdb', load_dir)
    s.load_rdb = os.path.join(load_dir, 'sosa.rdb')
    s.load_dir = os.path.join(outdir, 'loads')
    s.mp_dir = os.path.join(outdir, 'mp') 

    shutil.copytree('t/sosa_mp', os.path.join(outdir, 'mp'))
    mp_dir = os.path.join(outdir, 'mp')
    
    prefix = 'sosa_nsmpre_'
    s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)
    text_files = s.output_text_files(prefix=prefix)
    for etype in ['states','timelines']:
        fidfile = os.path.join('t', "%sfid_%s.dat" % (prefix, etype))
        match = text_compare(text_files[etype], fidfile, s.outdir, etype)
        if match:
            assert True
        else:
            assert False


    #shutil.copyfile( dbfilename, os.path.join(outdir, 'db_pre_interrupt.db3'))
    

    # a time for the interrupt (chosen in the middle of a segment)
    int_time = '2010:278:20:00:00.000'

    # run the interrupt commanding before running the rest of the commands
    # if the current time is the first simulated cron pass after the
    # actual insertion of the interrupt commands
    nsm_cmd = ("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
               + " --cmd-set nsm "
               + " --date '%s'" % DateTime(int_time).date
               + " --interrupt "
               + " --archive-file %s/nonload_cmds_archive.py " % outdir
               + db_str)
    err.write(nsm_cmd + "\n")
    bash_shell(nsm_cmd)

    scs_cmd = ("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
               + " --cmd-set scs107 "
               + " --date '%s'" % DateTime(int_time).date
               + " --interrupt "
               + " --archive-file %s/nonload_cmds_archive.py " % outdir
               + db_str)
    err.write(scs_cmd + "\n")
    bash_shell(scs_cmd)


    prefix = 'sosa_nsmpost_'
    s.run_at_time = '2010:283:20:40:02.056'
    s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)
    text_files = s.output_text_files(prefix=prefix)
    for etype in ['states','timelines']:
        fidfile = os.path.join('t', "%sfid_%s.dat" % (prefix, etype))
        match = text_compare(text_files[etype], fidfile, s.outdir, etype)
        if match:
            assert True
        else:
            assert False

    s.cleanup()

def test_sosa_scs107_V2(outdir='t/sosa_scs107', cmd_state_ska=os.environ['SKA']):
        
    err.write("Running sosa scs 107 v2 simulation \n" )

    s = Scenario(outdir)
    s.db_setup()
    db_str = s.db_cmd_str()

    # make a local copy of nonload_cmds_archive, (will be modified in test directory by interrupt)
    shutil.copyfile('t/sosa_nonload_cmds_archive.py', '%s/nonload_cmds_archive.py' % outdir)
    bash_shell("chmod 755 %s/nonload_cmds_archive.py" % outdir)

    load_dir = os.path.join(outdir, 'loads')
    os.makedirs(load_dir)
    shutil.copy('t/sosa.rdb', load_dir)
    s.load_rdb = os.path.join(load_dir, 'sosa.rdb')
    s.load_dir = os.path.join(outdir, 'loads')
    s.mp_dir = os.path.join(outdir, 'mp') 

    shutil.copytree('t/sosa_mp', os.path.join(outdir, 'mp'))
    shutil.copy('t/sosa_v2_C276_0906.sum', 
                os.path.join(outdir, 'mp/2010/OCT0410/oflsa/mps', 
                             'C276_0906.sum'))
    mp_dir = os.path.join(outdir, 'mp')
    
    prefix = 'sosa_pre_'
    s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)
    text_files = s.output_text_files(prefix=prefix)
    for etype in ['states','timelines']:
        fidfile = os.path.join('t', "%sfid_%s.dat" % (prefix, etype))
        match = text_compare(text_files[etype], fidfile, s.outdir, etype)
        if match:
            assert True
        else:
            assert False


    #shutil.copyfile( dbfilename, os.path.join(outdir, 'db_pre_interrupt.db3'))
    

    # a time for the interrupt (chosen in the middle of a segment)
    int_time = '2010:278:20:00:00.000'

    # run the interrupt commanding before running the rest of the commands
    # if the current time is the first simulated cron pass after the
    # actual insertion of the interrupt commands
    #print "Performing SCS107 interrupt"
    scs_cmd = ("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
               + " --cmd-set scs107 "
               + " --date '%s'" % DateTime(int_time).date
               + " --interrupt "
               + " --observing-only "
               + " --archive-file %s/nonload_cmds_archive.py " % outdir
               + db_str)
    err.write(scs_cmd + "\n")
    bash_shell(scs_cmd)


    prefix = 'sosa_post_'
    s.run_at_time = '2010:283:20:40:02.056'
    s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)
    text_files = s.output_text_files(prefix=prefix)
    for etype in ['states','timelines']:
        fidfile = os.path.join('t', "%sfid_%s.dat" % (prefix, etype))
        match = text_compare(text_files[etype], fidfile, s.outdir, etype)
        if match:
            assert True
        else:
            assert False
    s.cleanup()


def test_sosa_nsm_v2(outdir='t/sosa_nsm', cmd_state_ska=os.environ['SKA']):
        
    err.write("Running sosa nsm v2 simulation \n" )

    s = Scenario(outdir)
    s.db_setup()
    db_str = s.db_cmd_str()

    # make a local copy of nonload_cmds_archive, (will be modified in test directory by interrupt)
    shutil.copyfile('t/sosa_nonload_cmds_archive.py', '%s/nonload_cmds_archive.py' % outdir)
    bash_shell("chmod 755 %s/nonload_cmds_archive.py" % outdir)

    load_dir = os.path.join(outdir, 'loads')
    os.makedirs(load_dir)
    shutil.copy('t/sosa.rdb', load_dir)
    s.load_rdb = os.path.join(load_dir, 'sosa.rdb')
    s.load_dir = os.path.join(outdir, 'loads')
    s.mp_dir = os.path.join(outdir, 'mp') 

    shutil.copytree('t/sosa_mp', os.path.join(outdir, 'mp'))
    shutil.copy('t/sosa_v2_C276_0906.sum', 
                os.path.join(outdir, 'mp/2010/OCT0410/oflsa/mps', 
                             'C276_0906.sum'))
    mp_dir = os.path.join(outdir, 'mp')
    
    prefix = 'sosa_nsmpre_'
    s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)
    text_files = s.output_text_files(prefix=prefix)
    for etype in ['states','timelines']:
        fidfile = os.path.join('t', "%sfid_%s.dat" % (prefix, etype))
        match = text_compare(text_files[etype], fidfile, s.outdir, etype)
        if match:
            assert True
        else:
            assert False


    #shutil.copyfile( dbfilename, os.path.join(outdir, 'db_pre_interrupt.db3'))
    

    # a time for the interrupt (chosen in the middle of a segment)
    int_time = '2010:278:20:00:00.000'

    # run the interrupt commanding before running the rest of the commands
    # if the current time is the first simulated cron pass after the
    # actual insertion of the interrupt commands
    nsm_cmd = ("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
               + " --cmd-set nsm "
               + " --date '%s'" % DateTime(int_time).date
               + " --interrupt "
               + " --archive-file %s/nonload_cmds_archive.py " % outdir
               + db_str)
    err.write(nsm_cmd + "\n")
    bash_shell(nsm_cmd)

    scs_cmd = ("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
               + " --cmd-set scs107 "
               + " --date '%s'" % DateTime(int_time).date
               + " --interrupt "
               + " --archive-file %s/nonload_cmds_archive.py " % outdir
               + db_str)
    err.write(scs_cmd + "\n")
    bash_shell(scs_cmd)


    prefix = 'sosa_nsmpost_'
    s.run_at_time = '2010:283:20:40:02.056'
    s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)
    text_files = s.output_text_files(prefix=prefix)
    for etype in ['states','timelines']:
        fidfile = os.path.join('t', "%sfid_%s.dat" % (prefix, etype))
        match = text_compare(text_files[etype], fidfile, s.outdir, etype)
        if match:
            assert True
        else:
            assert False

    s.cleanup()


def test_sosa_transition(outdir='t/sosa_update', cmd_state_ska=SKA):

    # Simulate timelines and cmd_states for all of 2010

    err.write("Running SOSA transition simulation \n" )

    s = Scenario(outdir)
    s.db_setup()
    db_str = s.db_cmd_str()

    # use a clone of the load_segments "time machine"
    tm = 'iFOT_time_machine'
    load_rdb = os.path.join(tm, 'load_segment.rdb')
    
    # make a local copy of nonload_cmds_archive, modified in test directory by interrupt
    shutil.copyfile('t/nsm_nonload_cmds_archive.py', '%s/nonload_cmds_archive.py' % outdir)
    bash_shell("chmod 755 %s/nonload_cmds_archive.py" % outdir)

    # when to begin simulation
    start_time = DateTime('2011:334:00:00:00.000')

    # days between simulated cron task to update timelines
    step = 1

    for day_step in np.arange( 0, 5, step):
        ifot_time = start_time + day_step
        time_machine_update(ifot_time)

        # but do some extra work to skip running the whole process on days when
        # there are no updates from the load segments table
        if os.path.exists(os.path.join(outdir, 'last_loads.rdb')):
            last_rdb_lines = open(os.path.join(outdir, 'last_loads.rdb')).readlines()
            new_rdb_lines = open(load_rdb).readlines()
            differ = difflib.context_diff(last_rdb_lines, new_rdb_lines)
            # cheat and just get lines that begin with + or !
            # (new or changed)
            change_test = re.compile('^(\+|\!)\s.*')
            difflines = filter(change_test.match, [line for line in differ])
            if len(difflines) == 0:
                err.write("Skipping %s, no additions\n" % ifot_time)
                continue
            else:
                for line in difflines:
                    err.write("%s" % line)
            err.write("Processing %s\n" % ifot_time)
                    
        # and copy that load_segment file to testing/working directory
        shutil.copyfile(load_rdb, '%s/%s_loads.rdb' % (outdir, DateTime(ifot_time).date))

        # and copy that load_segment file to the 'last' file
        shutil.copyfile(load_rdb, '%s/last_loads.rdb' % (outdir))

        s.load_rdb = load_rdb
        s.data_setup()

        prefix = 'sosa_' + DateTime(ifot_time).date + '_'
        # run the rest of the cron task pieces: parse_cmd_load_gen.pl,
        # update_load_seg_db.py, update_cmd_states.py

        s.run_at_time = ifot_time
        s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)

        text_files = s.output_text_files(prefix=prefix)
        for etype in ['states','timelines']:
            fidfile = os.path.join('t', "%sfid_%s.dat" % (prefix, etype))
            match = text_compare(text_files[etype], fidfile, s.outdir, etype)
            if match:
                assert True
            else:
                assert False



    # write out everything in the the timelines and states..
    # doesn't use output_text_files, because I don't want the
    # date ranges from the last rdb, I just want everything

    dbh = s.db_handle()
    prefix='sosa_trans_'

    text_files = s.output_text_files(prefix=prefix, get_all=True)

    for etype in ['states','timelines']:
        fidfile = os.path.join('t', "%sfid_%s.dat" % (prefix, etype))
        match = text_compare(text_files[etype], fidfile, s.outdir, etype)
        if match:
            assert True
        else:
            assert False
    
    s.cleanup()


def test_reinsert(outdir='t/reinsert', cmd_state_ska=os.environ['SKA']):

    # Fixing the database for the command collision bugs in March 2012
    # revealed another bug whereby 2 timelines are not reinserted for 2 new
    # load segments because there is already 1 matching timeline.
    # Test added to prevent recurrence.

    err.write("Running reinsert simulation \n")
    s = Scenario(outdir)
    s.db_setup()

    # use a clone of the load_segments "time machine"
    load_rdb = 't/2012:082:15:57:01.000.rdb'

    # make a local copy of nonload_cmds_archive,
    # which will be modified in test directory by interrupt
    shutil.copyfile('t/nsm_nonload_cmds_archive.py',
                    '%s/nonload_cmds_archive.py' % outdir)
    bash_shell("chmod 755 %s/nonload_cmds_archive.py" % outdir)

    s.load_rdb = load_rdb
    s.run_at_time = '2012:082:15:57:01.000'
    s.data_setup()
    s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)
    dbh = s.db_handle()
    first_timelines = dbh.fetchall("select * from timelines")

    load_segments = dbh.fetchall("select * from load_segments")
    ls = load_segments[19]
    mock_datestart = DateTime(DateTime(ls['datestart']).secs + 1).date
    dbh.execute("update load_segments set datestart = '%s' where id = %d"
                % (mock_datestart, ls['id']))
    s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)
    second_timelines = dbh.fetchall("select * from timelines")
    # these should just all be the same
    match_cols = ['dir', 'datestart', 'datestop']
    for want_entry, have_entry in izip(first_timelines, second_timelines):
        if any(have_entry[x] != want_entry[x] for x in match_cols):
            assert False





# 'test' isn't in the name to skip using this as a nosetest by default.
# It just takes too long.
def all_2010(outdir='t/all_2010', cmd_state_ska=SKA):

    # Simulate timelines and cmd_states for all of 2010

    err.write("Running all 2010 simulation \n" )

    s = Scenario(outdir)
    s.db_setup()
    db_str = s.db_cmd_str()

    # use a clone of the load_segments "time machine"
    tm = 'iFOT_time_machine'
    load_rdb = os.path.join(tm, 'load_segment.rdb')
    
    # make a local copy of nonload_cmds_archive, modified in test directory by interrupt
    shutil.copyfile('t/nsm_nonload_cmds_archive.py', '%s/nonload_cmds_archive.py' % outdir)
    bash_shell("chmod 755 %s/nonload_cmds_archive.py" % outdir)

    # when to begin simulation
    start_time = DateTime('2010:001:00:00:00.000')

    # load interrupt time
    int_time = DateTime('2010:150:04:00:00.000')
    # days between simulated cron task to update timelines
    step = 1

    for day_step in np.arange( 0, 365, step):
        ifot_time = start_time + day_step
        time_machine_update(ifot_time)

        # run the interrupt commanding before running the rest of the commands
        # if the current time is the first simulated cron pass after the
        # actual insertion of the interrupt commands
        dtime = ifot_time - int_time.secs
        if dtime / 3600. >= 0 and dtime / 3600. < 24:
            err.write( "Performing interrupt \n")
            nsm_cmd = ("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
                       + " --cmd-set nsm "
                       + " --date '%s'" % DateTime(int_time).date
                       + " --interrupt "
                       + " --archive-file %s/nonload_cmds_archive.py " % outdir
                       + db_str)
            err.write(nsm_cmd + "\n")
            bash_shell(nsm_cmd)

            scs_cmd = ("%s/share/cmd_states/add_nonload_cmds.py " % cmd_state_ska
                       + " --cmd-set scs107 "
                       + " --date '%s'" % DateTime(int_time).date
                       + " --interrupt "
                       + " --archive-file %s/nonload_cmds_archive.py " % outdir
                       + db_str)
            err.write(scs_cmd + "\n")
            bash_shell(scs_cmd)
        else:
            # but do some extra work to skip running the whole process on days when
            # there are no updates from the load segments table
            if os.path.exists(os.path.join(outdir, 'last_loads.rdb')):
                last_rdb_lines = open(os.path.join(outdir, 'last_loads.rdb')).readlines()
                new_rdb_lines = open(load_rdb).readlines()
                differ = difflib.context_diff(last_rdb_lines, new_rdb_lines)
                # cheat and just get lines that begin with + or !
                # (new or changed)
                change_test = re.compile('^(\+|\!)\s.*')
                difflines = filter(change_test.match, [line for line in differ])
                if len(difflines) == 0:
                    err.write("Skipping %s, no additions\n" % ifot_time)
                    continue
                else:
                    for line in difflines:
                        err.write("%s" % line)

        err.write("Processing %s\n" % ifot_time)

        # and copy that load_segment file to testing/working directory
        shutil.copyfile(load_rdb, '%s/%s_loads.rdb' % (outdir, DateTime(ifot_time).date))

        # and copy that load_segment file to the 'last' file
        shutil.copyfile(load_rdb, '%s/last_loads.rdb' % (outdir))

        s.load_rdb = load_rdb
        s.data_setup()


        prefix = DateTime(ifot_time).date + '_'
        # run the rest of the cron task pieces: parse_cmd_load_gen.pl,
        # update_load_seg_db.py, update_cmd_states.py

        s.run_at_time = ifot_time
        s.populate_states(nonload_cmd_file="%s/nonload_cmds_archive.py" % outdir)


    # write out everything in the the timelines and states..
    # doesn't use output_text_files, because I don't want the
    # date ranges from the last rdb, I just want everything

    dbh = s.db_handle()
    prefix='all_'

    text_files = s.output_text_files(prefix=prefix, get_all=True)

    for etype in ['states','timelines']:
        fidfile = os.path.join('t', "%sfid_%s.dat" % (prefix, etype))
        match = text_compare(text_files[etype], fidfile, s.outdir, etype)
        if match:
            assert True
        else:
            assert False

    s.cleanup()






def run_model(opt, db):
    """
    Run the psmc twodof model for given states 

    :param opt: cmd line options opt.load_rdb, opt.verbose, opt.outdir
    :rtype: dict of just about everything
    """

    import Ska.Table

    loads = Ska.Table.read_ascii_table(opt.load_rdb, datastart=3)
    datestart = loads[0]['TStart (GMT)']
    datestop = loads[-1]['TStop (GMT)']


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

