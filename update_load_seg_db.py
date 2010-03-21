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

log = logging.getLogger()
log.setLevel(logging.DEBUG)
MP_DIR = '/data/mpcrit1/mplogs/'


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



def get_built_load( run_load, dbh=None):
    """
    Given an entry from the load_segments table, return the matching entry 
    from the tl_built_loads table
    :param load: run load from load_segments table
    :rtype: dict of matching built load 

    """

    built_query = ( """select * from tl_built_loads where load_segment = '%s' 
                       and year = %d order by sumfile_modtime desc"""
                    % ( run_load['load_segment'], run_load['year'] ))
    built = dbh.fetchone( built_query )
    if built is None:
        match_like = re.search('(CL\d{3}:\d{4})', run_load['load_segment'])
        if match_like is None:
            raise ValueError("Load name %s is in unknown form, expects /CL\d{3}:\d{4}/" %
                             run_load['load_segment'])
        like_load = match_like.group(1)
        built_query = ( """select * from tl_built_loads where load_segment like '%s%s%s' 
                           and year = %d order by sumfile_modtime desc"""
                        % ( '%', like_load, '%',run_load['year'] ))
        built = dbh.fetchone( built_query )
        # if a built version *still* hasn't been found
        if built  is None:
            raise ValueError("Unable to find file for %s,%s" % (run_load['year'], run_load['load_segment']))
    return built


def get_processing( built, dbh=None ):
    """
    Given an entry from the tl_built_loads table, return the entry for the 
    corresponding file from tl_processing
    :param built: tl_built_loads entry/dict
    :rtype: dict of matching tl_processing entry
    """
    processing_query = ("""select * from tl_processing where file = '%s' 
                           and sumfile_modtime = %s order by dir desc """
                        % ( built['file'], built['sumfile_modtime'] ))
    processed = dbh.fetchone( processing_query )
    if processed is None:
        raise ValueError("Unable to find processing for built file %s", built['file'])
    return processed

def get_replan_dir( replan_seg, replan_year, dbh=None):
    match_like = re.search('C(\d{3}).?(\d{4})', replan_seg)
    if match_like is None:
        raise ValueError("Replan load seg %s is in unknown form, expects /C\d{3}?\d{4}/" %
                         replan_seg)
    replan_query = ("""select dir from tl_processing
                       where file like 'C%s%s%s.sum'
                       and year = %d 
                       order by year, sumfile_modtime desc
                       """ % (match_like.group(1), '%', match_like.group(2), replan_year ))
    replan = dbh.fetchone( replan_query )
    # if a built version *still* hasn't been found
    if replan  is None:
        raise ValueError("Unable to find file for %s,%s" % (replan_year, replan_seg))
    return replan['dir']


def weeks_for_load( run_load, dbh=None, test=False ):
    """ 
    Determine the timeline intervals that exist for a load segment

    :param run_load: load segment dict
    :param dbh: database handle for tl_built_loads and tl_processing
    :param test: test mode option to allow the routine to continue on missing history

    :rtype: list of dicts.  Each dict a 'timeline'


    """
    built = get_built_load( run_load, dbh=dbh )
    processed = get_processing( built, dbh=dbh )

    match_load_pieces = []

    match = { 'dir' : processed['dir'],
              'load_segment_id' : run_load['id'],
              'datestart': run_load['datestart'],
              'datestop': run_load['datestop'],
              'replan': processed['replan'],
              'incomplete': 1 }

    if processed['replan'] and processed['bcf_cmd_count'] > 0:
        if processed['replan_cmds'] is None:
            raise ValueError("Replan/ReOpen load: %s without replan_cmd source load! " % (run_load['load_segment']))
        # if load is completely contained in processing (earlier load interrupted in week )
        if  (( run_load['datestart']  >=  processed['processing_tstart'] )
             & (  run_load['datestop'] <=  processed['processing_tstop'] )):
            match_load_pieces.append( match.copy() )
        # if processing covers the end but not the beginning (this one was interruped)
        if run_load['datestart'] < processed['processing_tstart'] and run_load['datestop'] <= processed['processing_tstop']:
            replan_dir = get_replan_dir( processed['replan_cmds'], run_load['year'], dbh=dbh )
            print replan_dir
            # the end
            match['datestart'] = processed['processing_tstart']
            match_load_pieces.append( match.copy() )
            # the pre-this-processed-file chunk
            match['datestart'] = run_load['datestart']
            match['datestop'] = processed['processing_tstart']
            match['dir'] = replan_dir
            match_load_pieces.append( match.copy() )
    else:
        # if the run load matches the times of the built load
        if ( (DateTime(run_load['datestart']).mxDateTime >= ( DateTime(built['first_cmd_time']).mxDateTime))
             & (DateTime(run_load['datestop']).mxDateTime <= ( DateTime(built['last_cmd_time']).mxDateTime))):
            match['incomplete'] = 0
        # append even if incomplete
        match_load_pieces.append(match.copy())

    timelines = sorted(match_load_pieces, key=lambda k: k['datestart'])
    return timelines

def get_ref_timelines( datestart, dbh=None, n=10):
    ref_timelines = []
    pre_query =  ("select * from timelines where datestart <= '%s' order by datestart desc"
                  %  datestart )
    pre_query_fetch = dbh.fetch(pre_query)
    for cnt in xrange(0,n):
        try:
            ref_timelines.append( pre_query_fetch.next() )
        except StopIteration:
            if (cnt == 0):
                log.warn("""TIMELINES WARN: no timelines found before current insert""")
            else:
                log.warn("""TIMELINES WARN: only found %i of %i timelines before current insert"""
                         % (cnt-1, 10))
            break
    ref_timelines = sorted(ref_timelines, key=lambda k: k['datestart'])
    return ref_timelines


def update_timelines_db( loads=None, dbh=None, dryrun=False, test=False ):
    """
    Given a list of load segments this routine determines the timelines (mission
    planning weeks and loads etc) over the loads and inserts new timelines 
    into the aca timelines db table.

    In common use, this will just insert new timelines at the end of the table.

    In case of scheduled replan, timelines will be updated when load 
    segments are updated.

    In case of autonomous safing, individual timelines are shortened by outside 
    processing and act as place holders.  This script will not update shortened 
    timelines until new load segments are seen in ifot.  

    :param loads: dict or recarray of loads 
    :param dryrun: do not update database
    :rtype: None
    """

    as_run = loads
    as_run = sorted(as_run, key=lambda k: k['datestart'])

    log.info("TIMELINES INFO: Updating timelines for range %s to %s" 
             % ( as_run[0]['datestart'], as_run[-1]['datestop']))

    timelines = []
    for run_load in as_run:
        run_timelines = weeks_for_load( run_load,
                                        dbh=dbh,
                                        test=test)
        if len(run_timelines) == 0:
            raise ValueError("No timelines found for load %s" % run_load )
        for run_timeline in run_timelines:
            # append the new timeline to the list to insert
            timelines.append(run_timeline)

    timelines = sorted(timelines, key=lambda k: k['datestart'])
    for timeline in timelines:
        timeline['id'] = int(DateTime(timeline['datestart']).secs)
        timeline['fixed_by_hand'] = 0

    # get existing entries
    db_timelines = dbh.fetchall("""select * from timelines 
                                   where datestop >= '%s' and datestart <= '%s'
                                   order by datestart 
                                   """ % ( timelines[0]['datestart'], timelines[-1]['datestop'] ))
       
    from itertools import count, izip
    if len(db_timelines) > 0:
        i_diff = 0
        for timeline in  timelines:
            # if there is an entry in the database already that
            # a) has the same datestart
            # b) is shorter or the same length
            # then there is a match, and skip along to the next one
            startmatch = timeline['datestart'] == db_timelines['datestart']
            endmatch = timeline['datestop'] >= db_timelines['datestop'] 
            if not any( startmatch & endmatch ):
                break
            i_diff += 1
    else:
       i_diff = 0

    # Mismatch occured at i_diff.  

    if i_diff == len(timelines):
        log.info('TIMELINES INFO: No database update required')
        return

    # warn if timeline is shorter than an hour
    for run_timeline in timelines[i_diff:]:
        time_length = DateTime(run_timeline['datestop']).mxDateTime - DateTime(run_timeline['datestart']).mxDateTime
        if time_length.minutes < 60:
            log.warn("TIMELINES WARN: short timeline at %s, %d minutes" % ( run_timeline['datestart'],
                                                                            time_length.minutes ))
    # find all db timelines that start after the last valid one [i_diff-1]
    findcmd = ("""SELECT id from timelines 
                  WHERE datestart > '%s'
                  AND fixed_by_hand = 0
                  AND datestart <= datestop """
               % timelines[i_diff-1]['datestart'] )
    defunct_tl = dbh.fetchall( findcmd )
    if len(defunct_tl):
        for id in defunct_tl.id:
            clear_timeline( id )
            
        
    # Insert new timelines[i_diff:] 
    log.info('TIMELINES INFO: inserting timelines[%d:%d] to timelines' %
                  (i_diff, len(timelines)+1))
    for timeline in timelines[i_diff:]:
        log.debug('TIMELINES DEBUG: inserting timeline:')
        insert_string = "\t %s %s %s %d" % ( timeline['dir'], 
                                             timeline['datestart'], timeline['datestop'], 
                                             timeline['id'] )
        log.debug(insert_string)
        if not dryrun:
            try:
                dbh.insert(timeline, 'timelines')
            except IntegrityError:
                log.warn('Could not insert timeline at %s' % timeline['datestart']) 


def rdb_to_db_schema( orig_ifot_loads ):
    """
    Convert the load segment data from a get_iFOT_events.pl rdb table into
    the schema used by the load segments table

    :param orig_rdb_loads: recarray from the get_iFOT_events.pl rdb table
    :rtype: recarray 
    """
    loads = []
    for orig_load in orig_ifot_loads:
        starttime = DateTime(orig_load['TStart (GMT)'])
        load = ( int(starttime.secs),
                 orig_load['LOADSEG.NAME'],
                 starttime.mxDateTime.year,
                 orig_load['TStart (GMT)'],
                 orig_load['TStop (GMT)'],
                 orig_load['LOADSEG.SCS'],
                 0,
                 )
        loads.append(load)
    names = ('id','load_segment','year','datestart','datestop','load_scs','fixed_by_hand')
    load_recs = np.rec.fromrecords( loads, names=names)
    return load_recs


def check_load_overlap( loads ):
    """
    Checks command load segment overlap

    Logs warnings for : overlap greater than 15 minutes
                        separation greater than 6 hours
                        any overlap in the same SCS

    :param loads: recarray of load segments
    :rtype: None
    """

    sep_times = ( DateTime(loads[1:]['datestart']).secs 
                  - DateTime(loads[:-1]['datestop']).secs )
    max_overlap_mins = 15
    max_sep_hours = 12
    max_overlap = -1 * max_overlap_mins * 60
    max_sep = max_sep_hours * 60 * 60
    # check for overlap
    if (any(sep_times < max_overlap)):
        log.warn('LOAD_SEG WARN: Loads overlap by more than %i minutes' % max_overlap_mins)
    # check for too much sep
    if (any(sep_times > max_sep )):
        for load_idx in np.flatnonzero(sep_times > max_sep):
            log.warn('LOAD_SEG WARN: Loads %s %s separated by more that %i hours' 
                         % (loads[load_idx]['load_segment'],
                            loads[load_idx+1]['load_segment'],
                            max_sep_hours ))
    # any SCS overlap
    for scs in (128,129,130):
        scs_loads = loads[ loads['load_scs'] == scs ]
        scs_times = ( DateTime(scs_loads[1:]['datestart']).secs 
                      - DateTime(scs_loads[:-1]['datestop']).secs)
        if (any(scs_times < 0 )):
            log.warn('LOAD_SEG WARN: Same SCS loads overlap')


def clear_timeline( id, dbh=None, dryrun=False ):
    """
    Clear the commands related to a timeline and then delete the timeline.

    :param id: unique id of timeline
    :param dbh: database handle for cmd_fltpars, cmd_intpars, cmds, and timlines tables
    :param dryrun: dryrun option/ no cmds are executed
    :rtype: None
    """

    # remove related cmds as if there were a foreign key constraint
    for table in ('cmd_fltpars', 'cmd_intpars', 'cmds'):
        cmd = (""" DELETE from %s
                   WHERE timeline_id = %s """
               % ( table, id ))
        log.debug('TIMELINES DEBUG: ' + cmd)
        if not dryrun:
            dbh.execute(cmd)
    
    # remove defunct timelines
    cmd = ("""DELETE FROM timelines 
              WHERE id = %s 
              AND fixed_by_hand = 0 """
           % id)
    log.debug('TIMELINES DEBUG: ' + cmd)
    if not dryrun:
        dbh.execute(cmd)


def clear_rel_timelines( load_segments, dbh=None ):
    """
    Remove timelines related to (fk constrained to) load_segments from the timelines db

    :param load_segments: recarray of to-be-deleted load_segments
    :rtype: None
    """
    for load in load_segments:
        db_timelines = dbh.fetchall("SELECT * from timelines where load_segment_id = %i" 
                                   % ( load['id']))
        
        if len(db_timelines) > 0:
            if (any(db_timelines['fixed_by_hand'])):
                for timeline in db_timelines[ db_timelines['fixed_by_hand'] == 1]:
                    log.warn("LOAD_SEG WARN: updating timelines across %i which is fixed_by_hand" 
                             % (timeline['id']))

            for timeline in db_timelines:
                clear_timeline( timeline['id'], dbh=dbh )




def get_last_timeline(dbh=None):
    """
    Select most recent timeline from timelines db

    :rtype: recarray from timelines db
    """

    timeline = dbh.fetchall("""SELECT * from timelines where datestart 
                                  = (select max(datestart) from timelines)""")[0]
    return timeline

                               


def update_loads_db( ifot_loads, dbh=None, test=False, dryrun=False):
    """
    Update the load_segments table with the loads from an RDB file.

    :param ifot_loads: recarray of ifot run loads 
    :param test: allow writes of < year 2009 data 
    :param dryrun: do not write to the database
    :rtype: list of new loads
    """
    
    log.info("LOAD_SEG INFO: Updating Loads for range %s to %s" 
             % ( ifot_loads[0]['datestart'], ifot_loads[-1]['datestop']))

    # raise errors on some unexpected possibilities
    if len(ifot_loads) == 0:
        raise ValueError("LOAD_SEG: No loads passed to update_loads_db()")
    if len(ifot_loads) == 1:
        raise ValueError("LOAD_SEG: only one load passed to update_loads_db()")
        
    # Tom doesn't want < year 2009 load segments to change ids etc
    min_time_datestart ='2009:001:00:00:00.000'
    if (ifot_loads[0]['datestart'] < min_time_datestart) and not test:
        raise ValueError("Attempting to update loads before %s" 
                         % min_time_datestart )

    db_loads = dbh.fetchall("""select * from load_segments 
                               where datestart >= '%s' and datestart <= '%s'
                               order by datestart """ % ( ifot_loads[0]['datestart'],
                               ifot_loads[-1]['datestop'],
                               )
                           )
    
    # if no overlap on time range, check for an empty table (which should be OK)
    if len(db_loads) == 0:
        count_all_db_loads = dbh.fetchall("select count(*) as all_count from load_segments")
        count_all = count_all_db_loads[0]['all_count']
        if count_all == 0:
            log.warn("LOAD_SEG_WARN: load_segment table is empty")
            i_diff = 0
        else:
            raise ValueError("No overlap in database for load segment interval")
    else:
#        if any(db_loads['fixed_by_hand']):
#            for load in db_loads[ db_loads['fixed_by_hand'] == 1]:
#                log.warn("LOAD_SEG WARN: Updating load_segments across %s which is fixed_by_hand" 
#                         % (load['load_segment']))

        # Find mismatches:
        match_cols = [x[0] for x in db_loads.dtype.descr if 'f' not in x[1]]
        # use min( ... ) to only check through the range that is in both lists
        for i_diff in xrange(0, min( len(ifot_loads), len(db_loads) )):
            ifot_load = ifot_loads[i_diff]
            db_load = db_loads[i_diff]
            # does the db load match?
            if (any(db_load[x] != ifot_load[x] for x in match_cols) ):
                log.info('LOAD_SEG INFO: Mismatch from ifot datestart %s load %s' 
                         % ( ifot_load['datestart'], ifot_load['load_segment']))
                log.info( ifot_load )
                log.info( db_load )
                break                

        if i_diff == 0:
            raise ValueError("Unexpected mismatch at first database entry in range")

        if i_diff == len(ifot_loads) - 1:
            log.info('LOAD_SEG INFO: No database update required')
            later_loads = dbh.fetchall("select * from load_segments where datestart >= '%s'"
                                           % ifot_loads[-1]['datestop'])
            if len(later_loads) != 0:
                log.warn("LOAD_SEG WARN: Loads exist in db AFTER update range!!!")
            return

        cmd = ("DELETE FROM load_segments WHERE datestart >= '%s'"
               % db_loads[i_diff]['datestart'] )
        log.info('LOAD_SEG INFO: ' + cmd)
        if not dryrun:
            clear_rel_timelines( db_loads[i_diff:], dbh=dbh)
            dbh.execute(cmd)

    # check for overlap in the loads... the check_load_overlap just logs warnings
    check_load_overlap( ifot_loads )    

    # Insert new loads[i_diff:] into load_segments
    log.info('LOAD_SEG INFO: inserting load_segs[%d:%d] to load_segments' %
                  (i_diff, len(ifot_loads)+1))
    for load in ifot_loads[i_diff:]:
        log.debug('LOAD_SEG DEBUG: inserting load')
        insert_string = "\t %s %d %s %s" % ( load['load_segment'], load['year'],
                                             load['datestart'], load['datestop'] )
        log.debug(insert_string)
        if not dryrun:
            dbh.insert(load, 'load_segments', commit=True)
    return ifot_loads[i_diff:]

    

        
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
        import fix_load_segments
        fix_load_segments.repair(dbh)
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
    



 
