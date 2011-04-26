#!/usr/bin/env python

import Ska.DBI
from Chandra.Time import DateTime


def repair(ifot_loads):
    """
    Make any known edits to a recarray of load segments.  This is called
    in update_load_seg_db.py before loads are inserted.  Returns a new recarray
    with the edits.

    :param ifot_loads: numpy.recarray from the (Ska.Table) parsed rdb of load segments from arc/iFOT
    :rtype: numpy.recarray
    
    """

    import numpy as np

    # delete a load that was never run
    match = ((ifot_loads.load_segment == 'CL304:0504') &
             (ifot_loads.year == 2003))
    if any(match):
        # slice to get the ones that don't match
        ifot_loads = ifot_loads[match == False]

    # repair a couple of loads
    # 352 ended early at...
    match = ((ifot_loads.load_segment == 'CL352:1208') &
             (ifot_loads.year == 2008)) 
    if any(match):
        ifot_loads.datestop[match] = '2008:353:05:00:00.000'
        ifot_loads.fixed_by_hand[match] = 1

    # CL110:1404 was run, not CL110:1409
    match = ((ifot_loads.load_segment == 'CL110:1409') &
             (ifot_loads.year == 2003)) 
    if any(match):
        rec_tuple = ( 'CL110:1404', 2003,
                      '2003:110:14:07:09.439', '2003:112:00:00:31.542', 130, 1)
        # ifot_list except the bad one
        ifot_list = ifot_loads[match == False].tolist()
        ifot_list.append(rec_tuple)
	    # sort by datestart in third field
        ifot_list.sort(lambda x,y: cmp(x[2],y[2]))
        new_ifot = np.rec.fromrecords( ifot_list,
                                       dtype=ifot_loads.dtype,
                                       )
        ifot_loads = new_ifot


    # CL188:0402 is missing
    cmd_date = '2009:188:04:00:00.000'

    if ((ifot_loads[0].datestart <= cmd_date) and (ifot_loads[-1].datestart > cmd_date)):
        if np.flatnonzero((ifot_loads.load_segment == 'CL188:0402') &
                          (ifot_loads.year == 2009)):
            pass
        else:
            rec_tuple = ( 'CL188:0402', 2009,
                         '2009:188:04:00:00.000', '2009:188:20:57:33.571', 129, 1)
            ifot_list = ifot_loads.tolist()
            ifot_list.append(rec_tuple)
            ifot_list.sort(lambda x,y: cmp(x[2],y[2]))
            new_ifot = np.rec.fromrecords( ifot_list,
                                           dtype=ifot_loads.dtype,
                                           )
            ifot_loads = new_ifot

    # 2011 CL103:2002 is missing, interrupted by  the APR1311 replan
    cmd_date = '2011:103:20:40:00.000'

    if ((ifot_loads[0].datestart <= cmd_date) and (ifot_loads[-1].datestart > cmd_date)):
        if np.flatnonzero((ifot_loads.load_segment == 'CL103:2002') &
                          (ifot_loads.year == 2011)):
            pass
        else:
            rec_tuple = ( 'CL103:2002', 2011,
                         '2011:103:20:40:00.000', '2011:103:22:57:00.000', 129, 1)
            ifot_list = ifot_loads.tolist()
            ifot_list.append(rec_tuple)
            ifot_list.sort(lambda x,y: cmp(x[2],y[2]))
            new_ifot = np.rec.fromrecords( ifot_list,
                                           dtype=ifot_loads.dtype,
                                           )
            ifot_loads = new_ifot


    return ifot_loads
