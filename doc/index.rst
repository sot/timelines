.. timelines documentation master file, created by
   sphinx-quickstart on Sun Mar  7 19:59:59 2010.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Timelines documentation
=====================================


The timelines project is a suite of tools that is used to determine,
for any time, which load segments were running and which command
products were used to create the commands for that time.  These
determined intervals or "timelines" are stored in the ACA database and
are the foundation for the commands and command states processes and
their associated database tables.  ( See the cmd_states_ project for
more on those tables )

.. _cmd_states: ../cmd_states/index.html

Timelines Database Access
-------------------------

To access the database, log in to a host on the HEAD network and use 
the "sqsh" command:

sqsh -U aca_read -S sybase

The password can be obtained from Tom Aldcroft or Jean Connelly. 


load_segments table
-------------------

The load_segments table stores the list of run and to-be-run load
segments as obtained from iFOT_.  

The timelines software creates a unique id (from the datestart in
Chandra seconds).  Note that the load_segment string
(e.g. 'CL180:0800') may not be unique for the the mission as it is
constructed from the day-of-year and time.  Any search by load_segment
name should also include a year.

==============   =========   ====
Name             Type        Size
==============   =========   ====
 id              int            4
 load_segment    varchar       15
 year            int            5
 datestart       varchar       21
 datestop        varchar       21
 load_scs        int            4
 fixed_by_hand   bit            1
==============   =========   ====

(Several iFOT entries are missing or incorrect and have been fixed in
the load_segments table and marked fixed_by_hand = 1).


.. _iFOT: http://occwww.cfa.harvard.edu/occweb/web/webapps/ifot/ifot.php?r=home&t=builder&a=show&size=auto&format=list&columns=type_desc,tstart,tstop,properties&e=LOADSEG.LOAD_NAME.NAME.SCS&tstart=1999:204:00:00:00.000&tstop=2010:175:17:25:55.000


timelines table
---------------

The timelines table is used to map time ranges (datestart,
datestop) to command products directories (dir).  These directories
are presently relative to the SOT MP area at /data/mpcrit1/mplogs .
Each timeline is associated with a load_segment_id and is assigned a
unique id.  Note that there may be more than one timeline for a load_segment
(This may happen, for example, if a command load is created with
Replan/ReOpen and the commands which are brought forward into the new
command load are found in the Backstop directory of the original
week.) 

==================   =========   ====
Name                 Type        Size
==================   =========   ====
 id                  int            4
 load_segment_id     int            4
 dir                 varchar       20
 datestart           varchar       21
 datestop            varchar       21
 replan              bit            1
 incomplete          bit            1
 fixed_by_hand       bit            1
==================   =========   ====

(replan, incomplete, and fixed_by_hand columns are only of interest to
the developers.)  

After SCS107 or other interrupt, the datestops of all
timelines at or after the interrupt are set to the interrupt time.
Those timeline entries will eventually be removed and replaced with
new entries during normal processing and ingest.

Infrastructure
==============

.. toctree::
   :maxdepth: 1

   infrastructure


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

