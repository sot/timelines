Timelines Infrastructure
=====================================

.. toctree::
   :maxdepth: 2


The key elements of the timelines project are:

  - ``parse_cmd_load_gen.pl``: parse command load processing summaries
    from mission planning directories
  - ``update_load_seg_db.py``: update load segments and timelines
    tables

Helper elements include:

  - ``fix_load_segments.py``: module to implement "manual" database
    fixes for load_segments (that are not yet included in iFOT).  If
    a load needs to be modified after it has rolled off of the
    current arc/iFOT load segment rdb, modify fix_load_segments.py and then call
    update_load_seg_db.py with a constructed rdb that includes the
    load segment that needs modification.
  - ``fix_tl_processing.py``: script to implement "manual" database
    fixes for the command load summary file parsing tables
    (tl_built_loads, tl_processing)
  - ``timelines_test.py``: package containing regression test
    elements.  suitable for nose tests and the following scripts
  - ``timelines_make_testdb.py``: make a testing db for ... testing
  - ``timelimes_check_testdb.py``: compare testing db to telemetry

Regression Testing
--------------------

The regression tests are packaged as nose tests in timelines_test.
The end-to-end tests in test_loads() generate a test databases from
load_segments and backstops and compare the created states with tables
of states stored in the testing directory.

Tests may be run with:

   nosetests timelines_test.py

(note that the script runs using sqlite test database by default.  Set
DBI='sybase' at the top of a local copy of timelines_test.py to test using
aca_tstdb sybase database)

If there is a diff in the output:

  .Checking t/july_fixed.rdb
  Made Test States in /tmp/tmpyOgXJT
  Diff in /tmp/tmpyOgXJT

look at the html diff (e.g. /tmp/tmpyOgXJT/diff.html) or manually diff the
states with the expected states, for example:

  meld /tmp/tmpyOgXJT/test_states.dat t/july_fixed.dat

If the diff is expected, due to a change in nonload commands since the
creation of the fiducial data, update the fiducial data:

  cp /tmp/tmpyOgXJT/test_states.dat t/july_fixed.dat                                    

and commit the change in the timelines project.



The utility scripts "timelines_make_testdb.py" and
"timelines_check_testdb.py" are provided to allow more manual testing:

* :mod:`timelines_make_testdb`

   For example:
      ./timelines_make_testdb.py --load_rdb t/replan.rdb --outdir replan

* :mod:`timelines_check_testdb`

   For example:
      ./timelines_check_testdb.py --load_rdb t/replan.rdb --outdir replan

Each script takes a load segment list in rdb format (as retrieved by
arc or construct to look like it was retrieved that way).

timelines_make_testdb.py builds a testing area, creating a softlink
tree to /data/mpcrit1/mplogs, parsing the files in that softlink tree,
and constructing and populating testing database tables from that tree
and the input load segment list.

timelines_check_testdb.py makes validation plots vs telemetry.
