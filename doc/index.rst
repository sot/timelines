.. timelines documentation master file, created by
   sphinx-quickstart on Sun Mar  7 19:59:59 2010.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Timelines documentation
=====================================

Contents:

.. toctree::
   :maxdepth: 2

The timelines project is a suite of tools that is used to determine,
for any time, which command products were used to create the commands
for that time.  These determined intervals or "timelines" are stored
in the ACA database and are the foundation for the commands and
command states processes and their associated database tables.
( See the Chandra.cmd_states project for more on those tables )

The key elements of the timelines project are:

  - ``parse_cmd_load_gen.pl``: parse command load processing summaries
    from mission planning directories
  - ``update_load_seg_db.py``: update load segments and timelines
    tables

Helper elements include:

  - ``fix_load_segments.py``: script to implement "manual" database
    fixes for load_segments (that are not yet included in iFOT)
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

timelines_check_testdb.py compares the testing states with the current
sybase versions of those states and makes validation plots vs telemetry.


Module summary
----------------

* :mod:`update_load_seg_db`
* :mod:`timelines_test`





Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

