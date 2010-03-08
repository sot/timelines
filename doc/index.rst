.. timelines documentation master file, created by
   sphinx-quickstart on Sun Mar  7 19:59:59 2010.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Timelines documentation
=====================================

Contents:

.. toctree::
   :maxdepth: 2


Module summary
----------------

* :mod:`timelines`
* :mod:`timelines_test`

Regression Testing
--------------------

The regression tests are packaged as nose tests in timelines_test.
The end-to-end tests in test_loads() generate a test databases from
load_segments and backstops and compare the created states with tables
of states stored in the testing directory.

The utility scripts "timelines_make_testdb.py" and
"timelines_check_testdb.py" are provided to allow more manual testing:

* :mod:`timelines_make_testdb`
* :mod:`timelines_check_testdb`



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

