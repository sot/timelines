from distutils.core import setup

version = 0.08

license = """\
New BSD/3-clause BSD License
Copyright (c) 2015 Smithsonian Astrophysical Observatory
All rights reserved."""

setup(name='timelines',
      description=('Determine which load segments have run or will run, and '
                   'which command products were used to create those load segments'),
      version=str(version),
      author='Jean Connelly',
      author_email='jconnelly@cfa.harvard.edu',
      license=license,
      zip_safe=False,
      packages=['timelines', 'timelines.load_seg_changes', 'timelines.load_seg_changes.web'],
      package_data={'timelines.load_seg_changes.web': ['templates/*/*.html', 'templates/*.html']},
      )
