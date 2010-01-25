TASK = timelines

include /proj/sot/ska/include/Makefile.FLIGHT

SHARE = parse_cmd_load_gen.pl update_load_seg_db.py make_new_tables.py 
DATA = task_schedule.cfg

install: 
ifdef SHARE
	mkdir -p $(INSTALL_SHARE)
	rsync --times --cvs-exclude $(SHARE) $(INSTALL_SHARE)/
endif
ifdef DATA
	mkdir -p $(INSTALL_DATA)
	rsync --times --cvs-exclude $(DATA) $(INSTALL_DATA)/
endif

