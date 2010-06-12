TASK = timelines

include /proj/sot/ska/include/Makefile.FLIGHT


SHARE = update_load_seg_db.py parse_cmd_load_gen.pl update_load_seg_db.py \
	fix_load_segments.py fix_tl_processing.py \
	load_segments_def.sql timeline_loads_def.sql timelines_def.sql tl_dep_def.sql 
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

