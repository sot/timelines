create table tl_built_loads
( 
year int,
load_segment varchar(10),
file varchar(15),
first_cmd_time varchar(22),
last_cmd_time varchar(22),
load_scs int,
sumfile_modtime float,
primary key (year, load_segment, file, sumfile_modtime)
);

create table tl_processing
( 
year int,
dir varchar(20),
file varchar(15),
replan boolean,
continuity_cmds varchar(10),
replan_cmds varchar(10),
bcf_cmd_count int,
planning_tstart varchar(22),
planning_tstop varchar(22),
processing_tstart varchar(22),
processing_tstop varchar(22),
execution_tstart varchar(22),
sumfile_modtime float,
primary key (dir, file)
);


create table tl_obsids
( 
year int,
load_segment varchar(10),
dir varchar(20),
obsid int,
date varchar(22),
primary key (year, dir, load_segment, obsid, date )
);





