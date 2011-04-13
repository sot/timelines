create table tl_built_loads
( 
year int not null,
load_segment varchar(10) not null,
file varchar(15) not null,
first_cmd_time varchar(22),
last_cmd_time varchar(22),
load_scs int not null,
sumfile_modtime float not null,
primary key (year, load_segment, file, load_scs, sumfile_modtime)
);

create table tl_processing
( 
year int,
dir varchar(20) not null,
file varchar(15) not null,
replan bit default 0 not null,
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
year int not null,
load_segment varchar(10) not null,
dir varchar(20) not null,
obsid int not null,
date varchar(22) not null,
primary key (year, dir, load_segment, obsid, date )
);





