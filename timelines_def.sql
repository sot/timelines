CREATE TABLE timelines (
  id              int not null,
  load_segment_id int not null,
  dir             varchar(20),
  datestart       varchar(21) not null,
  datestop        varchar(21) not null,
  replan          bit not null,
  incomplete      bit not null,
  fixed_by_hand   bit not null,
  CONSTRAINT pk_timelines_id PRIMARY KEY (id),
  CONSTRAINT fk_timelines_load_segments_id FOREIGN KEY (load_segment_id) REFERENCES load_segments (id)
) 

