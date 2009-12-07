CREATE TABLE load_segments (
 id              int          not null,
 load_segment    varchar(15)  not null,
 year            int          not null,
 datestart       varchar(21)  not null,
 datestop        varchar(21)  not null,
 load_scs        int          not null,
 fixed_by_hand   bit          not null,
 CONSTRAINT pk_load_segments_id PRIMARY KEY (id)
)
