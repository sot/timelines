CREATE TABLE cmd_states (
     datestart     varchar(21) not null,
     datestop      varchar(21) not null,
     tstart        float(16)     not null,
     tstop         float(16)     not null,
     obsid         int         not null,
     power_cmd     varchar(11) not null,
     si_mode       varchar( 8) not null,
     pcad_mode     varchar( 6) not null,
     vid_board     bit         not null,
     clocking      bit         not null,
     fep_count     int         not null,
     ccd_count     int         not null,
     simpos        int         not null,
     simfa_pos     int         not null,
     pitch         float       not null,
     ra            float       not null,
     dec           float       not null,
     roll          float       not null,
     q1            float       not null,
     q2            float       not null,
     q3            float       not null,
     q4            float       not null,
     trans_keys    varchar(60) not null,
     hetg          varchar(4)  null,
     letg          varchar(4)  null,
  CONSTRAINT pk_cmd_states_datestart PRIMARY KEY (datestart)
)
;
CREATE INDEX idx_cmd_states_obsid ON cmd_states (obsid)
;
CREATE INDEX idx_cmd_states_datestart ON cmd_states (datestart)
;
CREATE INDEX idx_cmd_states_datestop ON cmd_states (datestop)

