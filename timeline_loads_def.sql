CREATE VIEW timeline_loads AS
 SELECT 
  tl.id            as id,
  ls.id            as load_segment_id,
  ls.load_segment  as name,
  ls.year          as year,
  tl.datestart     as datestart,
  tl.datestop      as datestop,
  ls.load_scs      as scs,
  tl.dir           as mp_dir,
  tl.replan        as replan,
  tl.incomplete    as predicted,
  tl.fixed_by_hand as fixed_by_hand
 FROM timelines as tl, load_segments as ls
 WHERE tl.load_segment_id = ls.id
