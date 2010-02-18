update timelines set datestop='2008:353:05:00:00.000' where id=345905553;
update timelines set fixed_by_hand=1 where id=345905553;
update load_segments set fixed_by_hand=1 where id=345905551;
update load_segments set datestop='2008:353:05:00:00.000' where id=345905551;

update timelines set datestop='2003:001:00:00:00.000' where id=183963768;
update timelines set fixed_by_hand=1 where id=183963768;
update load_segments set fixed_by_hand=1 where id=183963766;
update load_segments set datestop='2003:001:00:00:00.000' where id=183963766;

delete from load_segments where load_segment = 'CL110:1409' and year = '2003';
insert into load_segments values (167234892, 'CL110:1404', 2003, '2003:110:14:07:09.439', '2003:112:00:00:31.542', 130, 1)

insert into load_segments values (363326466, 'CL188:0402', 2009, '2009:188:04:00:00.000', '2009:188:20:57:33.571', 129, 1)
