CREATE TRIGGER fkd_load_segment_id 
BEFORE DELETE on load_segments
FOR EACH ROW BEGIN
	SELECT CASE
	WHEN ((SELECT load_segment_id from timelines 
		where load_segment_id = OLD.id) IS NOT NULL)
	THEN RAISE(ABORT, 'delete on table "load_segments" violates foreign key trig/constraint on timelines')
	END;
END;	

CREATE TRIGGER fkd_timeline_id_cmds
BEFORE DELETE on timelines
FOR EACH ROW BEGIN
	SELECT CASE
	WHEN ((SELECT timeline_id from cmds 
		where timeline_id = OLD.id) IS NOT NULL)
	THEN RAISE(ABORT, 'delete on table "timelines" violates foreign key trig/constraint on cmds')
	END;
END;

CREATE TRIGGER fkd_timeline_id_cmd_fltpars
BEFORE DELETE on timelines
FOR EACH ROW BEGIN
	SELECT CASE
	WHEN ((SELECT timeline_id from cmd_fltpars 
		where timeline_id = OLD.id) IS NOT NULL)
	THEN RAISE(ABORT, 'delete on table "timelines" violates foreign key trig/constraint on cmd_fltpars')
	END;
END;

CREATE TRIGGER fkd_timeline_id_cmd_intpars
BEFORE DELETE on timelines
FOR EACH ROW BEGIN
	SELECT CASE
	WHEN ((SELECT timeline_id from cmd_intpars 
		where timeline_id = OLD.id) IS NOT NULL)
	THEN RAISE(ABORT, 'delete on table "timelines" violates foreign key trig/constraint on cmd_intpars')
	END;
END;

CREATE TRIGGER fkd_cmd_id_cmd_fltpars
BEFORE DELETE on cmds
FOR EACH ROW BEGIN
	SELECT CASE
	WHEN ((SELECT cmd_id from cmd_fltpars 
		where cmd_id = OLD.id) IS NOT NULL)
	THEN RAISE(ABORT, 'delete on table "cmds" violates foreign key trig/constraint on cmd_fltpars')
	END;
END;

CREATE TRIGGER fkd_cmd_id_cmd_intpars
BEFORE DELETE on cmds
FOR EACH ROW BEGIN
	SELECT CASE
	WHEN ((SELECT cmd_id from cmd_intpars 
		where cmd_id = OLD.id) IS NOT NULL)
	THEN RAISE(ABORT, 'delete on table "cmds" violates foreign key trig/constraint on cmd_intpars')
	END;
END;

.exit
