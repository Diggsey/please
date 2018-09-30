-- Drop everything

DROP TRIGGER please_refresh_expiry ON please_ids;
DROP FUNCTION please_refresh_expiry();
DROP TABLE please_ids;
DROP FUNCTION please_timeout();
DROP SEQUENCE please_id_seq;
