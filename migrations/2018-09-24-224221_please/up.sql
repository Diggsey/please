-- Create a sequence which cycles around, because rows shouldn't live more than a few minutes
CREATE SEQUENCE please_id_seq MAXVALUE 2147483647 CYCLE;

CREATE FUNCTION please_timeout()
  RETURNS interval IMMUTABLE LANGUAGE SQL AS $$ SELECT interval '2 minutes' $$;

CREATE TABLE please_ids (
    id integer DEFAULT nextval('please_id_seq') NOT NULL PRIMARY KEY,
    creation timestamptz DEFAULT CURRENT_TIMESTAMP NOT NULL,
    expiry timestamptz DEFAULT CURRENT_TIMESTAMP + please_timeout() NOT NULL,
    title text DEFAULT '' NOT NULL,
    refresh_count integer DEFAULT 0 NOT NULL
);

CREATE FUNCTION please_refresh_expiry() RETURNS trigger AS $$
    BEGIN
        NEW.expiry := CURRENT_TIMESTAMP + please_timeout();
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER please_refresh_expiry BEFORE UPDATE ON please_ids
    FOR EACH ROW EXECUTE PROCEDURE please_refresh_expiry();
