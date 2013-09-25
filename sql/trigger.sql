CREATE or replace FUNCTION notify_trigger() RETURNS trigger AS $$

DECLARE

BEGIN

  -- if the record has locked_at null then it it waiting to be processed
  -- so we send the notification
  -- the payload includes the message id, so it can be processed right away

  IF NEW.locked_at is null THEN
    EXECUTE 'NOTIFY ' || NEW.name || ', ' || quote_literal(NEW.id);
  END IF;
  RETURN NEW;
END;

$$ LANGUAGE plpgsql;

drop trigger trunk_queue_trigger on trunk_queue;
CREATE TRIGGER trunk_queue_trigger AFTER insert or update on trunk_queue FOR EACH ROW execute procedure notify_trigger();
