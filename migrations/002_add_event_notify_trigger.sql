-- Function to notify on new events
CREATE OR REPLACE FUNCTION notify_new_event()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('new_event', json_build_object(
        'id', NEW.id,
        'source_type', NEW.source_type,
        'external_id', NEW.external_id,
        'event_time', NEW.event_time,
        'lat', NEW.lat,
        'lon', NEW.lon,
        'category', NEW.category,
        'description', NEW.description
    )::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger on events table
DROP TRIGGER IF EXISTS events_notify_trigger ON events;
CREATE TRIGGER events_notify_trigger
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION notify_new_event();
