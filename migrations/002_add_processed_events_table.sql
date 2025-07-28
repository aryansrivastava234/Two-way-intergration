-- Migration: Add processed_stripe_events table for duplicate prevention
-- File: migrations/002_add_processed_events_table.sql

CREATE TABLE IF NOT EXISTS processed_stripe_events (
    id SERIAL PRIMARY KEY,
    stripe_event_id VARCHAR(255) NOT NULL UNIQUE,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_processed_events_stripe_id ON processed_stripe_events(stripe_event_id);
CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at ON processed_stripe_events(processed_at);

-- Add comments for documentation
COMMENT ON TABLE processed_stripe_events IS 'Tracks processed Stripe webhook events to prevent duplicate processing';
COMMENT ON COLUMN processed_stripe_events.stripe_event_id IS 'Unique Stripe event ID from webhook';
COMMENT ON COLUMN processed_stripe_events.processed_at IS 'When the event was successfully processed';

-- Optional: Add cleanup job for old events (keep last 30 days)
-- DELETE FROM processed_stripe_events WHERE processed_at < NOW() - INTERVAL '30 days';