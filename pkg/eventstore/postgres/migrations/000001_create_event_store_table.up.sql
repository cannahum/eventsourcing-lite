CREATE TABLE eventsourcinglite_event_store (
    aggregate_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    data JSONB NOT NULL,
    metadata JSONB NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (aggregate_id, version),
    UNIQUE (event_id) -- Ensures global uniqueness for deduplication
);
