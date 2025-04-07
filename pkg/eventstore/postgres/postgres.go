package postgresstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cannahum/eventsourcing-lite/pkg/eventstore"
	"github.com/google/uuid"
)

type PostgresEventStore struct {
	db *sql.DB
}

func NewPostgresEventStore(db *sql.DB) *PostgresEventStore {
	return &PostgresEventStore{db}
}

func (p *PostgresEventStore) Save(
	ctx context.Context,
	aggregateID string,
	records ...eventstore.Record,
) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO eventsourcinglite_event_store 
		(event_id, aggregate_id, version, event_type, data, metadata, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, record := range records {
		eventID := record.ID
		if eventID == "" {
			eventID = uuid.NewString()
		}
		createdAt := record.CreatedAt
		if createdAt == nil {
			now := time.Now().UTC()
			createdAt = &now
		}
		_, err := stmt.ExecContext(ctx,
			eventID, aggregateID, record.Version, record.EventType, record.Data, record.Metadata, createdAt)
		if err != nil {
			return fmt.Errorf("failed to insert record: %w", err)
		}
	}
	return tx.Commit()
}

func (p *PostgresEventStore) Load(
	ctx context.Context,
	aggregateID string,
	fromVersion, toVersion int,
) (eventstore.History, error) {
	query := `SELECT event_id, version, event_type, data, metadata, created_at
	FROM eventsourcinglite_event_store 
	WHERE aggregate_id = $1 AND version >= $2`
	args := []any{aggregateID, fromVersion}
	if toVersion > 0 {
		query += "AND version <= $3"
		args = append(args, toVersion)
	}
	query += " ORDER BY version ASC"
	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to return events: %w", err)
	}
	defer rows.Close()

	history := eventstore.History{}
	for rows.Next() {
		var record eventstore.Record
		var dataJSON, metadataJSON []byte
		if err := rows.Scan(
			&record.ID,
			&record.Version,
			&record.EventType,
			&dataJSON,
			&metadataJSON,
			&record.CreatedAt,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(dataJSON, &record.Data); err != nil {
			return nil, err
		}
		if len(metadataJSON) > 0 {
			if err := json.Unmarshal(metadataJSON, &record.Metadata); err != nil {
				return nil, err
			}
		}
		history = append(history, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return history, nil
}
