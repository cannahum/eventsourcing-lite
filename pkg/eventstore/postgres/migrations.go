package postgresstore

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

const migrationsTable = "eventsourcinglite_event_store_migrations"

//go:embed migrations/*.sql
var fs embed.FS

func RunMigrations(db *sql.DB, dbName string) error {
	d, err := iofs.New(fs, "migrations")
	if err != nil {
		return fmt.Errorf("failed to create iofs data source: %w", err)
	}
	// Create database connection with migrations library
	driver, err := postgres.WithInstance(db, &postgres.Config{
		MigrationsTable: migrationsTable,
	})
	if err != nil {
		return fmt.Errorf("failed to connect golang-migrate with db instance: %w", err)
	}
	// Set up migrations directory (folder that contains all the migrations)
	m, err := migrate.NewWithInstance("iofs", d, dbName, driver)
	if err != nil {
		return fmt.Errorf("failed to create migration instance: %w", err)
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}
	return nil
}
