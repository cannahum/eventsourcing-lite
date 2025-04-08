package testutils

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
)

type PostgresConf struct {
	Host         string
	Port         int    `default:"5432"`
	DBName       string `envconfig:"db_name" default:"postgres"`
	Username     string `default:"postgres"`
	Password     string
	CredArn      string `envconfig:"cred_arn"`
	SslMode      string `envconfig:"ssl_mode" default:"disable"`
	DBConn       *sql.DB
	postgresConn *sql.DB
}

func NewPostgres(cfg PostgresConf) (*PostgresConf, error) {
	pg := &PostgresConf{
		Host:     cfg.Host,
		Port:     cfg.Port,
		DBName:   "",
		Username: cfg.Username,
		Password: cfg.Password,
		CredArn:  "",
		SslMode:  cfg.SslMode,
	}

	adminDsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s sslmode=%s dbname=postgres",
		pg.Host, pg.Port, pg.Username, pg.Password, pg.SslMode,
	)
	adminDB, err := sql.Open("postgres", adminDsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	if err = adminDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping admin DB: %w", err)
	}
	pg.postgresConn = adminDB
	return pg, nil
}

func (pg *PostgresConf) CreateTestDB() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := pg.postgresConn.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE %s`, pg.DBName))
	if err != nil {
		return fmt.Errorf("failed to create test database %s\n%w", pg.DBName, err)
	}
	// Connect to the created database
	testDsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s sslmode=%s dbname=%s",
		pg.Host, pg.Port, pg.Username, pg.Password, pg.SslMode, pg.DBName,
	)
	testDB, err := sql.Open("postgres", testDsn)
	if err != nil {
		return fmt.Errorf("failed to connect to test database %w", err)
	}
	if err = testDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping test DB %s: %w", pg.DBName, err)
	}
	pg.DBConn = testDB
	return nil

}

func (pg *PostgresConf) DropTestDB() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Disconnect before dropping
	if pg.DBConn != nil {
		_ = pg.DBConn.Close()
	}
	pg.DBConn = nil
	_, err := pg.postgresConn.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, pg.DBName))
	if err != nil {
		return fmt.Errorf("failed to drop test database: %w", err)
	}
	pg.postgresConn.Close()
	pg.postgresConn = nil
	return nil
}
