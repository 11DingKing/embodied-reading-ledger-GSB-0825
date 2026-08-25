package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port        string
	DatabaseURL string
	MaxConns    int32
	MaxConnLife time.Duration
}

func Load() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"
	}
	maxConns := int32(10)
	if v := os.Getenv("DB_MAX_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxConns = int32(n)
		}
	}
	return Config{
		Port:        port,
		DatabaseURL: dbURL,
		MaxConns:    maxConns,
		MaxConnLife: 30 * time.Minute,
	}
}
