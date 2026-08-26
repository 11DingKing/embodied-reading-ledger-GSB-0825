package config

import "os"

type Config struct {
	Port        string
	DatabaseURL string
}

func Load() Config {
	return Config{
		Port:        envOr("PORT", "8080"),
		DatabaseURL: envOr("DATABASE_URL", "postgres://ledger:ledger@localhost:5433/ledger?sslmode=disable"),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
