// Package config resolves runtime configuration from the environment with sane
// defaults that match docker-compose.yml, so a fresh clone runs with zero setup.
package config

import (
	"fmt"
	"os"
)

// Config holds resolved server configuration.
type Config struct {
	// DatabaseURL is the pgx connection string.
	DatabaseURL string
	// Addr is the HTTP listen address.
	Addr string
}

// DefaultDatabaseURL points at the docker-compose db published on host port
// 5439 (chosen to avoid clashing with other local Postgres instances).
const DefaultDatabaseURL = "postgres://ledger:ledger@localhost:5439/ledger?sslmode=disable"

// DefaultAddr is the default HTTP listen address.
const DefaultAddr = ":8080"

// Load reads configuration from the environment, applying defaults.
func Load() Config {
	return Config{
		DatabaseURL: getenv("DATABASE_URL", DefaultDatabaseURL),
		Addr:        getenv("ADDR", DefaultAddr),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// String returns a redacted, printable summary for logs.
func (c Config) String() string {
	return fmt.Sprintf("addr=%s db=%s", c.Addr, redact(c.DatabaseURL))
}

// redact hides the password portion of a connection URL for logging.
func redact(url string) string {
	// Best-effort: mask everything between "://user:" and "@".
	at := -1
	for i := 0; i < len(url); i++ {
		if url[i] == '@' {
			at = i
			break
		}
	}
	if at == -1 {
		return url
	}
	colon := -1
	for i := 0; i < at; i++ {
		if url[i] == ':' && i > 0 && url[i-1] != '/' && i+1 < len(url) && url[i+1] != '/' {
			colon = i
		}
	}
	if colon == -1 {
		return url
	}
	return url[:colon+1] + "****" + url[at:]
}
