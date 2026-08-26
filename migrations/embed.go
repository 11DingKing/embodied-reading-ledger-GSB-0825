// Package migrations embeds the SQL migration files so the server can apply
// them at startup without depending on the working directory. The .sql files
// live alongside this file at the repository's migrations/ directory.
package migrations

import "embed"

// FS holds every migration, applied in lexical filename order.
//
//go:embed *.sql
var FS embed.FS
