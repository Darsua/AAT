package migrations

import _ "embed"

// InitSQL contains the idempotent schema initialization and reference seed data.
//
//go:embed 001_init.sql
var InitSQL string
