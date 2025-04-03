package dbmanager

import (
	"context"

	"github.com/rs/zerolog/log"
)

type ScopedDb interface {
	// Conn returns a new connection to the database.
	// Returns a ScopedConn and an error, if any.
	Conn(ctx context.Context) (ScopedConn, error)
	// Stats returns the number of connection requests and returns.
	Stats() (requests, returns uint64)
}

type ScopedConn interface {
	AddScopes(ctx context.Context, scopes map[string]string)
	DropScopes(ctx context.Context, scopes []string) error
	AddScope(ctx context.Context, scope, value string)
	DropScope(ctx context.Context, scope string) error
	DropAllScopes(ctx context.Context) error
	Conn() any
	Close(ctx context.Context)
}

func NewScopedDb(ctx context.Context, dbtype string, configuredScopes []string) ScopedDb {
	switch dbtype {
	case "postgresql":
		db, err := NewPostgresqlDb(configuredScopes)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("Failed to create PostgreSQL DB")
			return nil
		}
		return db
	}
	return nil
}
