package db

import (
	"context"

	"github.com/mugiliam/hatchcatalogsrv/internal/db/dbmanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/postgresql"
	"github.com/rs/zerolog/log"
)

type DB_ interface {
	// AddScopes adds the given scopes to the connection.
	AddScopes(ctx context.Context, scopes map[string]string)
	// DropScopes drops the given scopes from the connection.
	DropScopes(ctx context.Context, scopes []string) error
	// AddScope adds the given scope with the given value to the connection.
	AddScope(ctx context.Context, scope, value string)
	// DropScope drops the given scope from the connection.
	DropScope(ctx context.Context, scope string) error
	// DropAllScopes drops all scopes from the connection.
	DropAllScopes(ctx context.Context) error
	// Close the connection to the database.
	Close(ctx context.Context)
}

const (
	Scope_TenantId  string = "hatch.curr_tenantid"
	Scope_ProjectId string = "hatch.curr_projectid"
)

var configuredScopes = []string{
	Scope_TenantId,
	Scope_ProjectId,
}

var pool dbmanager.ScopedDb

func init() {
	ctx := log.Logger.WithContext(context.Background())
	pg := dbmanager.NewScopedDb(ctx, "postgresql", configuredScopes)
	if pg == nil {
		panic("unable to create db pool")
	}
	pool = pg
}

func Conn(ctx context.Context) dbmanager.ScopedConn {
	if pool != nil {
		conn, err := pool.Conn(ctx)
		if err == nil {
			return conn
		}
	}
	return nil
}

type ctxDbKeyType string

const ctxDbKey ctxDbKeyType = "HatchCatalogDb"

func ConnCtx(ctx context.Context) context.Context {
	conn := Conn(ctx)
	return context.WithValue(ctx, ctxDbKey, conn)
}

func DB(ctx context.Context) DB_ {
	if conn, ok := ctx.Value(ctxDbKey).(dbmanager.ScopedConn); ok {
		hatchCatalogDb := postgresql.NewHatchCatalogDb(conn)
		return hatchCatalogDb
	}
	return nil
}
