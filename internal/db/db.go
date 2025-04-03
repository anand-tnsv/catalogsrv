package db

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dbmanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/postgresql"
	"github.com/mugiliam/hatchcatalogsrv/internal/types"
	"github.com/rs/zerolog/log"
)

// DB_ is an interface for the database connection. It wraps the underlying sql.Conn interface while
// adding the ability to manage scopes.
type DB_ interface {
	// Tenant and Project
	CreateTenant(ctx context.Context, tenantID types.TenantId) error
	GetTenant(ctx context.Context, tenantID types.TenantId) (*models.Tenant, error)
	DeleteTenant(ctx context.Context, tenantID types.TenantId) error
	CreateProject(ctx context.Context, projectID types.ProjectId) error
	GetProject(ctx context.Context, projectID types.ProjectId) (*models.Project, error)
	DeleteProject(ctx context.Context, projectID types.ProjectId) error

	// Catalog
	CreateCatalog(ctx context.Context, catalog *models.Catalog) error
	GetCatalog(ctx context.Context, catalogID uuid.UUID, name string) (*models.Catalog, error)
	UpdateCatalog(ctx context.Context, catalog models.Catalog) error
	DeleteCatalog(ctx context.Context, catalogID uuid.UUID, name string) error

	// Scope Management
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
	// PingContext verifies a connection to the database is still alive,
	PingContext(ctx context.Context) error
	// ExecContext executes a query without returning any rows.
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	// QueryContext executes a query that returns rows, typically a SELECT.
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	// QueryRowContext executes a query that is expected to return at most one row.
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	// PrepareContext creates a prepared statement for later queries or executions.
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	// BeginTx starts a transaction.
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
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
