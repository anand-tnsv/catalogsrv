package db

import (
	"context"

	"github.com/google/uuid"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dbmanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/postgresql"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
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

	// Variant
	CreateVariant(ctx context.Context, variant *models.Variant) error
	GetVariant(ctx context.Context, catalogID uuid.UUID, variantID uuid.UUID, name string) (*models.Variant, error)
	UpdateVariant(ctx context.Context, variantID uuid.UUID, name string, updatedVariant *models.Variant) error
	DeleteVariant(ctx context.Context, catalogID uuid.UUID, variantID uuid.UUID, name string) error

	// Scope Management
	AddScopes(ctx context.Context, scopes map[string]string)
	DropScopes(ctx context.Context, scopes []string) error
	AddScope(ctx context.Context, scope, value string)
	DropScope(ctx context.Context, scope string) error
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
		log.Ctx(ctx).Error().Err(err).Msg("unable to get db connection")
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
	log.Ctx(ctx).Error().Msg("unable to get db connection from context")
	return nil
}
