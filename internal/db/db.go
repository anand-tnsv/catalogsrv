package db

import (
	"context"

	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
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
	CreateCatalog(ctx context.Context, catalog *models.Catalog) apperrors.Error
	GetCatalogIDByName(ctx context.Context, catalogName string) (uuid.UUID, apperrors.Error)
	GetCatalog(ctx context.Context, catalogID uuid.UUID, name string) (*models.Catalog, apperrors.Error)
	UpdateCatalog(ctx context.Context, catalog models.Catalog) apperrors.Error
	DeleteCatalog(ctx context.Context, catalogID uuid.UUID, name string) apperrors.Error

	// Variant
	CreateVariant(ctx context.Context, variant *models.Variant) apperrors.Error
	GetVariant(ctx context.Context, catalogID uuid.UUID, variantID uuid.UUID, name string) (*models.Variant, apperrors.Error)
	GetVariantIDFromName(ctx context.Context, catalogID uuid.UUID, name string) (uuid.UUID, apperrors.Error)
	UpdateVariant(ctx context.Context, variantID uuid.UUID, name string, updatedVariant *models.Variant) apperrors.Error
	DeleteVariant(ctx context.Context, catalogID uuid.UUID, variantID uuid.UUID, name string) apperrors.Error

	// Version
	CreateVersion(ctx context.Context, version *models.Version) error
	GetVersion(ctx context.Context, versionNum int, variantID, catalogID uuid.UUID) (*models.Version, error)
	GetVersionByLabel(ctx context.Context, label string, catalogID, variantID uuid.UUID) (*models.Version, error)
	SetVersionLabel(ctx context.Context, versionNum int, variantID, catalogID uuid.UUID, newLabel string) error
	UpdateVersionDescription(ctx context.Context, versionNum int, variantID, catalogID uuid.UUID, newDescription string) error
	DeleteVersion(ctx context.Context, versionNum int, variantID, catalogID uuid.UUID) error
	CountVersionsInCatalogAndVariant(ctx context.Context, catalogID, variantID uuid.UUID) (int, error)
	GetNamedVersions(ctx context.Context, catalogID, variantID uuid.UUID) ([]models.Version, error)

	// Workspace
	CreateWorkspace(ctx context.Context, workspace *models.Workspace) apperrors.Error
	DeleteWorkspace(ctx context.Context, workspaceID uuid.UUID) apperrors.Error
	GetWorkspace(ctx context.Context, workspaceID uuid.UUID) (*models.Workspace, apperrors.Error)
	UpdateWorkspaceLabel(ctx context.Context, workspaceID uuid.UUID, newLabel string) apperrors.Error

	// Catalog Object
	CreateCatalogObject(ctx context.Context, obj *models.CatalogObject) apperrors.Error
	GetCatalogObject(ctx context.Context, hash string) (*models.CatalogObject, apperrors.Error)

	// Schema Directory
	CreateSchemaDirectory(ctx context.Context, t types.CatalogObjectType, dir *models.SchemaDirectory) apperrors.Error
	SetDirectory(ctx context.Context, t types.CatalogObjectType, id uuid.UUID, dir []byte) apperrors.Error
	GetDirectory(ctx context.Context, t types.CatalogObjectType, id uuid.UUID) ([]byte, apperrors.Error)
	GetSchemaDirectory(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID) (*models.SchemaDirectory, apperrors.Error)
	GetObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (*models.ObjectRef, apperrors.Error)
	AddOrUpdateObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string, obj models.ObjectRef) apperrors.Error
	DeleteObjectByPath(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (bool, apperrors.Error)
	FindClosestObject(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, targetName, startPath string) (string, *models.ObjectRef, apperrors.Error)
	PathExists(ctx context.Context, t types.CatalogObjectType, directoryID uuid.UUID, path string) (bool, apperrors.Error)

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
