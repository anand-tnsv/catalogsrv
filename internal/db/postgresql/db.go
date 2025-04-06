// Description: This file contains the implementation of the hatchCatalogDb interface for the PostgreSQL database.
package postgresql

import (
	"context"
	"database/sql"

	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dbmanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/types"
	"github.com/rs/zerolog/log"
)

type hatchCatalogDb struct {
	c dbmanager.ScopedConn
}

func NewHatchCatalogDb(c dbmanager.ScopedConn) *hatchCatalogDb {
	return &hatchCatalogDb{c: c}
}

func (h *hatchCatalogDb) conn() *sql.Conn {
	return h.c.Conn()
}

func (h *hatchCatalogDb) AddScopes(ctx context.Context, scopes map[string]string) {
	h.c.AddScopes(ctx, scopes)
}

func (h *hatchCatalogDb) DropScopes(ctx context.Context, scopes []string) error {
	return h.c.DropScopes(ctx, scopes)
}

func (h *hatchCatalogDb) AddScope(ctx context.Context, scope, value string) {
	h.c.AddScope(ctx, scope, value)
}

func (h *hatchCatalogDb) DropScope(ctx context.Context, scope string) error {
	return h.c.DropScope(ctx, scope)
}

func (h *hatchCatalogDb) DropAllScopes(ctx context.Context) error {
	return h.c.DropAllScopes(ctx)
}

func (h *hatchCatalogDb) PingContext(ctx context.Context) error {
	return h.c.Conn().PingContext(ctx)
}

func (h *hatchCatalogDb) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return h.c.Conn().ExecContext(ctx, query, args...)
}

func (h *hatchCatalogDb) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return h.c.Conn().QueryContext(ctx, query, args...)
}

func (h *hatchCatalogDb) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return h.c.Conn().QueryRowContext(ctx, query, args...)
}

func (h *hatchCatalogDb) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return h.c.Conn().PrepareContext(ctx, query)
}

func (h *hatchCatalogDb) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return h.c.Conn().BeginTx(ctx, opts)
}

func (h *hatchCatalogDb) Close(ctx context.Context) {
	h.c.Close(ctx)
}

func getTenantAndProjectFromContext(ctx context.Context) (tenantID types.TenantId, projectID types.ProjectId, err error) {
	err = nil
	tenantID = common.TenantIdFromContext(ctx)
	projectID = common.ProjectIdFromContext(ctx)

	// Validate tenantID and projectID to ensure they are not empty
	if tenantID == "" {
		err = dberror.ErrMissingTenantID
	} else if projectID == "" {
		err = dberror.ErrMissingProjecID
	}
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve tenant and project IDs from context")
	}
	return
}
