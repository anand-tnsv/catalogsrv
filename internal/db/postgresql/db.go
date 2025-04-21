// Description: This file contains the implementation of the hatchCatalogDb interface for the PostgreSQL database.
package postgresql

import (
	"context"
	"database/sql"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dbmanager"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

type hatchCatalogDb struct {
	c  dbmanager.ScopedConn
	tp *tenantProject
	cm *catalog
}

func NewHatchCatalogDb(c dbmanager.ScopedConn) *hatchCatalogDb {
	h := &hatchCatalogDb{c: c}
	h.tp = NewTenantProjectManager(c)
	h.cm = newCatalogManager(h)
	return h
}

func NewHatchCatalogDbWithManagers(c dbmanager.ScopedConn) {
	c.SetTenantProjectManager(NewTenantProjectManager(c))
	c.SetCatalogManager(newCatalogManager(c))
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

func (h *hatchCatalogDb) Close(ctx context.Context) {
	h.c.Close(ctx)
}

func getTenantAndProjectFromContext(ctx context.Context) (tenantID types.TenantId, projectID types.ProjectId, err apperrors.Error) {
	err = nil
	tenantID = common.TenantIdFromContext(ctx)
	projectID = common.ProjectIdFromContext(ctx)

	// Validate tenantID and projectID to ensure they are not empty
	if tenantID == "" {
		err = dberror.ErrMissingTenantID.Err(dberror.ErrInvalidInput)
	} else if projectID == "" {
		err = dberror.ErrMissingProjecID.Err(dberror.ErrInvalidInput)
	}
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve tenant and project IDs from context")
	}
	return
}
