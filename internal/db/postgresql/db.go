package postgresql

import (
	"context"

	"github.com/mugiliam/hatchcatalogsrv/internal/db/dbmanager"
)

type hatchCatalogDb struct {
	conn dbmanager.ScopedConn
}

func NewHatchCatalogDb(conn dbmanager.ScopedConn) *hatchCatalogDb {
	return &hatchCatalogDb{conn: conn}
}

func (h *hatchCatalogDb) AddScopes(ctx context.Context, scopes map[string]string) {
	h.conn.AddScopes(ctx, scopes)
}

func (h *hatchCatalogDb) DropScopes(ctx context.Context, scopes []string) error {
	return h.conn.DropScopes(ctx, scopes)
}

func (h *hatchCatalogDb) AddScope(ctx context.Context, scope, value string) {
	h.conn.AddScope(ctx, scope, value)
}

func (h *hatchCatalogDb) DropScope(ctx context.Context, scope string) error {
	return h.conn.DropScope(ctx, scope)
}

func (h *hatchCatalogDb) DropAllScopes(ctx context.Context) error {
	return h.conn.DropAllScopes(ctx)
}

func (h *hatchCatalogDb) Close(ctx context.Context) {
	h.conn.Close(ctx)
}
