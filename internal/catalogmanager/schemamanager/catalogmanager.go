package schemamanager

import (
	"context"

	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
)

type CatalogManager interface {
	ID() uuid.UUID
	Name() string
	Description() string
	Save(context.Context) apperrors.Error
	Load(ctx context.Context, name string) apperrors.Error
}
