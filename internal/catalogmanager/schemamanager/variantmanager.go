package schemamanager

import (
	"context"

	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
)

type VariantManager interface {
	ID() uuid.UUID
	Name() string
	Description() string
	Catalog() string
	CatalogID() uuid.UUID
	Save(context.Context) apperrors.Error
}
