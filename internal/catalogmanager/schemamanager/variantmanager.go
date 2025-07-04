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
	CatalogID() uuid.UUID
	Save(context.Context) apperrors.Error
	ToJson(context.Context) ([]byte, apperrors.Error)
}
