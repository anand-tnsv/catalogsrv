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
	ToJson(context.Context) ([]byte, apperrors.Error)
}
