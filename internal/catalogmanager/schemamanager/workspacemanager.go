package schemamanager

import (
	"context"

	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
)

type WorkspaceManager interface {
	ID() uuid.UUID
	Description() string
	CatalogID() uuid.UUID
	VariantID() uuid.UUID
	BaseVersion() int
	ParametersDir() uuid.UUID
	CollectionsDir() uuid.UUID
	Save(context.Context) apperrors.Error
}
