package schemamanager

import (
	"context"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
)

type NamespaceManager interface {
	Name() string
	Description() string
	Catalog() string
	Variant() string
	GetNamespaceModel() *models.Namespace
	Save(context.Context) apperrors.Error
	ToJson(context.Context) ([]byte, apperrors.Error)
}
