package schemamanager

import (
	"context"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
)

type CollectionManager interface {
	ParameterSchemaReferences() []string
	ValidateDependencies(context.Context, ObjectLoaders) apperrors.Error
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
