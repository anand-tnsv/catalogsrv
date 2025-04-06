package schemamanager

import (
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schemamanager/schemastore"
)

type ParameterManager interface {
	Name() string
	Catalog() string
	Path() string
	DataType() string
	Default() any
	Validate(any) apperrors.Error
	StorageRepresentation() schemastore.SchemaStorageRepresentation
}
