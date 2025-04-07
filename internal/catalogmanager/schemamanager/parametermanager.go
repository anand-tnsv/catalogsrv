package schemamanager

import (
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
)

type ParameterManager interface {
	DataType() string
	Default() any
	Validate(any) apperrors.Error
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
