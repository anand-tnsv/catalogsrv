package schemamanager

import "github.com/mugiliam/common/apperrors"

type ParameterManager interface {
	Name() string
	Catalog() string
	Path() string
	DataType() string
	Default() any
	Validate(any) apperrors.Error
}
