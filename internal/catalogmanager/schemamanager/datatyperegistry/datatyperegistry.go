package datatyperegistry

import (
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
)

type Loader func([]byte) (schemamanager.Parameter, apperrors.Error)

var registry = make(map[schemamanager.ParamDataType]Loader)

func RegisterDataType(k schemamanager.ParamDataType, ld Loader) {
	registry[k] = ld
}

func GetLoader(k schemamanager.ParamDataType) Loader {
	return registry[k]
}

func DataTypeExists(k schemamanager.ParamDataType) bool {
	_, exists := registry[k]
	return exists
}
