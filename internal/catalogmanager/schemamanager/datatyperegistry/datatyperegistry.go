package datatyperegistry

import (
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
)

type Loader func([]byte) (schemamanager.Parameter, apperrors.Error)

type DataTypeKey struct {
	Type    string
	Version string
}

var registry = make(map[DataTypeKey]Loader)

func RegisterDataType(k DataTypeKey, ld Loader) {
	registry[k] = ld
}

func GetLoader(k DataTypeKey) Loader {
	return registry[k]
}

func DataTypeExists(k DataTypeKey) bool {
	_, exists := registry[k]
	return exists
}
