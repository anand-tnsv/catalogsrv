package schemamanager

import (
	"context"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type ParamDataType struct {
	Type    string `json:"type"`
	Version string `json:"version"`
}

func (dt ParamDataType) Equals(other ParamDataType) bool {
	return dt.Type == other.Type && dt.Version == other.Version
}

type ParamValue struct {
	Value    types.NullableAny `json:"value"`
	DataType ParamDataType     `json:"dataType"`
}

func (pv ParamValue) Equals(other ParamValue) bool {
	return pv.Value.Equals(other.Value) && pv.DataType.Equals(other.DataType)
}

type ParamValues map[string]ParamValue

type ParameterSchemaManager interface {
	DataType() ParamDataType
	Default() any
	ValidateValue(types.NullableAny) apperrors.Error
	ValidateDependencies(ctx context.Context, loaders SchemaLoaders, collectionRefs SchemaReferences) apperrors.Error
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
