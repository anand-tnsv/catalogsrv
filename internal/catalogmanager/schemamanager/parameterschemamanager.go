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

type ParamValue struct {
	Value    types.NullableAny `json:"value"`
	DataType ParamDataType     `json:"dataType"`
}

type ParamValues map[string]ParamValue

type ParameterSchemaManager interface {
	DataType() ParamDataType
	Default() any
	ValidateValue(types.NullableAny) apperrors.Error
	ValidateDependencies(ctx context.Context, loaders SchemaLoaders, collectionRefs SchemaReferences) apperrors.Error
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
