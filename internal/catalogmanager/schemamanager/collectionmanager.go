package schemamanager

import (
	"context"
	"encoding/json"
	"path"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type ParameterReference struct {
	Parameter string `json:"parameter"`
}

func (pr ParameterReference) String() string {
	return pr.Parameter
}

func (pr ParameterReference) Name() string {
	return path.Base(pr.Parameter)
}

func (pr ParameterReference) Path() string {
	return path.Dir(pr.Parameter)
}

type ParameterReferences []ParameterReference

func (prs ParameterReferences) Serialize() ([]byte, error) {
	s, err := json.Marshal(prs)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func DeserializeParameterReferences(b []byte) (ParameterReferences, error) {
	prs := ParameterReferences{}
	err := json.Unmarshal(b, &prs)
	return prs, err
}

type CollectionManager interface {
	ParameterNames() []string
	ValidateDependencies(context.Context, ObjectLoaders) (ParameterReferences, apperrors.Error)
	ValidateValue(ctx context.Context, loaders ObjectLoaders, param string, value types.NullableAny) apperrors.Error
	SetValue(ctx context.Context, param string, value types.NullableAny) apperrors.Error
	StorageRepresentation() *schemastore.SchemaStorageRepresentation
}
