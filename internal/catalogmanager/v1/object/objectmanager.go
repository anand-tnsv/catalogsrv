package resource

import (
	"context"
	"encoding/json"

	log "github.com/rs/zerolog/log"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/collection"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/parameter"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type V1ObjectManager struct {
	resourceSchema    *ObjectSchema
	parameterManager  *parameter.V1ParameterManager
	collectionManager *collection.V1CollectionManager
}

var _ schemamanager.ObjectManager = &V1ObjectManager{} // Ensure V1ObjectManager implements schemamanager.ObjectManager

func NewV1ObjectManager(ctx context.Context, rsrcJson []byte, options ...schemamanager.Options) (*V1ObjectManager, apperrors.Error) {
	o := schemamanager.OptionsConfig{}
	for _, option := range options {
		option(&o)
	}

	rs, err := ReadObjectSchema(string(rsrcJson))
	if err != nil {
		return nil, err
	}

	if rs.Version != "v1" {
		return nil, validationerrors.ErrInvalidVersion
	}
	if o.Validate {
		ves := rs.Validate()
		if ves != nil {
			return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
		}
	}
	return buildObjectManager(ctx, rs, rsrcJson, options...)
}

func LoadV1ObjectManager(ctx context.Context, s *schemastore.SchemaStorageRepresentation, m *schemamanager.ObjectMetadata) (*V1ObjectManager, apperrors.Error) {
	rs := &ObjectSchema{}
	rs.Version = s.Version
	switch s.Type {
	case types.CatalogObjectTypeParameterSchema:
		rs.Kind = "Parameter"
	case types.CatalogObjectTypeCollectionSchema:
		rs.Kind = "Collection"
	}
	rs.Metadata = *m
	rs.Spec = s.Schema

	ves := rs.Validate()
	if ves != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	var opts []schemamanager.Options
	if s.Values != nil && len(s.Values) > 0 && json.Valid(s.Values) {
		opts = append(opts, schemamanager.WithParamValues(s.Values))
	}

	rs.Metadata.Description = s.Description
	return buildObjectManager(ctx, rs, nil, opts...)
}

func buildObjectManager(ctx context.Context, rs *ObjectSchema, rsrcJson []byte, options ...schemamanager.Options) (*V1ObjectManager, apperrors.Error) {
	if rs == nil {
		return nil, validationerrors.ErrEmptySchema
	}
	if rsrcJson == nil {
		rsrcJson, _ = json.Marshal(rs)
	}
	rm := &V1ObjectManager{
		resourceSchema: rs,
	}

	// Initialize the appropriate manager based on the kind
	var err apperrors.Error
	switch rs.Kind {
	case "Parameter":
		if rm.parameterManager, err = parameter.NewV1ParameterManager(ctx, rs.Version, rsrcJson, options...); err != nil {
			return nil, err
		}
	case "Collection":
		if rm.collectionManager, err = collection.NewV1CollectionManager(ctx, rs.Version, rsrcJson, options...); err != nil {
			return nil, err
		}
	default:
		return nil, validationerrors.ErrInvalidKind
	}

	return rm, nil

}

func (rm *V1ObjectManager) Version() string {
	return rm.resourceSchema.Version
}

func (rm *V1ObjectManager) Kind() string {
	return rm.resourceSchema.Kind
}

func (rm *V1ObjectManager) Type() types.CatalogObjectType {
	switch rm.Kind() {
	case "Parameter":
		return types.CatalogObjectTypeParameterSchema
	case "Collection":
		return types.CatalogObjectTypeCollectionSchema
	default:
		return types.CatalogObjectTypeInvalid
	}
}
func (rm *V1ObjectManager) Metadata() schemamanager.ObjectMetadata {
	return rm.resourceSchema.Metadata
}

func (rm *V1ObjectManager) Name() string {
	return rm.resourceSchema.Metadata.Name
}

func (rm *V1ObjectManager) Path() string {
	return rm.resourceSchema.Metadata.Path
}

func (rm *V1ObjectManager) FullyQualifiedName() string {
	return rm.resourceSchema.Metadata.Path + "/" + rm.resourceSchema.Metadata.Name
}

func (rm *V1ObjectManager) Catalog() string {
	return rm.resourceSchema.Metadata.Catalog
}

func (rm *V1ObjectManager) Description() string {
	return rm.resourceSchema.Metadata.Description
}

func (rm *V1ObjectManager) SetName(name string) {
	rm.resourceSchema.Metadata.Name = name
}

func (rm *V1ObjectManager) SetPath(path string) {
	rm.resourceSchema.Metadata.Path = path
}

func (rm *V1ObjectManager) SetCatalog(catalog string) {
	rm.resourceSchema.Metadata.Catalog = catalog
}

func (rm *V1ObjectManager) SetDescription(description string) {
	rm.resourceSchema.Metadata.Description = description
}

func (rm *V1ObjectManager) ParameterManager() schemamanager.ParameterManager {
	return rm.parameterManager
}

func (rm *V1ObjectManager) CollectionManager() schemamanager.CollectionManager {
	return rm.collectionManager
}

func (rm *V1ObjectManager) StorageRepresentation() *schemastore.SchemaStorageRepresentation {
	var s *schemastore.SchemaStorageRepresentation = nil
	switch rm.Kind() {
	case "Parameter":
		if rm.parameterManager != nil {
			s = rm.parameterManager.StorageRepresentation()
		}
	case "Collection":
		if rm.collectionManager != nil {
			s = rm.collectionManager.StorageRepresentation()
		}
	}
	s.Description = rm.resourceSchema.Metadata.Description
	return s
}

func (rm *V1ObjectManager) ToJson(ctx context.Context) ([]byte, apperrors.Error) {
	j, err := json.Marshal(rm.resourceSchema)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to marshal object schema")
		return j, errors.ErrUnableToLoadObject
	}
	return j, nil
}

func (rm *V1ObjectManager) Compare(other schemamanager.ObjectManager, excludeMetadata bool) bool {
	thisObj := rm.StorageRepresentation()
	otherObj := other.StorageRepresentation()
	// to exclude metadata, just exclude description. If there are other values in future, we need to do more here.
	if excludeMetadata {
		thisObj.Description = ""
		otherObj.Description = ""
	}
	return thisObj.GetHash() == otherObj.GetHash()
}
