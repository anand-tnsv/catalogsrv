package resource

import (
	"context"
	"encoding/json"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/collection"
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
	rs.Spec, _ = json.Marshal(s.Schema)

	ves := rs.Validate()
	if ves != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}

	rs.Metadata.Description = s.Description
	return buildObjectManager(ctx, rs, nil)
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
