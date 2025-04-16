package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

type collectionSchema struct {
	Version  string                    `json:"version" validate:"required"`
	Kind     string                    `json:"kind" validate:"required,oneof=Collection"`
	Metadata CollectionMetadata        `json:"metadata" validate:"required"`
	Spec     collectionSpec            `json:"spec" validate:"required"`
	Values   schemamanager.ParamValues `json:"-"`
}

type CollectionMetadata struct {
	Name        string               `json:"name" validate:"required,nameFormatValidator"`
	Catalog     string               `json:"catalog" validate:"required,resourceNameValidator"`
	Variant     types.NullableString `json:"variant" validate:"required,resourceNameValidator"`
	Path        string               `json:"path" validate:"required,resourcePathValidator"`
	Description string               `json:"description"`
}

type collectionSpec struct {
	Schema string                       `json:"schema" validate:"required,nameFormatValidator"`
	Values map[string]types.NullableAny `json:"values"`
}

func (cs *collectionSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(cs)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(cs).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())
		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "oneof":
			ves = append(ves, schemaerr.ErrInvalidFieldSchema(jsonFieldName, e.Value().(string)))
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "resourcePathValidator":
			ves = append(ves, schemaerr.ErrInvalidObjectPath(jsonFieldName))
		case "catalogVersionValidator":
			ves = append(ves, schemaerr.ErrInvalidCatalogVersion(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return ves
}

type collectionManager struct {
	schema collectionSchema                      // schema for the collection
	csm    schemamanager.CollectionSchemaManager // collection schema manager
}

func (cm *collectionManager) Schema() string {
	return cm.schema.Spec.Schema
}

func (cm *collectionManager) CollectionSchema() []byte {
	b, _ := json.Marshal(cm.schema.Spec)
	return b
}

func (cm *collectionManager) Metadata() schemamanager.SchemaMetadata {
	return schemamanager.SchemaMetadata{
		Name:        cm.schema.Metadata.Name,
		Catalog:     cm.schema.Metadata.Catalog,
		Variant:     cm.schema.Metadata.Variant,
		Path:        cm.schema.Metadata.Path,
		Description: cm.schema.Metadata.Description,
	}
}

func (cm *collectionManager) CollectionSchemaManager() schemamanager.CollectionSchemaManager {
	return cm.csm
}

func (cm *collectionManager) StorageRepresentation() *schemastore.SchemaStorageRepresentation {
	s := schemastore.SchemaStorageRepresentation{
		Version: cm.schema.Version,
		Type:    types.CatalogObjectTypeCatalogCollectionValue,
	}
	s.Values, _ = json.Marshal(cm.schema.Values)
	s.Schema, _ = json.Marshal(cm.schema.Spec)
	s.Description = cm.schema.Metadata.Description
	return &s
}

func (cm *collectionManager) SetCollectionSchemaManager(csm schemamanager.CollectionSchemaManager) {
	cm.csm = csm
}

func (cm *collectionManager) Values() schemamanager.ParamValues {
	return cm.schema.Values
}

func LoadCollectionSchemaManager(ctx context.Context, cm schemamanager.CollectionManager, opts ...ObjectStoreOption) apperrors.Error {
	m := &schemamanager.SchemaMetadata{
		Name:    cm.Schema(),
		Catalog: cm.Metadata().Catalog,
		Variant: cm.Metadata().Variant,
	}
	sm, err := LoadSchemaByPath(ctx,
		types.CatalogObjectTypeCollectionSchema,
		m,
		opts...,
	)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load collection schema manager")
		return err
	}
	cm.SetCollectionSchemaManager(sm.CollectionSchemaManager())
	return nil
}

func (cm *collectionManager) SetDefaultValues() apperrors.Error {
	if cm.csm == nil {
		return ErrInvalidCollectionSchema
	}
	// set default values for the collection as defined in the schema
	cm.schema.Values = cm.csm.GetDefaultValues()
	return nil
}

func (cm *collectionManager) SetValue(ctx context.Context, schemaLoaders schemamanager.SchemaLoaders, param string, value types.NullableAny) apperrors.Error {
	if cm.csm == nil {
		return ErrInvalidCollectionSchema
	}
	if err := cm.csm.ValidateValue(ctx, schemaLoaders, param, value); err != nil {
		return err
	}
	// We need to copy the dataType and other annotations from the schema before we can copy over the value
	if cm.schema.Values == nil {
		cm.schema.Values = make(schemamanager.ParamValues)
	}
	v := cm.csm.GetValue(ctx, param)
	v.Value = value
	cm.schema.Values[param] = v
	return nil
}

func (cm *collectionManager) ValidateValues(ctx context.Context, schemaLoaders schemamanager.SchemaLoaders, currValues schemamanager.ParamValues) apperrors.Error {
	if cm.csm == nil {
		return ErrInvalidCollectionSchema
	}

	// There are few things to unwrap here:
	// At this time, the schema has all the parameters set in its Values. And these values either have the default set or are nil. But
	// the dataTypes and other annotations are always set.  So we need to copy all these over to the collection and substitute with new
	// values if the collection had any new values defined. Or we will copy over the defaults. If no defaults are set, the param will be a NullableAny
	// with dataType and other annotations set.
	if cm.schema.Values == nil {
		cm.schema.Values = make(schemamanager.ParamValues)
	}
	for _, param := range cm.csm.ParameterNames() {
		if v, ok := cm.schema.Spec.Values[param]; ok {
			// if the user set any value, we'll validate it and set it. If validation fails, we will return an error.
			if err := cm.SetValue(ctx, schemaLoaders, param, v); err != nil {
				return err
			}
		} else if v, ok := currValues[param]; ok {
			// we validate this again in case the parameter schemas have changed
			if err := cm.SetValue(ctx, schemaLoaders, param, v.Value); err != nil {
				return err
			}
		} else {
			// the values in the schema are already either the default or nil. But the dataType and other annotations are set. So it is safe to just copy over.
			cm.schema.Values[param] = cm.csm.GetValue(ctx, param)
		}
	}
	return nil
}

func NewCollectionManager(ctx context.Context, rsrcJson []byte, m *schemamanager.SchemaMetadata) (schemamanager.CollectionManager, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}

	// get the metadata, replace fields in json from provided metadata. Set defaults.
	rsrcJson, m, err := canonicalizeMetadata(rsrcJson, types.CollectionKind, m)
	if err != nil {
		return nil, validationerrors.ErrSchemaSerialization
	}

	var cs collectionSchema
	if err := json.Unmarshal(rsrcJson, &cs); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal resource schema")
		return nil, validationerrors.ErrSchemaValidation
	}
	ves := cs.Validate()
	if ves != nil {
		return nil, validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}

	// validate the metadata
	if err := validateMetadata(ctx, m); err != nil {
		return nil, err
	}

	return &collectionManager{
		schema: cs,
	}, nil
}

func SaveCollection(ctx context.Context, cm schemamanager.CollectionManager, opts ...ObjectStoreOption) apperrors.Error {
	if cm == nil {
		return validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	rsrcPath := cm.Metadata().Path
	pathWithName := path.Clean(rsrcPath + "/" + cm.Metadata().Name)
	t := types.CatalogObjectTypeCatalogCollectionValue
	var dir Directories

	// get the directory
	if !options.Dir.IsNil() {
		dir = options.Dir
	} else if options.WorkspaceID != uuid.Nil {
		var err apperrors.Error
		dir, err = getDirectoriesForWorkspace(ctx, options.WorkspaceID)
		if err != nil {
			return err
		}
	} else {
		return ErrInvalidVersionOrWorkspace
	}
	// TODO: handle version number
	existingCollection, err := db.DB(ctx).LoadObjectByPath(ctx, t, dir.ValuesDir, pathWithName)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			existingCollection = nil
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get existing collection")
			return err
		}
	}

	var cmCurrent schemamanager.CollectionManager
	if existingCollection != nil {
		if options.ErrorIfExists {
			return ErrAlreadyExists.Msg("collection already exists")
		}
		m := cm.Metadata()
		cmCurrent, err = collectionManagerFromObject(ctx, existingCollection, &m)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to load existing collection")
			return err
		}
		// collection cannot be modified if schema is different
		if cmCurrent.Schema() != cm.Schema() {
			return ErrSchemaOfCollectionNotMutable
		}
	}

	schemaPath := "/" + path.Base(string(cm.Schema()))
	if collectionSchemaExists(ctx, dir.CollectionsDir, schemaPath) != nil {
		return ErrInvalidCollectionSchema
	}

	if err := LoadCollectionSchemaManager(ctx, cm, WithDirectories(dir)); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load collection schema manager")
		return err
	}

	schemaLoaders := getSchemaLoaders(ctx, cm.Metadata(), WithDirectories(dir))
	schemaLoaders.ParameterRef = func(name string) string {
		return "/" + path.Base(name)
	}

	var cmCurrentValues schemamanager.ParamValues = nil
	if cmCurrent != nil {
		cmCurrentValues = cmCurrent.Values()
	}

	if err := cm.ValidateValues(ctx, schemaLoaders, cmCurrentValues); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to set default values")
		return err
	}

	s := cm.StorageRepresentation()
	data, err := s.Serialize()
	if err != nil {
		return err
	}
	newHash := s.GetHash()
	if existingCollection != nil && newHash == existingCollection.Hash {
		if options.ErrorIfEqualToExisting {
			return ErrEqualToExistingObject
		}
		return nil
	}
	// store this object and update the reference
	obj := models.CatalogObject{
		Type:    t,
		Hash:    newHash,
		Version: s.Version,
		Data:    data,
	}
	dberr := db.DB(ctx).CreateCatalogObject(ctx, &obj)
	if dberr != nil {
		if errors.Is(dberr, dberror.ErrAlreadyExists) {
			log.Ctx(ctx).Debug().Str("hash", obj.Hash).Msg("catalog object already exists")
		} else {
			log.Ctx(ctx).Error().Err(dberr).Msg("failed to save catalog object")
			return dberr
		}
	}
	// the reference will point to the collection schema
	var refModel models.References
	refModel = append(refModel, models.Reference{
		Name: cm.Schema(),
	})

	if err := db.DB(ctx).AddOrUpdateObjectByPath(ctx, t, dir.DirForType(t), pathWithName, models.ObjectRef{
		Hash:       obj.Hash,
		References: refModel,
	}); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to save object to directory")
		return ErrCatalogError
	}

	return nil
}

func LoadCollectionByHash(ctx context.Context, hash string, m *schemamanager.SchemaMetadata) (schemamanager.CollectionManager, apperrors.Error) {
	if hash == "" {
		return nil, validationerrors.ErrEmptySchema
	}

	obj, err := db.DB(ctx).GetCatalogObject(ctx, hash)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrObjectNotFound.Err(err)
		}
		return nil, ErrUnableToLoadObject.Err(err)
	}

	return collectionManagerFromObject(ctx, obj, m)
}

func LoadCollectionByPath(ctx context.Context, m *schemamanager.SchemaMetadata, opts ...ObjectStoreOption) (schemamanager.CollectionManager, apperrors.Error) {
	if m == nil {
		return nil, validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	rsrcPath := m.Path
	pathWithName := path.Clean(rsrcPath + "/" + m.Name)
	t := types.CatalogObjectTypeCatalogCollectionValue
	var dir Directories

	// get the directory
	if !options.Dir.IsNil() {
		dir = options.Dir
	} else if options.WorkspaceID != uuid.Nil {
		var err apperrors.Error
		dir, err = getDirectoriesForWorkspace(ctx, options.WorkspaceID)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, ErrInvalidVersionOrWorkspace
	}

	obj, err := db.DB(ctx).LoadObjectByPath(ctx, t, dir.ValuesDir, pathWithName)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load object by path")
		return nil, err
	}

	return collectionManagerFromObject(ctx, obj, m)
}

func collectionManagerFromObject(ctx context.Context, obj *models.CatalogObject, m *schemamanager.SchemaMetadata) (schemamanager.CollectionManager, apperrors.Error) {
	if obj == nil {
		return nil, validationerrors.ErrEmptySchema
	}

	s := schemastore.SchemaStorageRepresentation{}
	if err := json.Unmarshal(obj.Data, &s); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal collection schema")
		return nil, validationerrors.ErrSchemaValidation
	}
	if s.Type != types.CatalogObjectTypeCatalogCollectionValue {
		log.Ctx(ctx).Error().Msg("invalid collection schema type")
		return nil, ErrUnableToLoadObject
	}
	if s.Type != types.CatalogObjectTypeCatalogCollectionValue {
		log.Ctx(ctx).Error().Msg("invalid collection schema kind")
		return nil, ErrUnableToLoadObject
	}

	cm := &collectionManager{}
	if err := json.Unmarshal(s.Schema, &cm.schema.Spec); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal collection schema spec")
		return nil, ErrUnableToLoadObject
	}
	if err := json.Unmarshal(s.Values, &cm.schema.Values); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal collection schema values")
		return nil, ErrUnableToLoadObject
	}
	cm.schema.Kind = types.CollectionKind
	cm.schema.Version = s.Version
	cm.schema.Metadata.Name = m.Name
	cm.schema.Metadata.Catalog = m.Catalog
	cm.schema.Metadata.Variant = m.Variant
	cm.schema.Metadata.Path = m.Path
	cm.schema.Metadata.Description = s.Description

	return cm, nil
}
