package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"path"

	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

func UpdateCollectionValue(ctx context.Context, m *schemamanager.SchemaMetadata, param string, value types.NullableAny, opts ...ObjectStoreOption) apperrors.Error {
	if m == nil || param == "" {
		return validationerrors.ErrEmptySchema
	}

	options := storeOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	var dir Directories
	pathWithName := path.Clean(m.Path + "/" + m.Name)
	t := types.CatalogObjectTypeCatalogCollection

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

	existingCollection, err := db.DB(ctx).LoadObjectByPath(ctx, t, dir.ValuesDir, pathWithName)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			existingCollection = nil
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get existing collection")
			return err
		}
	}
	var cm schemamanager.CollectionManager
	if existingCollection != nil {
		if options.ErrorIfExists {
			return ErrAlreadyExists.Msg("collection already exists")
		}
		cm, err = collectionManagerFromObject(ctx, existingCollection, m)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to load existing collection")
			return err
		}
	}

	v, _ := cm.GetValue(ctx, param)
	if v.Equals(value) {
		log.Ctx(ctx).Info().Msg("value is the same, no update needed")
		return nil
	}

	if err := loadCollectionSchemaManager(ctx, cm, WithDirectories(dir)); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to load collection schema manager")
		return err
	}
	schemaLoaders := getSchemaLoaders(ctx, *m, WithDirectories(dir))
	schemaLoaders.ParameterRef = func(name string) string {
		return "/" + path.Base(name)
	}
	err = cm.SetValue(ctx, schemaLoaders, param, value)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to set value in collection manager")
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
		Type:    types.CatalogObjectTypeCatalogCollection,
		Hash:    newHash,
		Version: s.Version,
		Data:    data,
	}
	return saveCollectionObject(ctx, &obj, dir, path.Clean(m.Path+"/"+m.Name), cm.Schema())
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
	t := types.CatalogObjectTypeCatalogCollection
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

	if err := loadCollectionSchemaManager(ctx, cm, WithDirectories(dir)); err != nil {
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

	return saveCollectionObject(ctx, &obj, dir, pathWithName, cm.Schema())
}

func saveCollectionObject(ctx context.Context, obj *models.CatalogObject, dir Directories, pathWithName, collectionSchema string) apperrors.Error {
	dberr := db.DB(ctx).CreateCatalogObject(ctx, obj)
	if dberr != nil {
		if errors.Is(dberr, dberror.ErrAlreadyExists) {
			log.Ctx(ctx).Debug().Str("hash", obj.Hash).Msg("catalog object already exists")
			return nil // already exists, nothing to do
		}
		log.Ctx(ctx).Error().Err(dberr).Msg("failed to save catalog object")
		return dberr
	}

	// the reference will point to the collection schema
	var refModel models.References
	refModel = append(refModel, models.Reference{
		Name: collectionSchema,
	})

	// store the reference in the directory
	if err := db.DB(ctx).AddOrUpdateObjectByPath(ctx, types.CatalogObjectTypeCatalogCollection, dir.ValuesDir, pathWithName, models.ObjectRef{
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
	t := types.CatalogObjectTypeCatalogCollection
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

func loadCollectionSchemaManager(ctx context.Context, cm schemamanager.CollectionManager, opts ...ObjectStoreOption) apperrors.Error {
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

func collectionManagerFromObject(ctx context.Context, obj *models.CatalogObject, m *schemamanager.SchemaMetadata) (schemamanager.CollectionManager, apperrors.Error) {
	if obj == nil {
		return nil, validationerrors.ErrEmptySchema
	}

	s := schemastore.SchemaStorageRepresentation{}
	if err := json.Unmarshal(obj.Data, &s); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal collection schema")
		return nil, validationerrors.ErrSchemaValidation
	}
	if s.Type != types.CatalogObjectTypeCatalogCollection {
		log.Ctx(ctx).Error().Msg("invalid collection schema type")
		return nil, ErrUnableToLoadObject
	}
	if s.Type != types.CatalogObjectTypeCatalogCollection {
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
