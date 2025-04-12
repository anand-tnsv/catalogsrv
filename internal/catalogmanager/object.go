package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	resource "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/v1/object"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api/schemastore"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

type VersionHeader struct {
	Version string `json:"version"`
}

func NewObject(ctx context.Context, rsrcJson []byte, m *schemamanager.ObjectMetadata) (schemamanager.ObjectManager, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}
	// get the version
	var version VersionHeader
	err := json.Unmarshal(rsrcJson, &version)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal version header")
		return nil, validationerrors.ErrSchemaValidation
	}
	if version.Version == "" {
		return nil, validationerrors.ErrSchemaValidation.Msg(schemaerr.ErrMissingRequiredAttribute("version").Error())
	}

	// validate the version
	if version.Version != "v1" {
		return nil, validationerrors.ErrInvalidVersion
	}

	// get the metadata, replace fields in json from provided metadata. Set defaults.
	rsrcJson, m, err = canonicalizeMetadata(rsrcJson, m)
	if err != nil {
		return nil, validationerrors.ErrSchemaSerialization
	}

	// validate the metadata
	if err := validateMetadata(ctx, m); err != nil {
		return nil, err
	}

	return resource.NewV1ObjectManager(ctx, []byte(rsrcJson), schemamanager.WithValidation(), schemamanager.WithDefaultValues())
}

type storeOptions struct {
	ErrorIfExists           bool
	WorkspaceID             uuid.UUID
	Dir                     Directories
	SkipValidationForUpdate bool
	VersionNum              int
}

type Directories struct {
	ParametersDir  uuid.UUID
	CollectionsDir uuid.UUID
}

func (d Directories) IsNil() bool {
	return d.ParametersDir == uuid.Nil && d.CollectionsDir == uuid.Nil
}

func (d Directories) DirForType(t types.CatalogObjectType) uuid.UUID {
	switch t {
	case types.CatalogObjectTypeParameterSchema:
		return d.ParametersDir
	case types.CatalogObjectTypeCollectionSchema:
		return d.CollectionsDir
	default:
		return uuid.Nil
	}
}

type ObjectStoreOption func(*storeOptions)

func WithErrorIfExists() ObjectStoreOption {
	return func(o *storeOptions) {
		o.ErrorIfExists = true
	}
}

func WithWorkspaceID(id uuid.UUID) ObjectStoreOption {
	return func(o *storeOptions) {
		o.WorkspaceID = id
	}
}

func WithDirectories(d Directories) ObjectStoreOption {
	return func(o *storeOptions) {
		o.Dir = d
	}
}

func WithVersionNum(num int) ObjectStoreOption {
	return func(o *storeOptions) {
		o.VersionNum = num
	}
}

func SkipValidationForUpdate() ObjectStoreOption {
	return func(o *storeOptions) {
		o.SkipValidationForUpdate = true
	}
}

func SaveObject(ctx context.Context, om schemamanager.ObjectManager, opts ...ObjectStoreOption) apperrors.Error {
	if om == nil {
		return validationerrors.ErrEmptySchema
	}
	s := om.StorageRepresentation()
	if s == nil {
		return validationerrors.ErrEmptySchema
	}

	m := om.Metadata()

	// get the options
	options := &storeOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var (
		t            types.CatalogObjectType = s.Type      // object type
		dir          Directories                           // directories for this object type
		hash         string                  = s.GetHash() // hash of the object's storage representation
		path         string                  = m.Path      // path to the object in the directory
		pathWithName string                  = ""          // fully qualified resource path with name
		refs         schemamanager.ObjectReferences
	)

	// strip path with any trailing slashes and append the name to get a FQRP
	path = strings.TrimRight(path, "/")
	pathWithName = path + "/" + m.Name

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

	switch t {
	case types.CatalogObjectTypeParameterSchema:
		if options.SkipValidationForUpdate {
			break
		}
		var err apperrors.Error
		if refs, err = validateParameterSchema(ctx, om, dir, hash); err != nil {
			if errors.Is(err, ErrAlreadyExists) {
				if options.ErrorIfExists {
					return ErrAlreadyExists
				}
				return nil
			}
			return err
		}
	case types.CatalogObjectTypeCollectionSchema:
		if options.SkipValidationForUpdate {
			break
		}
		var err apperrors.Error
		if refs, err = validateCollectionSchema(ctx, om, dir, hash); err != nil {
			if errors.Is(err, ErrAlreadyExists) {
				if options.ErrorIfExists {
					return ErrAlreadyExists
				}
				return nil
			}
			return err
		}
	default:
		return ErrCatalogError.Msg("invalid object type")
	}

	// if we came here, we have a new object to save
	data, err := s.Serialize()
	if err != nil {
		return validationerrors.ErrSchemaSerialization
	}

	obj := models.CatalogObject{
		Type:    s.Type,
		Version: s.Version,
		Data:    data,
		Hash:    hash,
	}

	// Save obj to the database
	dberr := db.DB(ctx).CreateCatalogObject(ctx, &obj)
	if dberr != nil {
		if errors.Is(dberr, dberror.ErrAlreadyExists) {
			log.Ctx(ctx).Debug().Str("hash", obj.Hash).Msg("catalog object already exists")
			// in this case, we don't return. If we came here it means the object is not in the directory,
			// so we'll keep chugging along and save the object to the directory
		} else {
			log.Ctx(ctx).Error().Err(dberr).Msg("failed to save catalog object")
			return dberr
		}
	}

	// For Collections, we obtain the existing references and process the delta later.
	// For Parameters, we copy over the existing references

	var existingRefs schemamanager.ObjectReferences
	if t == types.CatalogObjectTypeCollectionSchema && !options.SkipValidationForUpdate {
		existingRefs, _ = getObjectReferences(ctx, t, dir.CollectionsDir, pathWithName)
	}

	var refModel models.References
	for _, ref := range refs {
		refModel = append(refModel, models.Reference{
			Name: ref.Name,
			Hash: ref.Hash,
		})
	}

	if err := db.DB(ctx).AddOrUpdateObjectByPath(ctx, t, dir.DirForType(t), pathWithName, models.ObjectRef{
		Hash:       obj.Hash,
		References: refModel,
	}); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to save object to directory")
		return ErrCatalogError
	}

	if t == types.CatalogObjectTypeCollectionSchema && !options.SkipValidationForUpdate {
		syncCollectionReferencesInParameters(ctx, dir.ParametersDir, models.Reference{Name: pathWithName, Hash: obj.Hash}, existingRefs, refs)
	}

	return nil
}

var _ = syncParameterReferencesInCollection // suppress unused

func syncParameterReferencesInCollection(ctx context.Context, collectionDir uuid.UUID, collectionFqdp string, refs schemamanager.ObjectReferences) {
	var r models.References
	for _, ref := range refs {
		r = append(r, models.Reference{
			Name: ref.Name,
			Hash: ref.Hash,
		})
	}
	if err := db.DB(ctx).AddReferencesToObject(ctx, types.CatalogObjectTypeCollectionSchema, collectionDir, collectionFqdp, r); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to add references to collection schema")
	}
}

func syncCollectionReferencesInParameters(ctx context.Context, paramDir uuid.UUID, collectionRef models.Reference, existingRefs, newRefs schemamanager.ObjectReferences) {
	type refAction string
	const (
		actionAdd    refAction = "add"
		actionDelete refAction = "delete"
	)

	refActions := make(map[string]refAction)

	// Mark new references for addition
	for _, newRef := range newRefs {
		refActions[newRef.Name] = actionAdd
	}

	// Handle existing references (remove or keep)
	for _, existingRef := range existingRefs {
		if _, ok := refActions[existingRef.Name]; !ok {
			refActions[existingRef.Name] = actionDelete
		}
	}

	// Execute actions
	for param, action := range refActions {
		switch action {
		case actionAdd:
			if err := db.DB(ctx).AddReferencesToObject(ctx, types.CatalogObjectTypeParameterSchema, paramDir, param, []models.Reference{collectionRef}); err != nil {
				log.Ctx(ctx).Error().
					Str("param", param).
					Str("collection", collectionRef.Name).
					Err(err).
					Msg("failed to add references to collection schema")
			}
		case actionDelete:
			if err := db.DB(ctx).DeleteReferenceFromObject(ctx, types.CatalogObjectTypeParameterSchema, paramDir, param, collectionRef.Name); err != nil {
				log.Ctx(ctx).Error().
					Str("param", param).
					Str("collection", collectionRef.Name).
					Err(err).
					Msg("failed to delete references from collection schema")
			}
		}
	}
}

func validateParameterSchema(ctx context.Context, om schemamanager.ObjectManager, dir Directories, hash string) (schemamanager.ObjectReferences, apperrors.Error) {
	var refs schemamanager.ObjectReferences

	m := om.Metadata()
	pathWithName := m.Path + "/" + m.Name

	// get this objectRef from the directory
	r, err := db.DB(ctx).GetObjectByPath(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, pathWithName)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Debug().Str("path", pathWithName).Msg("object not found")
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get object by path")
			return refs, ErrCatalogError
		}
	}

	if r != nil {
		if r.Hash == hash {
			// if the hash is the same, we don't need to save the object
			// since there is no modification, we don't need to re-evaluate parameters
			log.Ctx(ctx).Debug().Str("hash", hash).Msg("object already exists")
			return refs, ErrAlreadyExists
		}
		if len(r.References) > 0 {
			for _, ref := range r.References {
				refs = append(refs, schemamanager.ObjectReference{
					Name: ref.Name,
					Hash: ref.Hash,
				})
			}
			loaders := getObjectLoaders(ctx, om.Metadata(), WithDirectories(dir))
			if pm := om.ParameterManager(); pm != nil {
				if err := pm.ValidateDependencies(ctx, loaders, refs); err != nil {
					return refs, err
				}
			}
		}
	}

	return refs, nil
}

// validateCollectionSchema ensures that all the dataTypes referenced by parameters in the Spec are valid.
// Similarly, it ensures that all the parameters referenced by the collection schema exist and also returns the
// references to the parameter schemas.
func validateCollectionSchema(ctx context.Context, om schemamanager.ObjectManager, dir Directories, hash string) (schemamanager.ObjectReferences, apperrors.Error) {
	var refs schemamanager.ObjectReferences
	if om == nil {
		log.Ctx(ctx).Error().Msg("object manager is nil")
		return nil, ErrCatalogError
	}

	cm := om.CollectionManager()
	if cm == nil {
		log.Ctx(ctx).Error().Msg("collection manager is nil")
		return nil, ErrCatalogError
	}

	m := om.Metadata()
	parentPath := m.Path
	pathWithName := parentPath + "/" + m.Name

	loaders := getObjectLoaders(ctx, m, WithDirectories(dir))

	// validate the collection schema
	var err apperrors.Error
	if refs, err = cm.ValidateDependencies(ctx, loaders); err != nil {
		return nil, err
	}

	// get this objectRef from the directory
	r, err := db.DB(ctx).GetObjectByPath(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, pathWithName)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Debug().Str("path", pathWithName).Msg("object not found")
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get object by path")
			return nil, ErrCatalogError
		}
	}
	if r != nil && r.Hash == hash {
		log.Ctx(ctx).Debug().Str("hash", hash).Msg("object already exists")
		return refs, ErrAlreadyExists
	}

	return refs, nil
}

func LoadObjectByPath(ctx context.Context, t types.CatalogObjectType, m *schemamanager.ObjectMetadata, opts ...ObjectStoreOption) (schemamanager.ObjectManager, apperrors.Error) {
	o := &storeOptions{}
	for _, opt := range opts {
		opt(o)
	}

	var dir uuid.UUID
	if !o.Dir.IsNil() && o.Dir.DirForType(t) != uuid.Nil {
		dir = o.Dir.DirForType(t)
	} else if o.WorkspaceID != uuid.Nil {
		var wm schemamanager.WorkspaceManager
		var apperr apperrors.Error

		if wm, apperr = LoadWorkspaceManagerByID(ctx, o.WorkspaceID); apperr != nil {
			return nil, apperr
		}

		switch t {
		case types.CatalogObjectTypeParameterSchema:
			if dir = wm.ParametersDir(); dir == uuid.Nil {
				return nil, ErrInvalidWorkspace.Msg("workspace does not have a parameters directory")
			}
		case types.CatalogObjectTypeCollectionSchema:
			if dir = wm.CollectionsDir(); dir == uuid.Nil {
				return nil, ErrInvalidWorkspace.Msg("workspace does not have a collections directory")
			}
		default:
			return nil, ErrCatalogError.Msg("invalid object type")
		}
	} else {
		return nil, ErrInvalidVersionOrWorkspace
	}

	fqrp := m.Path + "/" + m.Name
	obj, err := db.DB(ctx).GetObjectByPath(ctx, t, dir, fqrp)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrObjectNotFound
		}
		return nil, ErrCatalogError.Err(err)
	}
	if obj == nil { //should never get here
		return nil, ErrObjectNotFound
	}
	return LoadObjectByHash(ctx, obj.Hash, m)
}

func LoadObjectByHash(ctx context.Context, hash string, m *schemamanager.ObjectMetadata) (schemamanager.ObjectManager, apperrors.Error) {
	if hash == "" {
		return nil, dberror.ErrInvalidInput.Msg("hash cannot be empty")
	}

	obj, err := db.DB(ctx).GetCatalogObject(ctx, hash)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrObjectNotFound.Err(err)
		}
		return nil, ErrUnableToLoadObject.Err(err)
	}

	s := &schemastore.SchemaStorageRepresentation{}
	// we'll get the data from the object and not the table
	if err := json.Unmarshal(obj.Data, s); err != nil {
		return nil, ErrUnableToLoadObject.Err(err).Msg("failed to de-serialize catalog object data")
	}

	if s.Type != obj.Type {
		log.Ctx(ctx).Error().Str("Hash", hash).Msg("type mismatch when loading resource")
	}

	if s.Version != obj.Version {
		log.Ctx(ctx).Error().Str("Hash", hash).Msg("version mismatch when loading resource")
	}

	return resource.LoadV1ObjectManager(ctx, s, m)
}

func validateMetadata(ctx context.Context, m *schemamanager.ObjectMetadata) apperrors.Error {
	if m == nil {
		return ErrEmptyMetadata
	}
	ves := m.Validate()
	if ves != nil {
		return validationerrors.ErrSchemaValidation.Msg(ves.Error())
	}
	// Check if the catalog exists
	c, err := db.DB(ctx).GetCatalog(ctx, uuid.Nil, m.Catalog)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrInvalidCatalog.Err(err)
		}
		return ErrCatalogError.Err(err)
	}
	// check if the variant exists
	if !m.Variant.IsNil() {
		if _, err := db.DB(ctx).GetVariant(ctx, c.CatalogID, uuid.Nil, m.Variant.String()); err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return ErrVariantNotFound.Err(err)
			}
			return ErrCatalogError.Err(err)
		}
	}
	// we won't handle resource path here
	return nil
}

func getClosestParentObjectFinder(ctx context.Context, m schemamanager.ObjectMetadata, opts ...ObjectStoreOption) schemamanager.ClosestParentObjectFinder {
	o := &storeOptions{}
	for _, opt := range opts {
		opt(o)
	}

	var paramDir, collectionDir uuid.UUID
	if !o.Dir.IsNil() {

		paramDir = o.Dir.DirForType(types.CatalogObjectTypeParameterSchema)
		collectionDir = o.Dir.DirForType(types.CatalogObjectTypeCollectionSchema)

	} else if o.WorkspaceID != uuid.Nil {
		var dir Directories
		var apperr apperrors.Error
		dir, apperr = getDirectoriesForWorkspace(ctx, o.WorkspaceID)
		if apperr != nil {
			return nil
		}
		paramDir = dir.DirForType(types.CatalogObjectTypeParameterSchema)
		collectionDir = dir.DirForType(types.CatalogObjectTypeCollectionSchema)
		if paramDir == uuid.Nil || collectionDir == uuid.Nil {
			return nil
		}
	} else {
		return nil
	}

	startPath := strings.TrimRight(m.Path, "/") // remove trailing slash if exists

	return func(ctx context.Context, t types.CatalogObjectType, targetName string) (path string, hash string, err apperrors.Error) {
		var dir uuid.UUID
		switch t {
		case types.CatalogObjectTypeParameterSchema:
			dir = paramDir
		case types.CatalogObjectTypeCollectionSchema:
			dir = collectionDir
		default:
			return "", "", ErrCatalogError.Msg("invalid object type")
		}

		path, obj, err := db.DB(ctx).FindClosestObject(ctx, t, dir, targetName, startPath)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return "", "", ErrObjectNotFound
			}
			return "", "", ErrCatalogError.Err(err)
		}
		if obj == nil {
			return "", "", ErrObjectNotFound
		}
		hash = obj.Hash
		return
	}
}

func getObjectLoaderByPath(ctx context.Context, m schemamanager.ObjectMetadata, opts ...ObjectStoreOption) schemamanager.ObjectLoaderByPath {
	o := &storeOptions{}
	for _, opt := range opts {
		opt(o)
	}

	var paramDir, collectionDir uuid.UUID
	if !o.Dir.IsNil() {

		paramDir = o.Dir.DirForType(types.CatalogObjectTypeParameterSchema)
		collectionDir = o.Dir.DirForType(types.CatalogObjectTypeCollectionSchema)

	} else if o.WorkspaceID != uuid.Nil {

		var wm schemamanager.WorkspaceManager
		var apperr apperrors.Error

		if wm, apperr = LoadWorkspaceManagerByID(ctx, o.WorkspaceID); apperr != nil {
			return nil
		}

		if paramDir = wm.ParametersDir(); paramDir == uuid.Nil {
			return nil
		}
		if collectionDir = wm.CollectionsDir(); collectionDir == uuid.Nil {
			return nil
		}
	} else {
		return nil
	}

	// We do this so load workspace never gets called again
	opts = append(opts, WithDirectories(Directories{
		ParametersDir:  paramDir,
		CollectionsDir: collectionDir,
	}))

	return func(ctx context.Context, t types.CatalogObjectType, path string) (schemamanager.ObjectManager, apperrors.Error) {
		return LoadObjectByPath(ctx, t, &m, opts...)
	}
}

func getObjectLoaderByHash(m schemamanager.ObjectMetadata) schemamanager.ObjectLoaderByHash {
	return func(ctx context.Context, t types.CatalogObjectType, hash string, mOverride ...schemamanager.ObjectMetadata) (schemamanager.ObjectManager, apperrors.Error) {
		if len(mOverride) > 0 {
			if mOverride[0].Name != "" {
				m.Name = mOverride[0].Name
			}
			if mOverride[0].Path != "" {
				m.Path = mOverride[0].Path
			}
			if mOverride[0].Catalog != "" {
				m.Catalog = mOverride[0].Catalog
			}
			if !mOverride[0].Variant.IsNil() {
				m.Variant = mOverride[0].Variant
			}
		}
		return LoadObjectByHash(ctx, hash, &m)
	}
}

func getObjectLoaders(ctx context.Context, m schemamanager.ObjectMetadata, opts ...ObjectStoreOption) schemamanager.ObjectLoaders {
	return schemamanager.ObjectLoaders{
		ByPath:        getObjectLoaderByPath(ctx, m, opts...),
		ByHash:        getObjectLoaderByHash(m),
		ClosestParent: getClosestParentObjectFinder(ctx, m, opts...),
		SelfMetadata: func() schemamanager.ObjectMetadata {
			return m
		},
	}
}

/*
	func getClosestParentObjectFromReferences(ctx context.Context, opts ...ObjectStoreOption) schemamanager.ClosestParentObjectFinder {
		o := &storeOptions{}
		for _, opt := range opts {
			opt(o)
		}

		var paramDir, collectionDir uuid.UUID
		if !o.Dir.IsNil() {
			paramDir = o.Dir.DirForType(types.CatalogObjectTypeParameterSchema)
			collectionDir = o.Dir.DirForType(types.CatalogObjectTypeCollectionSchema)
		} else if o.WorkspaceID != uuid.Nil {
			var dir Directories
			var apperr apperrors.Error
			dir, apperr = getDirectoriesForWorkspace(ctx, o.WorkspaceID)
			if apperr != nil {
				return nil
			}
			paramDir = dir.ParametersDir
			collectionDir = dir.CollectionsDir
		} else {
			return nil
		}

		if paramDir == uuid.Nil || collectionDir == uuid.Nil {
			return nil
		}

		return func(ctx context.Context, t types.CatalogObjectType, targetName string) (path string, hash string, err apperrors.Error) {
			var dir uuid.UUID
			switch t {
			case types.CatalogObjectTypeParameterSchema:
				dir = paramDir
			case types.CatalogObjectTypeCollectionSchema:
				dir = collectionDir
			default:
				return "", "", ErrCatalogError.Msg("invalid object type")
			}

			path, obj, err := db.DB(ctx).FindClosestObjectFromReferences(ctx, t, dir, targetName)
			if err != nil {
				if errors.Is(err, dberror.ErrNotFound) {
					return "", "", ErrObjectNotFound
				}
				return "", "", ErrCatalogError.Err(err)
			}
			if obj == nil {
				return "", "", ErrObjectNotFound
			}
			hash = obj.Hash
			return
		}
	}
*/
func getDirectoriesForWorkspace(ctx context.Context, workspaceId uuid.UUID) (Directories, apperrors.Error) {
	var wm schemamanager.WorkspaceManager
	var apperr apperrors.Error
	var dir Directories

	if wm, apperr = LoadWorkspaceManagerByID(ctx, workspaceId); apperr != nil {
		return dir, apperr
	}

	if dir.ParametersDir = wm.ParametersDir(); dir.ParametersDir == uuid.Nil {
		return dir, ErrInvalidWorkspace.Msg("workspace does not have a parameters directory")
	}

	if dir.CollectionsDir = wm.CollectionsDir(); dir.CollectionsDir == uuid.Nil {
		return dir, ErrInvalidWorkspace.Msg("workspace does not have a collections directory")
	}

	return dir, nil
}

func getObjectReferences(ctx context.Context, t types.CatalogObjectType, dir uuid.UUID, path string) (schemamanager.ObjectReferences, apperrors.Error) {
	var refs schemamanager.ObjectReferences
	r, err := db.DB(ctx).GetAllReferences(ctx, t, dir, path)
	if err != nil {
		return nil, ErrCatalogError.Err(err)
	}
	for _, ref := range r {
		refs = append(refs, schemamanager.ObjectReference{
			Name: ref.Name,
			Hash: ref.Hash,
		})
	}
	return refs, nil
}
