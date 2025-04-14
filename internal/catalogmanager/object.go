package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
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
	SetDefaultValues        bool
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

func SetDefaultValues() ObjectStoreOption {
	return func(o *storeOptions) {
		o.SetDefaultValues = true
	}
}

func SaveObject(ctx context.Context, om schemamanager.ObjectManager, opts ...ObjectStoreOption) apperrors.Error {
	if om == nil {
		return validationerrors.ErrEmptySchema
	}

	m := om.Metadata()

	// get the options
	options := &storeOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var (
		t                  types.CatalogObjectType = om.Type() // object type
		dir                Directories                         // directories for this object type
		hash               string                              // hash of the object's storage representation
		path               string                  = m.Path    // path to the object in the directory
		pathWithName       string                  = ""        // fully qualified resource path with name
		refs, existingRefs schemamanager.ObjectReferences
		existingParamPath  string
		existingParamRef   *models.ObjectRef
		existingObjHash    string // Hash of the existing object with same path in the directory
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
		if existingObjHash, refs, existingParamPath, existingParamRef, err = validateParameterSchema(ctx, om, dir); err != nil {
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
		if existingObjHash, refs, existingRefs, err = validateCollectionSchema(ctx, om, dir); err != nil {
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

	if om.Type() == types.CatalogObjectTypeCollectionSchema {
		om.CollectionManager().SetDefaultValues(ctx)
	}

	s := om.StorageRepresentation()
	if s == nil {
		return validationerrors.ErrEmptySchema
	}

	hash = s.GetHash()
	if hash == existingObjHash {
		if options.ErrorIfExists {
			return ErrAlreadyExists
		}
		return nil
	}

	_ = existingParamRef
	_ = existingParamPath
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

	var refModel models.References
	for _, ref := range refs {
		refModel = append(refModel, models.Reference{
			Name: ref.Name,
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
		syncCollectionReferencesInParameters(ctx, dir.ParametersDir, pathWithName, existingRefs, refs)
	} else if t == types.CatalogObjectTypeParameterSchema && len(refs) > 0 {
		syncParameterReferencesInCollections(ctx, dir, existingParamPath, pathWithName, existingParamRef, refs)
	}

	return nil
}

func syncParameterReferencesInCollections(ctx context.Context, dir Directories, existingPath, newPath string, existingParamObjRef *models.ObjectRef, newCollectionRefs schemamanager.ObjectReferences) {
	var newRefsForExistingParam models.References
	if existingParamObjRef != nil {
		for _, ref := range existingParamObjRef.References {
			remove := false
			for _, newRef := range newCollectionRefs {
				if ref.Name == newRef.Name {
					remove = true
				}
			}
			if !remove {
				newRefsForExistingParam = append(newRefsForExistingParam, ref)
			}
		}
	}
	if len(newRefsForExistingParam) > 0 {
		existingParamObjRef.References = newRefsForExistingParam
		// save the updated references for the parameter
		if err := db.DB(ctx).AddOrUpdateObjectByPath(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, existingPath, *existingParamObjRef); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to update parameter references")
		}
	}

	// if there are no existing references, we don't need to do anything
	if existingParamObjRef == nil {
		return
	}

	// for all the collections that will now map to the new parameter, replace the old reference with the new one
	for _, newRef := range newCollectionRefs {
		if err := db.DB(ctx).AddReferencesToObject(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, newRef.Name, []models.Reference{{Name: newPath}}); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to add new param path to collection")
		}
		if err := db.DB(ctx).DeleteReferenceFromObject(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, newRef.Name, existingPath); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to delete new param path from collection")
		}
	}
}

func syncCollectionReferencesInParameters(ctx context.Context, paramDir uuid.UUID, collectionFqp string, existingParamRefs, newParamRefs schemamanager.ObjectReferences) {
	type refAction string
	const (
		actionAdd    refAction = "add"
		actionDelete refAction = "delete"
	)

	refActions := make(map[string]refAction)

	// Mark new references for addition
	for _, newRef := range newParamRefs {
		refActions[newRef.Name] = actionAdd
	}

	// Handle existing references (remove or keep)
	for _, existingRef := range existingParamRefs {
		if _, ok := refActions[existingRef.Name]; !ok {
			refActions[existingRef.Name] = actionDelete
		} else {
			delete(refActions, existingRef.Name)
		}
	}

	// Execute actions
	for param, action := range refActions {
		switch action {
		case actionAdd:
			if err := db.DB(ctx).AddReferencesToObject(ctx, types.CatalogObjectTypeParameterSchema, paramDir, param, []models.Reference{{Name: collectionFqp}}); err != nil {
				log.Ctx(ctx).Error().
					Str("param", param).
					Str("collection", collectionFqp).
					Err(err).
					Msg("failed to add references to collection schema")
			}
		case actionDelete:
			if err := db.DB(ctx).DeleteReferenceFromObject(ctx, types.CatalogObjectTypeParameterSchema, paramDir, param, collectionFqp); err != nil {
				log.Ctx(ctx).Error().
					Str("param", param).
					Str("collection", collectionFqp).
					Err(err).
					Msg("failed to delete references from collection schema")
			}
		}
	}
}

func validateParameterSchema(ctx context.Context, om schemamanager.ObjectManager, dir Directories) (
	existingObjHash string,
	newRefs schemamanager.ObjectReferences,
	existingPath string,
	existingParamRef *models.ObjectRef,
	err apperrors.Error) {

	m := om.Metadata()
	pathWithName := m.Path + "/" + m.Name

	// get this objectRef from the directory
	r, err := db.DB(ctx).GetObjectRefByPath(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, pathWithName)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Debug().Str("path", pathWithName).Msg("object not found")
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get object by path")
			err = ErrCatalogError
			return
		}
	}

	if r != nil {
		if len(r.References) > 0 {
			for _, ref := range r.References {
				newRefs = append(newRefs, schemamanager.ObjectReference{
					Name: ref.Name,
				})
			}
		}
		existingObjHash = r.Hash
	} else {
		// This is a new object. Check if the parent collection exists
		if err = collectionExists(ctx, dir.CollectionsDir, m.Path); err != nil {
			return
		}

		// If there are existing parameters with the same name and there are collections referencing those,
		// we will need to remap them.
		existingPath, existingParamRef, err = db.DB(ctx).FindClosestObject(ctx, types.CatalogObjectTypeParameterSchema, dir.ParametersDir, m.Name, m.Path)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Str("path", existingPath).Msg("failed to find closest object")
			err = ErrCatalogError
			return
		}

		if existingPath != "" && existingParamRef != nil {
			collectionRefs := existingParamRef.References
			for _, ref := range collectionRefs {
				if isParentOrSame(m.Path, path.Dir(ref.Name)) {
					newRefs = append(newRefs, schemamanager.ObjectReference{
						Name: ref.Name,
					})
				}
			}
		}
	}
	// if there are reference to this object - either new or updated - we need to validate them
	if len(newRefs) > 0 {
		loaders := getObjectLoaders(ctx, om.Metadata(), WithDirectories(dir))
		if pm := om.ParameterManager(); pm != nil {
			if err = pm.ValidateDependencies(ctx, loaders, newRefs); err != nil {
				return
			}
		}
	}

	return
}

// isParentOrSame checks if p1 is a parent or the same as p2
func isParentOrSame(p1, p2 string) bool {
	// Clean paths to remove redundant elements
	p1 = path.Clean(p1)
	p2 = path.Clean(p2)

	// Check if p1 is a prefix of p2
	return p2 == p1 || strings.HasPrefix(p2, p1+"/")
}

// validateCollectionSchema ensures that all the dataTypes referenced by parameters in the Spec are valid.
// Similarly, it ensures that all the parameters referenced by the collection schema exist and also returns the
// references to the parameter schemas.
func validateCollectionSchema(ctx context.Context, om schemamanager.ObjectManager, dir Directories) (
	existingObjHash string,
	newRefs schemamanager.ObjectReferences,
	existingRefs schemamanager.ObjectReferences,
	err apperrors.Error) {
	if om == nil {
		log.Ctx(ctx).Error().Msg("object manager is nil")
		err = ErrCatalogError
		return
	}

	cm := om.CollectionManager()
	if cm == nil {
		log.Ctx(ctx).Error().Msg("collection manager is nil")
		err = ErrCatalogError
		return
	}

	m := om.Metadata()
	parentPath := m.Path
	pathWithName := parentPath + "/" + m.Name

	// get this objectRef from the directory
	r, err := db.DB(ctx).GetObjectRefByPath(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, pathWithName)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Debug().Str("path", pathWithName).Msg("object not found")
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get object by path")
			err = ErrCatalogError
			return
		}
	}
	if r != nil {
		if len(r.References) > 0 {
			for _, ref := range r.References {
				existingRefs = append(existingRefs, schemamanager.ObjectReference{
					Name: ref.Name,
				})
			}
		}
		existingObjHash = r.Hash
	} else {
		// This is a new object. Check if the parent collection exists
		if err = collectionExists(ctx, dir.CollectionsDir, m.Path); err != nil {
			return
		}
	}
	// validate the collection schema
	loaders := getObjectLoaders(ctx, m, WithDirectories(dir))

	// refs are updated after validation
	if newRefs, err = cm.ValidateDependencies(ctx, loaders, existingRefs); err != nil {
		return
	}

	return
}

func deleteCollectionSchema(ctx context.Context, om schemamanager.ObjectManager, dir Directories) apperrors.Error {
	m := om.Metadata()
	pathWithName := m.Path + "/" + m.Name

	// get this objectRef from the directory
	r, err := db.DB(ctx).GetObjectRefByPath(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, pathWithName)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			log.Ctx(ctx).Debug().Str("path", pathWithName).Msg("object not found")
		} else {
			log.Ctx(ctx).Error().Err(err).Msg("failed to get object by path")
			return ErrCatalogError
		}
	}

	existingRefs := make(schemamanager.ObjectReferences, 0)
	existingObjHash := ""

	if r != nil {
		if len(r.References) > 0 {
			for _, ref := range r.References {
				existingRefs = append(existingRefs, schemamanager.ObjectReference{
					Name: ref.Name,
				})
			}
		}
		existingObjHash = r.Hash
	} else {
		return ErrObjectNotFound
	}

	if err := db.DB(ctx).DeleteObjectByPath(ctx, types.CatalogObjectTypeCollectionSchema, dir.CollectionsDir, pathWithName); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete object from directory")
		return ErrCatalogError
	}

	return nil
}

func collectionExists(ctx context.Context, collectionsDir uuid.UUID, path string) apperrors.Error {
	if path != "/" {
		var (
			exists bool
			err    apperrors.Error
		)
		if exists, err = db.DB(ctx).PathExists(ctx, types.CatalogObjectTypeCollectionSchema, collectionsDir, path); err != nil {
			log.Ctx(ctx).Error().Err(err).Str("path", path).Msg("failed to check if parent path exists")
			return ErrCatalogError
		}
		if !exists {
			return ErrParentCollectionNotFound.Msg(path + " does not exist")
		}
	}
	return nil
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
		dirs, err := getDirectoriesForWorkspace(ctx, o.WorkspaceID)
		if err != nil {
			return nil, err
		}
		dir = dirs.DirForType(t)
	} else {
		return nil, ErrInvalidVersionOrWorkspace
	}

	rsrcPath := m.Path + "/" + m.Name
	rsrcPath = path.Clean(rsrcPath)

	obj, err := db.DB(ctx).LoadObjectByPath(ctx, t, dir, rsrcPath)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrObjectNotFound
		}
		return nil, ErrCatalogError.Err(err)
	}
	if obj == nil { //should never get here
		return nil, ErrObjectNotFound
	}

	s := &schemastore.SchemaStorageRepresentation{}
	// we'll get the data from the object and not the table
	if err := json.Unmarshal(obj.Data, s); err != nil {
		return nil, ErrUnableToLoadObject.Err(err).Msg("failed to de-serialize catalog object data")
	}
	if s.Type != obj.Type {
		log.Ctx(ctx).Error().Str("Hash", obj.Hash).Msg("type mismatch when loading resource")
	}
	if s.Version != obj.Version {
		log.Ctx(ctx).Error().Str("Hash", obj.Hash).Msg("version mismatch when loading resource")
	}

	return resource.LoadV1ObjectManager(ctx, s, m)
}

func DeleteObject(ctx context.Context, om schemamanager.ObjectManager) apperrors.Error {
	if om == nil {
		return validationerrors.ErrEmptySchema
	}
	//m := om.Metadata()
	return nil
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

	var dir Directories
	if !o.Dir.IsNil() {
		dir = o.Dir
	} else if o.WorkspaceID != uuid.Nil {
		var apperr apperrors.Error
		dir, apperr = getDirectoriesForWorkspace(ctx, o.WorkspaceID)
		if apperr != nil {
			return nil
		}
		if o.Dir.IsNil() {
			return nil
		}
	} else {
		return nil
	}

	startPath := strings.TrimRight(m.Path, "/") // remove trailing slash if exists

	return func(ctx context.Context, t types.CatalogObjectType, targetName string) (path string, hash string, err apperrors.Error) {
		path, obj, err := db.DB(ctx).FindClosestObject(ctx, t, dir.DirForType(t), targetName, startPath)
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

func getObjectLoaderByPath(ctx context.Context, opts ...ObjectStoreOption) schemamanager.ObjectLoaderByPath {
	o := &storeOptions{}
	for _, opt := range opts {
		opt(o)
	}
	var dir Directories

	if !o.Dir.IsNil() {
		dir = o.Dir
	} else if o.WorkspaceID != uuid.Nil {
		var err apperrors.Error
		dir, err = getDirectoriesForWorkspace(ctx, o.WorkspaceID)
		if err != nil {
			return nil
		}
	} else {
		return nil
	}

	// We do this so load workspace never gets called again
	opts = append(opts, WithDirectories(dir))

	return func(ctx context.Context, t types.CatalogObjectType, m *schemamanager.ObjectMetadata) (schemamanager.ObjectManager, apperrors.Error) {
		return LoadObjectByPath(ctx, t, m, opts...)
	}
}

func getObjectLoaderByHash() schemamanager.ObjectLoaderByHash {
	return func(ctx context.Context, t types.CatalogObjectType, hash string, m *schemamanager.ObjectMetadata) (schemamanager.ObjectManager, apperrors.Error) {
		return LoadObjectByHash(ctx, hash, m)
	}
}

func getObjectLoaders(ctx context.Context, m schemamanager.ObjectMetadata, opts ...ObjectStoreOption) schemamanager.ObjectLoaders {
	return schemamanager.ObjectLoaders{
		ByPath:        getObjectLoaderByPath(ctx, opts...),
		ByHash:        getObjectLoaderByHash(),
		ClosestParent: getClosestParentObjectFinder(ctx, m, opts...),
		SelfMetadata: func() schemamanager.ObjectMetadata {
			return m
		},
	}
}

func getParameterRefForName(refs schemamanager.ObjectReferences) schemamanager.ParameterReferenceForName {
	return func(name string) string {
		for _, ref := range refs {
			if ref.ObjectName() == name {
				return ref.Name
			}
		}
		return ""
	}
}

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
		})
	}
	return refs, nil
}

type objectResource struct {
	name        ResourceName
	catalogID   uuid.UUID
	variantID   uuid.UUID
	workspaceID uuid.UUID
	version     int
	rsrcJson    []byte
	om          schemamanager.ObjectManager
}

func (or *objectResource) Name() string {
	return or.name.ObjectName
}

func (or *objectResource) Location() string {
	var versonOrWorkspace string
	if or.workspaceID == uuid.Nil {
		versonOrWorkspace = "/versions/" + strconv.Itoa(or.version)
	} else {
		versonOrWorkspace = "/workspaces/" + or.workspaceID.String()
	}
	var objType string
	if or.name.ObjectType == types.CatalogObjectTypeCollectionSchema {
		objType = "collection"
	} else {
		objType = "parameter"
	}
	return path.Clean(or.name.Catalog + "/variants/" + or.name.Variant + versonOrWorkspace + "/" + objType + "/" + or.name.ObjectPath + "/" + or.Name())
}

func (or *objectResource) ResourceJson() []byte {
	return or.rsrcJson
}

func (or *objectResource) Manager() schemamanager.ObjectManager {
	return or.om
}

func (or *objectResource) Create(ctx context.Context) (string, apperrors.Error) {
	m := &schemamanager.ObjectMetadata{
		Catalog: or.name.Catalog,
		Variant: types.NullableStringFrom(or.name.Variant),
	}
	// We need a valid workspace to save the object
	if or.workspaceID == uuid.Nil {
		return "", ErrInvalidWorkspace
	}
	object, err := NewObject(ctx, or.rsrcJson, m)
	if err != nil {
		return "", err
	}
	err = SaveObject(ctx, object, WithWorkspaceID(or.workspaceID))
	if err != nil {
		return "", err
	}

	or.name.ObjectName = object.Metadata().Name
	or.name.ObjectPath = object.Metadata().Path
	or.name.ObjectType = object.Type()
	or.om = object
	if or.name.Catalog == "" {
		or.name.Catalog = object.Metadata().Catalog
	}
	if or.name.Variant == "" {
		or.name.Variant = object.Metadata().Variant.String()
	}

	return or.Location(), nil
}

func (or *objectResource) Get(ctx context.Context) ([]byte, apperrors.Error) {
	if or.workspaceID == uuid.Nil {
		return nil, ErrInvalidWorkspace
	}
	object, err := LoadObjectByPath(ctx, or.name.ObjectType, &schemamanager.ObjectMetadata{
		Catalog: or.name.Catalog,
		Variant: types.NullableStringFrom(or.name.Variant),
		Path:    or.name.ObjectPath,
		Name:    or.name.ObjectName,
	}, WithWorkspaceID(or.workspaceID))
	if err != nil {
		return nil, err
	}
	return object.ToJson(ctx)
}

func (or *objectResource) Update(ctx context.Context, rsrcJson []byte) apperrors.Error {
	return nil
}

func (or *objectResource) Delete(ctx context.Context) apperrors.Error {
	return nil
}

func NewObjectResource(ctx context.Context, rsrcJson []byte, name ResourceName) (schemamanager.ResourceManager, apperrors.Error) {
	catalogID, variantID := uuid.Nil, uuid.Nil
	if len(rsrcJson) == 0 || (len(name.Catalog) > 0 && len(name.Variant) > 0) {
		if len(name.Catalog) > 0 && schemavalidator.ValidateObjectName(name.Catalog) {
			var err apperrors.Error
			catalogID, err = db.DB(ctx).GetCatalogIDByName(ctx, name.Catalog)
			if err != nil {
				if errors.Is(err, dberror.ErrNotFound) {
					return nil, ErrCatalogNotFound
				}
				log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
				return nil, ErrUnableToLoadObject.Msg("failed to load catalog")
			}
		} else {
			return nil, ErrInvalidCatalog.Msg("invalid catalog name")
		}
		if len(name.Variant) > 0 && schemavalidator.ValidateObjectName(name.Variant) {
			var err apperrors.Error
			variantID, err = db.DB(ctx).GetVariantIDFromName(ctx, catalogID, name.Variant)
			if err != nil {
				if errors.Is(err, dberror.ErrNotFound) {
					return nil, ErrVariantNotFound
				}
				log.Ctx(ctx).Error().Err(err).Msg("failed to load variant")
				return nil, ErrUnableToLoadObject.Msg("failed to load variant")
			}
		} else {
			return nil, validationerrors.ErrInvalidNameFormat
		}
	}
	// TODO: handle version number
	name.WorkspaceID = uuid.Nil
	if name.Workspace != "" {
		id, err := uuid.Parse(name.Workspace)
		if err != nil {
			name.WorkspaceLabel = name.Workspace
			// get the workspace id
			wm, err := LoadWorkspaceManagerByLabel(ctx, catalogID, variantID, name.Workspace)
			if err != nil {
				return nil, err
			}
			id = wm.ID()
			name.WorkspaceID = id
		}
		name.WorkspaceID = id
	}

	return &objectResource{
		name:        name,
		catalogID:   catalogID,
		variantID:   variantID,
		workspaceID: name.WorkspaceID,
		rsrcJson:    rsrcJson,
	}, nil
}
