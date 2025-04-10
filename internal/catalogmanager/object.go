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

	return resource.NewV1ObjectManager(ctx, []byte(rsrcJson), schemamanager.WithValidation())
}

type StoreOptions struct {
	ErrorIfExists bool
	WorkspaceID   uuid.UUID
	VersionNum    int
}

type ObjectStoreOption func(*StoreOptions)

func WithErrorIfExists() ObjectStoreOption {
	return func(o *StoreOptions) {
		o.ErrorIfExists = true
	}
}

func WithWorkspaceID(id uuid.UUID) ObjectStoreOption {
	return func(o *StoreOptions) {
		o.WorkspaceID = id
	}
}

func WithVersionNum(num int) ObjectStoreOption {
	return func(o *StoreOptions) {
		o.VersionNum = num
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
	options := &StoreOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var (
		t            types.CatalogObjectType = s.Type      // object type
		dir          uuid.UUID                             // directory for this object type
		hash         string                  = s.GetHash() // hash of the object's storage representation
		path         string                  = m.Path      // path to the object in the directory
		pathWithName string                  = ""          // path with name
	)

	// strip path with any trailing slashes and append the name to get a FQRP
	path = strings.TrimRight(path, "/")
	pathWithName = path + "/" + m.Name

	// get the directory
	if options.WorkspaceID != uuid.Nil {
		var wm schemamanager.WorkspaceManager
		var apperr apperrors.Error

		if wm, apperr = LoadWorkspaceManagerByID(ctx, options.WorkspaceID); apperr != nil {
			return apperr
		}

		if t == types.CatalogObjectTypeParameterSchema {
			if dir = wm.ParametersDir(); dir == uuid.Nil {
				return ErrInvalidWorkspace.Msg("workspace does not have a parameters directory")
			}
		} else if t == types.CatalogObjectTypeCollectionSchema {
			if dir = wm.CollectionsDir(); dir == uuid.Nil {
				return ErrInvalidWorkspace.Msg("workspace does not have a collections directory")
			}
		} else {
			return ErrCatalogError.Msg("invalid object type")
		}
	} else {
		return ErrInvalidVersionOrWorkspace
	}
	// TODO: handle version number

	if s.Type == types.CatalogObjectTypeParameterSchema {
		// get this objectRef from the directory
		r, err := db.DB(ctx).GetObjectByPath(ctx, t, dir, pathWithName)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				log.Ctx(ctx).Debug().Str("path", pathWithName).Msg("object not found")
			} else {
				log.Ctx(ctx).Error().Err(err).Msg("failed to get object by path")
				return ErrCatalogError
			}
		}
		if r != nil {
			// if the hash is the same, we don't need to save the object
			if r.Hash == hash {
				log.Ctx(ctx).Debug().Str("hash", hash).Msg("object already exists")
				if options.ErrorIfExists {
					return ErrAlreadyExists
				}
				return nil
			}
		}
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
			if options.ErrorIfExists {
				log.Ctx(ctx).Debug().Str("hash", obj.Hash).Msg("object already exists in DB")
				// in this case, we don't return. If we came here it means the object is not in the directory,
				// so we'll keep chugging along and save the object to the directory
			}
		} else {
			log.Ctx(ctx).Error().Err(dberr).Msg("failed to save catalog object")
			return dberr
		}
	}

	// write the object to the directory
	if err := db.DB(ctx).AddOrUpdateObjectByPath(ctx, t, dir, pathWithName, models.ObjectRef{
		Hash: obj.Hash,
	}); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to save object to directory")
		return ErrCatalogError
	}

	return nil
}

func LoadObject(ctx context.Context, hash string, m *schemamanager.ObjectMetadata) (schemamanager.ObjectManager, apperrors.Error) {
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
