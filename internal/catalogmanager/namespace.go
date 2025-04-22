package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

type namespaceSchema struct {
	Version  string            `json:"version" validate:"requireVersionV1"`
	Kind     string            `json:"kind" validate:"required,kindValidator"`
	Metadata namespaceMetadata `json:"metadata" validate:"required"`
}

type namespaceMetadata struct {
	Catalog     string `json:"catalog" validate:"required,resourceNameValidator"`
	Variant     string `json:"variant" validate:"required,resourceNameValidator"`
	Name        string `json:"name" validate:"required,resourceNameValidator"`
	Description string `json:"description"`
}

type namespaceManager struct {
	n models.Namespace
}

// var _ schemamanager.VariantManager = (*variantManager)(nil)

func NewNamespaceManager(ctx context.Context, rsrcJson []byte, catalog string, variant string) (schemamanager.NamespaceManager, apperrors.Error) {
	projectID := common.ProjectIdFromContext(ctx)
	if projectID == "" {
		return nil, ErrInvalidProject
	}

	if len(rsrcJson) == 0 {
		return nil, ErrInvalidSchema
	}

	ns := &namespaceSchema{}
	if err := json.Unmarshal(rsrcJson, ns); err != nil {
		return nil, ErrInvalidSchema.Err(err)
	}

	ves := ns.Validate()
	if ves != nil {
		return nil, ErrInvalidSchema.Err(ves)
	}

	if catalog != "" {
		if !schemavalidator.ValidateSchemaName(catalog) {
			return nil, ErrInvalidCatalog
		}
		ns.Metadata.Catalog = catalog
	}

	if variant != "" {
		if !schemavalidator.ValidateSchemaName(variant) {
			return nil, validationerrors.ErrInvalidNameFormat
		}
		ns.Metadata.Variant = variant
	}

	// retrieve the catalogID
	catalogID, err := db.DB(ctx).GetCatalogIDByName(ctx, ns.Metadata.Catalog)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
		return nil, err
	}

	// retrieve the variantID
	variantID, err := db.DB(ctx).GetVariantIDFromName(ctx, catalogID, ns.Metadata.Variant)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrVariantNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load variant")
		return nil, err
	}

	n := models.Namespace{
		Description: ns.Metadata.Description,
		VariantID:   variantID,
		CatalogID:   catalogID,
		Name:        ns.Metadata.Name,
		Catalog:     ns.Metadata.Catalog,
		Variant:     ns.Metadata.Variant,
		Info:        nil,
	}

	return &namespaceManager{
		n: n,
	}, nil
}

func (ns *namespaceSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(ns)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(ns).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())

		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "kindValidator":
			ves = append(ves, schemaerr.ErrUnsupportedKind(jsonFieldName))
		case "requireVersionV1":
			ves = append(ves, schemaerr.ErrInvalidVersion(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return ves
}

func (nm *namespaceManager) Name() string {
	return nm.n.Name
}

func (nm *namespaceManager) Description() string {
	return nm.n.Description
}

func (nm *namespaceManager) VariantID() uuid.UUID {
	return nm.n.VariantID
}

func (nm *namespaceManager) CatalogID() uuid.UUID {
	return nm.n.CatalogID
}

func (nm *namespaceManager) Catalog() string {
	return nm.n.Catalog
}

func (nm *namespaceManager) Variant() string {
	return nm.n.Variant
}

func (nm *namespaceManager) GetNamespaceModel() *models.Namespace {
	return &nm.n
}

func LoadNamespaceManagerByName(ctx context.Context, variantID uuid.UUID, name string) (schemamanager.NamespaceManager, apperrors.Error) {
	if variantID == uuid.Nil {
		return nil, ErrInvalidVariant
	}
	n, err := db.DB(ctx).GetNamespace(ctx, name, variantID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrNamespaceNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load namespace")
		return nil, ErrCatalogError.Msg("unable to load namespace")
	}
	return &namespaceManager{
		n: *n,
	}, nil
}

func (nm *namespaceManager) Save(ctx context.Context) apperrors.Error {
	err := db.DB(ctx).CreateNamespace(ctx, &nm.n)
	if err != nil {
		if errors.Is(err, dberror.ErrAlreadyExists) {
			return ErrAlreadyExists.Msg("namespace already exists")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to create namespace")
		return ErrCatalogError.Msg("unable to create namespace")
	}
	return nil
}

func (nm *namespaceManager) ToJson(ctx context.Context) ([]byte, apperrors.Error) {
	ns := &namespaceSchema{
		Version: "v1",
		Kind:    types.NamespaceKind,
		Metadata: namespaceMetadata{
			Catalog:     nm.n.Catalog,
			Variant:     nm.n.Variant,
			Name:        nm.n.Name,
			Description: nm.n.Description,
		},
	}

	jsonData, e := json.Marshal(ns)
	if e != nil {
		log.Ctx(ctx).Error().Err(e).Msg("unable to marshal workspace schema")
		return nil, ErrCatalogError.Msg("unable to marshal workspace schema")
	}

	return jsonData, nil
}

func DeleteNamespace(ctx context.Context, name string, variantID uuid.UUID) apperrors.Error {
	err := db.DB(ctx).DeleteNamespace(ctx, name, variantID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrNamespaceNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete namespace")
		return err
	}
	return nil
}

type namespaceResource struct {
	name      ResourceName
	catalogID uuid.UUID
	variantID uuid.UUID
	rsrcJson  []byte
	nm        schemamanager.NamespaceManager
}

func (nr *namespaceResource) Name() string {
	return nr.name.Catalog
}

func (nr *namespaceResource) ID() uuid.UUID {
	return nr.name.WorkspaceID
}

func (nr *namespaceResource) Location() string {
	return nr.name.Catalog + "/variants/" + nr.name.Variant + "/namespaces/" + nr.name.Namespace
}

func (nr *namespaceResource) ResourceJson() []byte {
	return nr.rsrcJson
}

func (nr *namespaceResource) Manager() schemamanager.NamespaceManager {
	return nr.nm
}

func (nr *namespaceResource) Create(ctx context.Context) (string, apperrors.Error) {
	namespace, err := NewNamespaceManager(ctx, nr.rsrcJson, nr.name.Catalog, nr.name.Variant)
	if err != nil {
		return "", err
	}
	err = namespace.Save(ctx)
	if err != nil {
		return "", err
	}
	nr.name.Namespace = namespace.Name()
	if nr.name.Catalog == "" {
		nr.name.Catalog = namespace.Catalog()
	}
	if nr.name.Variant == "" {
		nr.name.Variant = namespace.Variant()
	}
	return nr.Location(), nil
}

func (nr *namespaceResource) Get(ctx context.Context) ([]byte, apperrors.Error) {
	if nr.variantID == uuid.Nil || nr.name.Namespace == "" {
		return nil, ErrInvalidNamespace
	}
	namespace, err := LoadNamespaceManagerByName(ctx, nr.variantID, nr.name.Namespace)
	if err != nil {
		if errors.Is(err, ErrNamespaceNotFound) {
			return nil, nil
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load namespace")
		return nil, ErrUnableToLoadObject.Msg("unable to load namespace")
	}
	jsonData, err := namespace.ToJson(ctx)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("unable to marshal namespace schema")
		return nil, ErrUnableToLoadObject.Msg("unable to marshal namespace schema")
	}
	nr.nm = namespace
	return jsonData, nil
}

func (nr *namespaceResource) Delete(ctx context.Context) apperrors.Error {
	if nr.variantID == uuid.Nil || nr.name.Namespace == "" {
		return ErrInvalidNamespace
	}
	err := DeleteNamespace(ctx, nr.name.Namespace, nr.variantID)
	if err != nil {
		if errors.Is(err, ErrNamespaceNotFound) {
			return nil
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete namespace")
		return ErrUnableToDeleteObject.Msg("unable to delete namespace")
	}
	return nil
}

func (nr *namespaceResource) Update(ctx context.Context, rsrcJson []byte) apperrors.Error {
	if nr.nm == nil {
		return ErrInvalidNamespace
	}
	ns := &namespaceSchema{}
	if err := json.Unmarshal(rsrcJson, ns); err != nil {
		return ErrInvalidSchema.Err(err)
	}
	ves := ns.Validate()
	if ves != nil {
		return ErrInvalidSchema.Err(ves)
	}
	_, err := nr.Get(ctx)
	if err != nil {
		return err
	}
	namespace := nr.nm.GetNamespaceModel()
	if namespace == nil {
		return ErrInvalidNamespace
	}
	namespace.Description = ns.Metadata.Description
	namespace.Name = ns.Metadata.Name
	err = db.DB(ctx).UpdateNamespace(ctx, namespace)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrNamespaceNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to update namespace")
		return ErrUnableToLoadObject.Msg("unable to update namespace")
	}
	nr.name.Namespace = namespace.Name
	return nil
}

func NewNamespaceResource(ctx context.Context, rsrcJson []byte, name ResourceName) (schemamanager.ResourceManager, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, ErrInvalidSchema
	}
	catalogID, variantID := uuid.Nil, uuid.Nil
	if len(rsrcJson) == 0 || (len(name.Catalog) > 0 && len(name.Variant) > 0) {
		if len(name.Catalog) > 0 && schemavalidator.ValidateSchemaName(name.Catalog) {
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
		if len(name.Variant) > 0 && schemavalidator.ValidateSchemaName(name.Variant) {
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
	return &namespaceResource{
		name:      name,
		catalogID: catalogID,
		variantID: variantID,
		rsrcJson:  rsrcJson,
	}, nil
}
