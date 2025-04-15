package catalogmanager

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

type collectionSchema struct {
	Version  string             `json:"version" validate:"required"`
	Kind     string             `json:"kind" validate:"required,kindValidator"`
	Metadata CollectionMetadata `json:"metadata" validate:"required"`
	Spec     collectionSpec     `json:"spec" validate:"required"`
}

type CollectionMetadata struct {
	Name        string               `json:"name" validate:"required,nameFormatValidator"`
	Catalog     string               `json:"catalog" validate:"required,resourceNameValidator"`
	Variant     types.NullableString `json:"variant" validate:"required,resourceNameValidator"`
	Path        string               `json:"path" validate:"required,resourcePathValidator"`
	Description string               `json:"description"`
}

type collectionSpec struct {
	Schema string `json:"schema" validate:"required, nameFormatValidator"`
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
	Schema collectionSchema
	csm    schemamanager.CollectionSchemaManager
}

func (cm *collectionManager) GetSchema() []byte {
	b, _ := json.Marshal(cm.Schema)
	return b
}

func (cm *collectionManager) GetMetadata() schemamanager.SchemaMetadata {
	return schemamanager.SchemaMetadata{
		Name:        cm.Schema.Metadata.Name,
		Catalog:     cm.Schema.Metadata.Catalog,
		Variant:     cm.Schema.Metadata.Variant,
		Path:        cm.Schema.Metadata.Path,
		Description: cm.Schema.Metadata.Description,
	}
}

func (cm *collectionManager) GetCollectionSchemaManager() schemamanager.CollectionSchemaManager {
	return cm.csm
}

func NewCollectionManager(ctx context.Context, rsrcJson []byte, m *schemamanager.SchemaMetadata) (schemamanager.CollectionManager, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}

	// get the metadata, replace fields in json from provided metadata. Set defaults.
	rsrcJson, m, err := canonicalizeMetadata(rsrcJson, m)
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
		Schema: cs,
	}, nil
}
