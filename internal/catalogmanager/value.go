package catalogmanager

import (
	"context"
	"encoding/json"
	"path"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

type valueSchema struct {
	Version  string        `json:"version" validate:"required"`
	Kind     string        `json:"kind" validate:"required,kindValidator"`
	Metadata ValueMetadata `json:"metadata" validate:"required"`
	Spec     valueSpec     `json:"spec" validate:"required"`
}

type ValueMetadata struct {
	Catalog    string               `json:"catalog" validate:"required,nameFormatValidator"`
	Variant    types.NullableString `json:"variant" validate:"required,nameFormatValidator"`
	Collection string               `json:"collection" validate:"required,resourcePathValidator"`
}

type valueSpec map[string]types.NullableAny

func (vs *valueSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(vs)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(vs).Elem()
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
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return ves
}

func SaveValue(ctx context.Context, valueJson []byte, m *ValueMetadata, opts ...ObjectStoreOption) apperrors.Error {
	if len(valueJson) == 0 {
		return validationerrors.ErrEmptySchema
	}

	// get the options
	options := &storeOptions{}
	for _, opt := range opts {
		opt(options)
	}

	v := valueSchema{}
	if err := json.Unmarshal(valueJson, &v); err != nil {
		log.Ctx(ctx).Debug().Err(err).Msg("failed to unmarshal value schema")
		return validationerrors.ErrInvalidSchema
	}

	if err := canonicalizeValueMetadata(v, m); err != nil {
		return err
	}

	if err := v.Validate(); err != nil {
		return validationerrors.ErrSchemaValidation.Msg(err.Error())
	}

	var dir Directories

	// get the directories
	if options.WorkspaceID != uuid.Nil {
		var wm schemamanager.WorkspaceManager
		var apperr apperrors.Error

		if wm, apperr = LoadWorkspaceManagerByID(ctx, options.WorkspaceID); apperr != nil {
			return apperr
		}

		if dir.ParametersDir = wm.ParametersDir(); dir.ParametersDir == uuid.Nil {
			return ErrInvalidWorkspace.Msg("workspace does not have a parameters directory")
		}

		if dir.CollectionsDir = wm.CollectionsDir(); dir.CollectionsDir == uuid.Nil {
			return ErrInvalidWorkspace.Msg("workspace does not have a collections directory")
		}
	} else {
		return ErrInvalidVersionOrWorkspace
	}

	// load the object manager
	om, err := LoadObjectByPath(ctx,
		types.CatalogObjectTypeCollectionSchema,
		&schemamanager.ObjectMetadata{
			Catalog: v.Metadata.Catalog,
			Variant: v.Metadata.Variant,
			Path:    path.Dir(v.Metadata.Collection),
			Name:    path.Base(v.Metadata.Collection),
		},
		WithDirectories(dir))
	if err != nil {
		return err
	}

	// get the loaders
	loaders := getObjectLoaders(ctx, om.Metadata(), WithDirectories(dir))

	// validate the value against the collection
	c := om.CollectionManager()
	if c == nil {
		return validationerrors.ErrSchemaValidation.Msg("failed to load collection manager")
	}
	for param, value := range v.Spec {
		if err := c.ValidateValue(ctx, loaders, param, value); err != nil {
			return err
		}
		c.SetValue(ctx, param, value)
	}

	// save the collection object
	if err := SaveObject(ctx, om, WithDirectories(dir), SkipValidationForUpdate()); err != nil {
		return err
	}

	return nil
}

func canonicalizeValueMetadata(v valueSchema, m *ValueMetadata) apperrors.Error {
	if m != nil {
		if m.Catalog != "" {
			v.Metadata.Catalog = m.Catalog
		}
		if !m.Variant.IsNil() {
			v.Metadata.Variant = m.Variant
		}
		if m.Collection != "" {
			v.Metadata.Collection = m.Collection
		}
	}

	if v.Metadata.Variant.IsNil() {
		v.Metadata.Variant = types.NullableString{Value: types.DefaultVariant, Valid: true}
	}

	return nil
}
