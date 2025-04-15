package catalogmanager

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type ResourceName struct {
	Catalog        string
	Variant        string
	WorkspaceID    uuid.UUID
	WorkspaceLabel string
	Workspace      string
	ObjectName     string
	ObjectType     types.CatalogObjectType
	ObjectPath     string
}

// Header of all resource requests
type resourceRequest struct {
	Version string `json:"version" validate:"requireVersionV1"`
	Kind    string `json:"kind" validate:"required,kindValidator"`
}

func (rr *resourceRequest) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(rr)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(rr).Elem()
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
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrUnsupportedKind(jsonFieldName, val))
		case "requireVersionV1":
			ves = append(ves, schemaerr.ErrInvalidVersion(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}

	if ves == nil && rr.Kind != types.CatalogKind {
		ves = append(ves, schemaerr.ErrUnsupportedKind("kind"))
	}

	return ves
}

func RequestType(rsrcJson []byte) (kind string, apperr apperrors.Error) {
	rr := &resourceRequest{}
	if err := json.Unmarshal(rsrcJson, rr); err != nil {
		return "", ErrInvalidSchema.Err(err)
	}

	ves := rr.Validate()
	if ves != nil {
		return "", ErrInvalidSchema.Err(ves)
	}

	return rr.Kind, nil
}

func ResourceManagerFromRequest(ctx context.Context, rsrcJson []byte, name ResourceName) (schemamanager.ResourceManager, apperrors.Error) {
	kind, err := RequestType(rsrcJson)
	if err != nil {
		return nil, err
	}
	switch kind {
	case types.CatalogKind:
		return NewCatalogResource(ctx, rsrcJson, name)
	case types.VariantKind:
		return NewVariantResource(ctx, rsrcJson, name)
	case types.WorkspaceKind:
		return NewWorkspaceResource(ctx, rsrcJson, name)
	case types.CollectionSchemaKind:
		name.ObjectType = types.CatalogObjectTypeCollectionSchema
		return NewObjectResource(ctx, rsrcJson, name)
	case types.ParameterSchemaKind:
		name.ObjectType = types.CatalogObjectTypeParameterSchema
		return NewObjectResource(ctx, rsrcJson, name)
	}
	return nil, ErrInvalidSchema.Msg("unsupported resource kind")
}

func ResourceManagerFromName(ctx context.Context, kind string, name ResourceName) (schemamanager.ResourceManager, apperrors.Error) {
	switch kind {
	case types.CatalogKind:
		return NewCatalogResource(ctx, nil, name)
	case types.VariantKind:
		return NewVariantResource(ctx, nil, name)
	case types.WorkspaceKind:
		return NewWorkspaceResource(ctx, nil, name)
	case types.CollectionSchemaKind:
		return NewObjectResource(ctx, nil, name)
	case types.ParameterSchemaKind:
		return NewObjectResource(ctx, nil, name)
	}
	return nil, ErrInvalidSchema.Msg("unsupported resource kind")
}
