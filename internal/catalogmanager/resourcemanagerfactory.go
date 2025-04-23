package catalogmanager

import (
	"context"

	"github.com/google/uuid"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/tidwall/gjson"
)

type ResourceName struct {
	Catalog        string
	CatalogID      uuid.UUID
	Variant        string
	VariantID      uuid.UUID
	WorkspaceID    uuid.UUID
	WorkspaceLabel string
	Workspace      string
	Namespace      string
	ObjectName     string
	ObjectType     types.CatalogObjectType
	ObjectPath     string
}

func RequestType(rsrcJson []byte) (kind string, apperr apperrors.Error) {
	if !gjson.Valid(string(rsrcJson)) {
		return "", ErrInvalidSchema.Msg("invalid message format")
	}
	result := gjson.GetBytes(rsrcJson, "kind")
	if !result.Exists() {
		return "", ErrInvalidSchema.Msg("missing kind")
	}
	kind = result.String()
	result = gjson.GetBytes(rsrcJson, "version")
	if !result.Exists() {
		return "", ErrInvalidSchema.Msg("missing version")
	}
	version := result.String()
	if schemavalidator.ValidateSchemaKind(kind) && version == types.VersionV1 {
		return kind, nil
	}
	return "", ErrInvalidSchema.Msg("invalid kind or version")
}

type ResourceManagerFactory func(context.Context, []byte, ResourceName) (schemamanager.ResourceManager, apperrors.Error)

var resourceFactories = map[string]ResourceManagerFactory{
	types.CatalogKind:          NewCatalogResource,
	types.VariantKind:          NewVariantResource,
	types.NamespaceKind:        NewNamespaceResource,
	types.WorkspaceKind:        NewWorkspaceResource,
	types.CollectionSchemaKind: NewSchemaResource,
	types.ParameterSchemaKind:  NewSchemaResource,
}

func ResourceManagerFromRequest(ctx context.Context, rsrcJson []byte, name ResourceName) (schemamanager.ResourceManager, apperrors.Error) {
	kind, err := RequestType(rsrcJson)
	if err != nil {
		return nil, err
	}
	if kind == types.CollectionSchemaKind {
		name.ObjectType = types.CatalogObjectTypeCollectionSchema
	} else if kind == types.ParameterSchemaKind {
		name.ObjectType = types.CatalogObjectTypeParameterSchema
	}
	if factory, ok := resourceFactories[kind]; ok {
		return factory(ctx, rsrcJson, name)
	}
	return nil, ErrInvalidSchema.Msg("unsupported resource kind")
}

func ResourceManagerFromName(ctx context.Context, kind string, name ResourceName) (schemamanager.ResourceManager, apperrors.Error) {
	if factory, ok := resourceFactories[kind]; ok {
		return factory(ctx, nil, name)
	}
	return nil, ErrInvalidSchema.Msg("unsupported resource kind")
}
