package apis

import (
	"io"
	"net/http"
	"path"

	"github.com/go-chi/chi/v5"
	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

// Create a new resource object
func updateObject(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	var kind string

	catalogName := chi.URLParam(r, "catalogName")
	variantName := chi.URLParam(r, "variantName")
	namespace := chi.URLParam(r, "namespace")
	workspace := chi.URLParam(r, "workspaceRef")

	n := catalogmanager.ResourceName{}
	catalogContext := common.CatalogContextFromContext(ctx)
	if workspace != "" {
		n.Workspace = workspace
	} else if catalogContext != nil {
		n.Workspace = catalogContext.WorkspaceLabel
		n.WorkspaceID = catalogContext.WorkspaceId
	}
	if variantName != "" {
		n.Variant = variantName
	} else if catalogContext != nil {
		n.Variant = catalogContext.Variant
		n.VariantID = catalogContext.VariantId
	}
	if catalogName != "" {
		n.Catalog = catalogName
	} else if catalogContext != nil {
		n.Catalog = catalogContext.Catalog
		n.CatalogID = catalogContext.CatalogId
	}
	if namespace != "" {
		n.Namespace = namespace
	} else if catalogContext != nil {
		n.Namespace = catalogContext.Namespace
	}

	objType := chi.URLParam(r, "objectType")
	objectFqn := chi.URLParam(r, "*")
	var objectName, objectPath string

	if objType != "" && !types.InValidObjectTypes(objType) {
		return nil, httpx.ErrInvalidRequest()
	}

	if objectFqn != "" {
		objectName = path.Base(objectFqn)
		if objectName == "/" || objectName == "." {
			objectName = ""
		}

		// objectPath is the path without the last part
		objectPath = path.Dir(objectFqn)
		if objectPath == "." {
			objectPath = "/"
		}
	}

	if objectFqn != "" && objType == types.ObjectTypeCollection {
		kind = types.CollectionSchemaKind
	} else if objectFqn != "" && objType == types.ObjectTypeParameter {
		kind = types.ParameterSchemaKind
	} else if objectFqn != "" && objType == types.ObjectTypeValue {
		kind = types.ValueKind
	} else if workspace != "" {
		kind = types.WorkspaceKind
	} else if variantName != "" {
		kind = types.VariantKind
	} else if catalogName != "" {
		kind = types.CatalogKind
	} else if namespace != "" {
		kind = types.NamespaceKind
	}

	var catObjType types.CatalogObjectType
	if objType == types.ObjectTypeCollection {
		catObjType = types.CatalogObjectTypeCollectionSchema
	} else if objType == types.ObjectTypeParameter {
		catObjType = types.CatalogObjectTypeParameterSchema
	} else if objType == types.ObjectTypeValue {
		catObjType = types.CatalogObjectTypeCatalogCollection
	}

	n.ObjectName = objectName
	n.ObjectPath = objectPath
	n.ObjectType = catObjType

	if r.Body == nil {
		return nil, httpx.ErrInvalidRequest()
	}

	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, httpx.ErrUnableToReadRequest()
	}

	rm, err := catalogmanager.ResourceManagerFromName(ctx, kind, n)
	if err != nil {
		return nil, err
	}

	err = rm.Update(ctx, req)
	if err != nil {
		return nil, err
	}

	rsp := &httpx.Response{
		StatusCode: http.StatusOK,
		Response:   nil,
	}
	return rsp, nil
}
