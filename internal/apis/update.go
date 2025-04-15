package apis

import (
	"io"
	"net/http"
	"path"

	"github.com/go-chi/chi/v5"
	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

// Create a new resource object
func updateObject(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()
	var kind string

	catalogName := chi.URLParam(r, "catalogName")
	variantName := chi.URLParam(r, "variantName")
	workspaceName := chi.URLParam(r, "workspaceRef")
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
		kind = types.CollectionKind
	} else if objectFqn != "" && objType == types.ObjectTypeParameter {
		kind = types.ParameterKind
	} else if objectFqn != "" && objType == types.ObjectTypeValue {
		kind = types.ValueKind
	} else if workspaceName != "" {
		kind = types.WorkspaceKind
	} else if variantName != "" {
		kind = types.VariantKind
	} else if catalogName != "" {
		kind = types.CatalogKind
	}

	var catObjType types.CatalogObjectType
	if objType == types.ObjectTypeCollection {
		catObjType = types.CatalogObjectTypeCollectionSchema
	} else if objType == types.ObjectTypeParameter {
		catObjType = types.CatalogObjectTypeParameterSchema
	} else if objType == types.ObjectTypeValue {
		catObjType = types.CatalogObjectTypeCatalogCollectionValue
	}

	if r.Body == nil {
		return nil, httpx.ErrInvalidRequest()
	}

	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, httpx.ErrUnableToReadRequest()
	}

	rm, err := catalogmanager.ResourceManagerFromName(ctx, kind, catalogmanager.ResourceName{
		Catalog:    catalogName,
		Variant:    variantName,
		Workspace:  workspaceName, // either label or workspace ID is accepted and will be parsed later
		ObjectName: objectName,
		ObjectPath: objectPath,
		ObjectType: catObjType,
	})
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
