package apis

import (
	"net/http"
	"path"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/tidwall/gjson"
)

func getResourceName(r *http.Request) (catalogmanager.ResourceName, error) {
	ctx := r.Context()

	catalogName := chi.URLParam(r, "catalogName")
	variantName := chi.URLParam(r, "variantName")
	namespace := chi.URLParam(r, "namespaceName")
	workspace := chi.URLParam(r, "workspaceRef")

	n := catalogmanager.ResourceName{}
	catalogContext := common.CatalogContextFromContext(ctx)
	if workspace != "" {
		n.Workspace = workspace
		n.WorkspaceLabel, n.WorkspaceID = getUUIDOrName(workspace)
	} else if catalogContext != nil {
		n.WorkspaceLabel = catalogContext.WorkspaceLabel
		n.WorkspaceID = catalogContext.WorkspaceId
	}
	if variantName != "" {
		n.Variant, n.VariantID = getUUIDOrName(variantName)
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

	// parse schema and collection objects
	resourceName := chi.URLParam(r, "objectType")
	resourceFqn := chi.URLParam(r, "*")
	var objectName, objectPath string

	if resourceName != "" && !types.InValidObjectTypes(resourceName) {
		return n, httpx.ErrInvalidRequest()
	}

	if resourceFqn != "" {
		objectName = path.Base(resourceFqn)
		if objectName == "/" || objectName == "." {
			objectName = ""
		}

		// objectPath is the path without the last part
		objectPath = path.Dir(resourceFqn)
		if objectPath == "." {
			objectPath = "/"
		}
		objectPath = path.Clean("/" + objectPath) // this will always start with /
	}

	var catObjType types.CatalogObjectType
	if resourceName == types.ResourceNameCollectionSchemas {
		catObjType = types.CatalogObjectTypeCollectionSchema
	} else if resourceName == types.ResourceNameParameterSchemas {
		catObjType = types.CatalogObjectTypeParameterSchema
	} else if resourceName == types.ResourceNameCollections {
		catObjType = types.CatalogObjectTypeCatalogCollection
	}

	n.ObjectName = objectName
	n.ObjectPath = objectPath
	n.ObjectType = catObjType

	return n, nil
}

func getResourceKind(r *http.Request) string {
	// Trim leading and trailing slashes
	path := strings.Trim(r.URL.Path, "/")
	segments := strings.Split(path, "/")
	var resourceName string
	if len(segments) > 0 {
		resourceName = segments[0]
	}
	return types.KindFromResourceName(resourceName)
}

func getUUIDOrName(ref string) (string, uuid.UUID) {
	if ref == "" {
		return "", uuid.Nil
	}
	u, err := uuid.Parse(ref)
	if err != nil {
		return ref, uuid.Nil
	}
	return "", u
}

func validateRequest(reqJson []byte, kind string) error {
	if !gjson.ValidBytes(reqJson) {
		return httpx.ErrInvalidRequest("unable to parse request")
	}
	result := gjson.GetBytes(reqJson, "kind")
	if !result.Exists() {
		return httpx.ErrInvalidRequest("missing kind")
	}
	if result.String() != kind {
		return httpx.ErrInvalidRequest("invalid kind")
	}
	return nil
}
