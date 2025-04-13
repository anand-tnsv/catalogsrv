package apis

import (
	"io"
	"net/http"

	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

// Create a new resource object
func createObject(r *http.Request) (rspRet *httpx.Response, errRet error) {
	ctx := r.Context()
	defer func() {
		errRet = ToHttpxError(errRet)
	}()

	if r.Body == nil {
		return nil, httpx.ErrInvalidRequest()
	}

	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, httpx.ErrUnableToReadRequest()
	}

	kind, err := catalogmanager.RequestType(req)
	if err != nil {
		return nil, err
	}

	var resourceName string = ""

	switch kind {
	case types.CatalogKind:
		catalog, err := catalogmanager.NewCatalogManager(ctx, req, "")
		if err != nil {
			return nil, err
		}
		err = catalog.Save(ctx)
		if err != nil {
			return nil, err
		}
		resourceName = catalog.Name()
	}

	rsp := &httpx.Response{
		StatusCode: http.StatusAccepted,
		Location:   resourceName, //TODO: Implement location
		Response:   nil,
	}

	return rsp, nil
}
