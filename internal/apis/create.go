package apis

import (
	"io"
	"net/http"

	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager"
)

// Create a new resource object
func createObject(r *http.Request) (*httpx.Response, error) {
	ctx := r.Context()

	if r.Body == nil {
		return nil, httpx.ErrInvalidRequest()
	}

	req, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, httpx.ErrUnableToReadRequest()
	}

	rm, err := catalogmanager.ResourceManagerFromRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	resourceName, err := rm.Create(ctx)
	if err != nil {
		return nil, err
	}
	rsp := &httpx.Response{
		StatusCode: http.StatusCreated,
		Location:   resourceName, //TODO: Implement location
		Response:   nil,
	}

	return rsp, nil
}
