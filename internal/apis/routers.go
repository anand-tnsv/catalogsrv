package apis

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/mugiliam/common/hatchrbac"
	"github.com/mugiliam/common/httpx"
)

var resourceObjectHandlers = []httpx.RoleAuthorizedHandlerParam{
	{
		Method:  http.MethodPost,
		Path:    "/create",
		Handler: createObject,
		Op:      hatchrbac.Create,
	},
	{
		Method:  http.MethodGet,
		Path:    "/{catalogName}",
		Handler: getCatalog,
		Op:      hatchrbac.Read,
	},
}

func Router(r chi.Router) {
	//TODO: Implement authentication
	for _, handler := range resourceObjectHandlers {
		r.Method(handler.Method, handler.Path, httpx.WrapHttpRsp(handler.Handler))
	}
}
