package middleware

import (
	"net/http"

	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/types"
)

func LoadContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantId := r.URL.Query().Get("tenantId")
		projectId := r.URL.Query().Get("projectId")
		r = r.WithContext(
			common.SetProjectIdInContext(
				common.SetTenantIdInContext(r.Context(), types.TenantId(tenantId)),
				types.ProjectId(projectId),
			),
		)
		next.ServeHTTP(w, r)
	})
}
