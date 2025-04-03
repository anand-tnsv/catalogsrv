package server

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi"
	"github.com/mugiliam/common/hatchservicemiddleware"
	"github.com/mugiliam/common/httpx"
	"github.com/mugiliam/common/logtrace"
	"github.com/mugiliam/hatchcatalogsrv/internal/config"
	"github.com/mugiliam/hatchcatalogsrv/internal/server/middleware"
	"github.com/mugiliam/hatchcatalogsrv/pkg/api"
	"github.com/rs/zerolog/log"
)

type HatchCatalogServer struct {
	Router *chi.Mux
}

func CreateNewServer() (*HatchCatalogServer, error) {
	s := &HatchCatalogServer{}
	s.Router = chi.NewRouter()
	return s, nil
}

func (s *HatchCatalogServer) MountHandlers() {
	s.Router.Use(hatchservicemiddleware.RequestLogger)
	if config.Config().HandleCORS {
		s.Router.Use(s.HandleCORS)
	}
	s.Router.Route("/tenant/{tenantId}/project/{projectId}/catalogs", s.mountResourceHandlers)
	if logtrace.IsTraceEnabled() {
		//print all the routes in the router by transversing the tree and printing the patterns
		fmt.Println("Routes in tenant router")
		walkFunc := func(method string, route string, handler http.Handler, middlewares ...func(http.Handler) http.Handler) error {
			fmt.Printf("%s %s\n", method, route)
			return nil
		}
		if err := chi.Walk(s.Router, walkFunc); err != nil {
			fmt.Printf("Logging err: %s\n", err.Error())
		}
	}
}

func (s *HatchCatalogServer) mountResourceHandlers(r chi.Router) {
	r.Use(middleware.LoadScopedDB)
	//	r.Mount("/node", node.NodeOnboardingRouter())
	r.Get("/version", s.getVersion)
}

func (s *HatchCatalogServer) getVersion(w http.ResponseWriter, r *http.Request) {
	log.Ctx(r.Context()).Debug().Msg("GetVersion")
	rsp := &api.GetVersionRsp{
		ServerVersion: "CatalogSrv: 1.0.0", //TODO - Implement server versioning
		ApiVersion:    api.ApiVersion_1_0,
	}
	httpx.SendJsonRsp(r.Context(), w, http.StatusOK, rsp)
}

func (s *HatchCatalogServer) HandleCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:8190")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")                                                       // Allowed methods
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, X-Hatch-IDToken") // Allowed headers

		// Check if the request method is OPTIONS
		if r.Method == "OPTIONS" {
			log.Ctx(r.Context()).Debug().Msg("OPTIONS request")
			// Respond with appropriate headers and no body
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
