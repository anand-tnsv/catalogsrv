package models

import "github.com/mugiliam/hatchcatalogsrv/pkg/types"

type Tenant struct {
	TenantID types.TenantId
}

type Project struct {
	ProjectID types.ProjectId
	TenantID  types.TenantId
}
