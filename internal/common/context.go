// Description: This file contains the context package which is used to set and retrieve data from the context.
package common

import (
	"context"

	"github.com/mugiliam/hatchcatalogsrv/internal/types"
)

// ctxTenantIdKeyType represents the key type for the tenant ID in the context.
type ctxTenantIdKeyType string

const ctxTenantIdKey ctxTenantIdKeyType = "HatchCatalogTenantId"

// ctxProjectIdKeyType represents the key type for the project ID in the context.
type ctxProjectIdKeyType string

const ctxProjectIdKey ctxProjectIdKeyType = "HatchCatalogProjectId"

// SetTenantIdInContext sets the tenant ID in the provided context.
func SetTenantIdInContext(ctx context.Context, tenantId types.TenantId) context.Context {
	return context.WithValue(ctx, ctxTenantIdKey, tenantId)
}

// TenantIdFromContext retrieves the tenant ID from the provided context.
func TenantIdFromContext(ctx context.Context) types.TenantId {
	if tenantId, ok := ctx.Value(ctxTenantIdKey).(types.TenantId); ok {
		return tenantId
	}
	return ""
}

// SetProjectIdInContext sets the project ID in the provided context.
func SetProjectIdInContext(ctx context.Context, projectId types.ProjectId) context.Context {
	return context.WithValue(ctx, ctxProjectIdKey, projectId)
}

// ProjectIdFromContext retrieves the project ID from the provided context.
func ProjectIdFromContext(ctx context.Context) types.ProjectId {
	if projectId, ok := ctx.Value(ctxProjectIdKey).(types.ProjectId); ok {
		return projectId
	}
	return ""
}
