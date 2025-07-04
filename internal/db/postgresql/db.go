// Description: This file contains the implementation of the hatchCatalogDb interface for the PostgreSQL database.
package postgresql

import (
	"context"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dbmanager"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

type hatchCatalogDb struct {
	mm *metadataManager
	om *objectManager
	cm *connectionManager
}

func NewHatchCatalogDb(c dbmanager.ScopedConn) (*metadataManager, *objectManager, *connectionManager) {
	h := &hatchCatalogDb{}
	h.mm = newMetadataManager(c)
	h.om = newObjectManager(c)
	h.cm = newConnectionManager(c)
	h.om.m = h.mm
	return h.mm, h.om, h.cm
}

func getTenantAndProjectFromContext(ctx context.Context) (tenantID types.TenantId, projectID types.ProjectId, err apperrors.Error) {
	err = nil
	tenantID = common.TenantIdFromContext(ctx)
	projectID = common.ProjectIdFromContext(ctx)

	// Validate tenantID and projectID to ensure they are not empty
	if tenantID == "" {
		err = dberror.ErrMissingTenantID.Err(dberror.ErrInvalidInput)
	} else if projectID == "" {
		err = dberror.ErrMissingProjecID.Err(dberror.ErrInvalidInput)
	}
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve tenant and project IDs from context")
	}
	return
}
