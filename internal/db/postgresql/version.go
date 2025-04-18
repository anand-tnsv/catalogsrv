package postgresql

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

// CreateVersion creates a new version in the database.
// It automatically assigns a unique version number within the catalog and variant based on the sequence.
// Returns an error if the label already exists for the given variant and catalog (when not empty),
// the label format is invalid, the catalog or variant ID is invalid, or there is a database error.
func (h *hatchCatalogDb) CreateVersion(ctx context.Context, version *models.Version) (err error) {
	// Retrieve tenantID from the context
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}
	version.TenantID = tenantID

	// create a transaction
	tx, err := h.conn().BeginTx(ctx, nil)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to begin transaction")
		return dberror.ErrDatabase.Err(err)
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Ctx(ctx).Error().Err(rollbackErr).Msg("failed to rollback transaction")
			}
		}
	}()
	errDb := h.createVersionWithTransaction(ctx, version, tx)
	if errDb != nil {
		tx.Rollback()
		log.Ctx(ctx).Error().Err(errDb).Msg("failed to create version")
		return errDb
	}
	if err := tx.Commit(); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to commit transaction")
		return dberror.ErrDatabase.Err(err)
	}
	return nil
}

func (h *hatchCatalogDb) createVersionWithTransaction(ctx context.Context, version *models.Version, tx *sql.Tx) apperrors.Error {

	label := sql.NullString{String: version.Label, Valid: version.Label != ""} // Set label as sql.NullString
	query := `
		INSERT INTO versions (label, description, info, variant_id, catalog_id, tenant_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING version_num;
	`

	// Execute the query and scan the returned version_num
	row := tx.QueryRowContext(ctx, query, label, version.Description, version.Info, version.VariantID, version.CatalogID, version.TenantID)
	var versionNum int
	err := row.Scan(&versionNum)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("label", version.Label).Str("variant_id", version.VariantID.String()).Str("catalog_id", version.CatalogID.String()).Msg("version already exists")
			return dberror.ErrAlreadyExists.Msg("version already exists")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			if pgErr.Code == "23505" && pgErr.ConstraintName == "unique_label_variant_catalog_tenant" { // Unique constraint violation
				log.Ctx(ctx).Error().Str("label", version.Label).Str("variant_id", version.VariantID.String()).Str("catalog_id", version.CatalogID.String()).Msg("label already exists for the given variant and catalog")
				return dberror.ErrAlreadyExists.Msg("label already exists for the given variant and catalog")
			}
			if pgErr.Code == "23514" && pgErr.ConstraintName == "versions_label_check" { // Check constraint violation code
				log.Ctx(ctx).Error().Str("label", version.Label).Msg("invalid label format")
				return dberror.ErrInvalidInput.Msg("invalid label format")
			}
			if pgErr.ConstraintName == "versions_variant_id_catalog_id_tenant_id_fkey" || pgErr.ConstraintName == "version_sequences_variant_id_catalog_id_tenant_id_fkey" { // Foreign key constraint violations
				log.Ctx(ctx).Info().Str("variant_id", version.VariantID.String()).Str("catalog_id", version.CatalogID.String()).Msg("catalog or variant not found")
				return dberror.ErrInvalidCatalog
			}
		}
		log.Ctx(ctx).Error().Err(err).Str("label", version.Label).Str("variant_id", version.VariantID.String()).Str("catalog_id", version.CatalogID.String()).Msg("failed to insert version")
		return dberror.ErrDatabase.Err(err)
	}
	version.VersionNum = versionNum

	// Create the parameters, collections, and values directories
	pd := models.SchemaDirectory{
		VersionNum: version.VersionNum,
		VariantID:  version.VariantID,
		CatalogID:  version.CatalogID,
		TenantID:   version.TenantID,
		Directory:  []byte("{}"), // Initialize with empty JSON
	}
	if err := h.createSchemaDirectoryWithTransaction(ctx, types.CatalogObjectTypeParameterSchema, &pd, tx); err != nil {
		return err
	}
	cd := models.SchemaDirectory{
		VersionNum: version.VersionNum,
		VariantID:  version.VariantID,
		CatalogID:  version.CatalogID,
		TenantID:   version.TenantID,
		Directory:  []byte("{}"), // Initialize with empty JSON
	}
	if err := h.createSchemaDirectoryWithTransaction(ctx, types.CatalogObjectTypeCollectionSchema, &cd, tx); err != nil {
		return err
	}
	vd := models.SchemaDirectory{
		VersionNum: version.VersionNum,
		VariantID:  version.VariantID,
		CatalogID:  version.CatalogID,
		TenantID:   version.TenantID,
		Directory:  []byte("{}"), // Initialize with empty JSON
	}
	if err := h.createSchemaDirectoryWithTransaction(ctx, types.CatalogObjectTypeCatalogCollection, &vd, tx); err != nil {
		return err
	}

	// update the parameter, collections, and values directories in version
	version.ParametersDir = pd.DirectoryID
	version.CollectionsDir = cd.DirectoryID
	version.ValuesDir = vd.DirectoryID

	query = `
		UPDATE versions SET parameters_directory = $1, collections_directory = $2, values_directory = $3
		WHERE version_num = $4 AND variant_id = $5 AND catalog_id = $6 AND tenant_id = $7;
	`
	_, err = tx.ExecContext(ctx, query, version.ParametersDir, version.CollectionsDir, version.ValuesDir, version.VersionNum, version.VariantID, version.CatalogID, version.TenantID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to update version with directories")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

// GetVersion retrieves a version from the database based on version_num, variant_id, and catalog_id.
// Returns the version if found, or an error if the version is not found or there is a database error.
func (h *hatchCatalogDb) GetVersion(ctx context.Context, versionNum int, variantID, catalogID uuid.UUID) (*models.Version, error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT version_num, label, description, info, variant_id, catalog_id, tenant_id
		FROM versions
		WHERE version_num = $1 AND variant_id = $2 AND catalog_id = $3 AND tenant_id = $4;
	`

	row := h.conn().QueryRowContext(ctx, query, versionNum, variantID, catalogID, tenantID)
	version := &models.Version{}
	err := row.Scan(&version.VersionNum, &version.Label, &version.Description, &version.Info, &version.VariantID, &version.CatalogID, &version.TenantID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Int("version_num", versionNum).Str("variant_id", variantID.String()).Str("catalog_id", catalogID.String()).Msg("version not found")
			return nil, dberror.ErrNotFound.Msg("version not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve version")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return version, nil
}

// SetVersionLabel updates the label of a version based on its version_num, variant_id, and catalog_id.
// Returns an error if the new label already exists for the variant and catalog,
// the label format is invalid, or there is a database error.
func (h *hatchCatalogDb) SetVersionLabel(ctx context.Context, versionNum int, variantID, catalogID uuid.UUID, newLabel string) error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	if newLabel == "" {
		log.Ctx(ctx).Error().Msg("new label cannot be empty")
		return dberror.ErrInvalidInput.Msg("label cannot be empty")
	}

	query := `
		UPDATE versions
		SET label = $1
		WHERE version_num = $2 AND variant_id = $3 AND catalog_id = $4 AND tenant_id = $5
		RETURNING version_num;
	`

	row := h.conn().QueryRowContext(ctx, query, newLabel, versionNum, variantID, catalogID, tenantID)
	var returnedVersionNum int
	err := row.Scan(&returnedVersionNum)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Int("version_num", versionNum).Str("variant_id", variantID.String()).Str("catalog_id", catalogID.String()).Msg("version not found")
			return dberror.ErrNotFound.Msg("version not found")
		}
		if pgErr, ok := err.(*pgconn.PgError); ok {
			if pgErr.Code == "23505" && pgErr.ConstraintName == "unique_label_variant_catalog_tenant" { // Unique constraint violation
				log.Ctx(ctx).Error().Str("label", newLabel).Str("variant_id", variantID.String()).Str("catalog_id", catalogID.String()).Msg("label already exists for the given variant and catalog")
				return dberror.ErrAlreadyExists.Msg("label already exists for the given variant and catalog")
			}
			if pgErr.Code == "23514" && pgErr.ConstraintName == "versions_label_check" { // Check constraint violation code
				log.Ctx(ctx).Error().Str("label", newLabel).Msg("invalid label format")
				return dberror.ErrInvalidInput.Msg("invalid label format")
			}
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to update version label")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

// UpdateVersionDescription updates the description of a version based on its version_num, variant_id, and catalog_id.
// Returns an error if the version is not found or there is a database error.
func (h *hatchCatalogDb) UpdateVersionDescription(ctx context.Context, versionNum int, variantID, catalogID uuid.UUID, newDescription string) error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	query := `
		UPDATE versions
		SET description = $1
		WHERE version_num = $2 AND variant_id = $3 AND catalog_id = $4 AND tenant_id = $5
		RETURNING version_num;
	`

	row := h.conn().QueryRowContext(ctx, query, newDescription, versionNum, variantID, catalogID, tenantID)
	var returnedVersionNum int
	err := row.Scan(&returnedVersionNum)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Int("version_num", versionNum).Str("variant_id", variantID.String()).Str("catalog_id", catalogID.String()).Msg("version not found")
			return dberror.ErrNotFound.Msg("version not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to update version description")
		return dberror.ErrDatabase.Err(err)
	}

	return nil
}

// DeleteVersion deletes a version from the database based on version_num, variant_id, and catalog_id.
// Returns an error if the version is not found or there is a database error.
func (h *hatchCatalogDb) DeleteVersion(ctx context.Context, versionNum int, variantID, catalogID uuid.UUID) error {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return dberror.ErrMissingTenantID
	}

	query := `
		DELETE FROM versions
		WHERE version_num = $1 AND variant_id = $2 AND catalog_id = $3 AND tenant_id = $4;
	`

	result, err := h.conn().ExecContext(ctx, query, versionNum, variantID, catalogID, tenantID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete version")
		return dberror.ErrDatabase.Err(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve result information")
		return dberror.ErrDatabase.Err(err)
	}

	if rowsAffected == 0 {
		log.Ctx(ctx).Info().Int("version_num", versionNum).Str("variant_id", variantID.String()).Str("catalog_id", catalogID.String()).Msg("version not found")
		return dberror.ErrNotFound.Msg("version not found")
	}

	return nil
}

// CountVersionsInCatalogAndVariant returns the count of all versions for a given catalog and variant.
func (h *hatchCatalogDb) CountVersionsInCatalogAndVariant(ctx context.Context, catalogID, variantID uuid.UUID) (int, error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return 0, dberror.ErrMissingTenantID
	}

	query := `
		SELECT COUNT(*)
		FROM versions
		WHERE catalog_id = $1 AND variant_id = $2 AND tenant_id = $3;
	`

	var count int
	err := h.conn().QueryRowContext(ctx, query, catalogID, variantID, tenantID).Scan(&count)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to count versions")
		return 0, dberror.ErrDatabase.Err(err)
	}

	return count, nil
}

// GetNamedVersions returns all named versions (non-null label) for a given catalog and variant, along with their descriptions.
func (h *hatchCatalogDb) GetNamedVersions(ctx context.Context, catalogID, variantID uuid.UUID) ([]models.Version, error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT version_num, label, description
		FROM versions
		WHERE catalog_id = $1 AND variant_id = $2 AND tenant_id = $3 AND label IS NOT NULL;
	`

	rows, err := h.conn().QueryContext(ctx, query, catalogID, variantID, tenantID)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve named versions")
		return nil, dberror.ErrDatabase.Err(err)
	}
	defer rows.Close()

	var namedVersions []models.Version
	for rows.Next() {
		var version models.Version
		err = rows.Scan(&version.VersionNum, &version.Label, &version.Description)
		if err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to scan named version row")
			return nil, dberror.ErrDatabase.Err(err)
		}
		namedVersions = append(namedVersions, version)
	}

	if err = rows.Err(); err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("error after scanning named versions")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return namedVersions, nil
}

// GetVersionByLabel retrieves a version from the database based on label, catalog_id, and variant_id.
// Returns the version if found, or an error if the version is not found or there is a database error.
func (h *hatchCatalogDb) GetVersionByLabel(ctx context.Context, label string, catalogID, variantID uuid.UUID) (*models.Version, error) {
	tenantID := common.TenantIdFromContext(ctx)
	if tenantID == "" {
		return nil, dberror.ErrMissingTenantID
	}

	query := `
		SELECT version_num, label, description, info, variant_id, catalog_id, tenant_id
		FROM versions
		WHERE label = $1 AND catalog_id = $2 AND variant_id = $3 AND tenant_id = $4;
	`

	row := h.conn().QueryRowContext(ctx, query, label, catalogID, variantID, tenantID)
	version := &models.Version{}
	err := row.Scan(&version.VersionNum, &version.Label, &version.Description, &version.Info, &version.VariantID, &version.CatalogID, &version.TenantID)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Ctx(ctx).Info().Str("label", label).Str("variant_id", variantID.String()).Str("catalog_id", catalogID.String()).Msg("version not found")
			return nil, dberror.ErrNotFound.Msg("version not found")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to retrieve version by label")
		return nil, dberror.ErrDatabase.Err(err)
	}

	return version, nil
}
