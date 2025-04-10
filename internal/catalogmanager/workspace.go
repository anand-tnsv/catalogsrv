package catalogmanager

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/mugiliam/common/apperrors"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/db"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/dberror"
	"github.com/mugiliam/hatchcatalogsrv/internal/db/models"
	"github.com/rs/zerolog/log"
)

type workspaceSchema struct {
	Version  string            `json:"version" validate:"requireVersionV1"`
	Kind     string            `json:"kind" validate:"required,kindValidator"`
	Metadata workspaceMetadata `json:"metadata" validate:"required"`
}

type workspaceMetadata struct {
	Catalog     string `json:"catalog" validate:"required,nameFormatValidator"`
	Variant     string `json:"variant" validate:"required,nameFormatValidator"`
	BaseVersion int    `json:"base_version"`
	Description string `json:"description"`
}

type workspaceManager struct {
	w models.Workspace
}

// var _ schemamanager.VariantManager = (*variantManager)(nil)

func NewWorkspaceManager(ctx context.Context, rsrcJson []byte, catalog string, variant string) (schemamanager.WorkspaceManager, apperrors.Error) {
	projectID := common.ProjectIdFromContext(ctx)
	if projectID == "" {
		return nil, ErrInvalidProject
	}

	if len(rsrcJson) == 0 {
		return nil, ErrInvalidSchema
	}

	ws := &workspaceSchema{}
	if err := json.Unmarshal(rsrcJson, ws); err != nil {
		return nil, ErrInvalidSchema.Err(err)
	}

	ves := ws.Validate()
	if ves != nil {
		return nil, ErrInvalidSchema.Err(ves)
	}

	if catalog != "" {
		if !schemavalidator.ValidateObjectName(catalog) {
			return nil, ErrInvalidCatalog
		}
		ws.Metadata.Catalog = catalog
	}

	if variant != "" {
		if !schemavalidator.ValidateObjectName(variant) {
			return nil, validationerrors.ErrInvalidNameFormat
		}
		ws.Metadata.Variant = variant
	}

	// retrieve the catalogID
	catalogID, err := db.DB(ctx).GetCatalogIDByName(ctx, ws.Metadata.Catalog)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
		return nil, err
	}

	// retrieve the variantID
	variantID, err := db.DB(ctx).GetVariantIDFromName(ctx, catalogID, ws.Metadata.Variant)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrVariantNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load variant")
		return nil, err
	}

	// handle the base version
	// TODO: pick the latest version if empty
	if ws.Metadata.BaseVersion == 0 {
		ws.Metadata.BaseVersion = 1
	}

	w := models.Workspace{
		Description: ws.Metadata.Description,
		Info:        pgtype.JSONB{Status: pgtype.Null},
		CatalogID:   catalogID,
		VariantID:   variantID,
		BaseVersion: ws.Metadata.BaseVersion,
	}

	return &workspaceManager{
		w: w,
	}, nil
}

func (ws *workspaceSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(ws)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(ws).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())

		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "nameFormatValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "kindValidator":
			ves = append(ves, schemaerr.ErrUnsupportedKind(jsonFieldName))
		case "requireVersionV1":
			ves = append(ves, schemaerr.ErrInvalidVersion(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return ves
}

func (wm *workspaceManager) ID() uuid.UUID {
	return wm.w.WorkspaceID
}

func (wm *workspaceManager) Description() string {
	return wm.w.Description
}

func (wm *workspaceManager) CatalogID() uuid.UUID {
	return wm.w.CatalogID
}

func (wm *workspaceManager) VariantID() uuid.UUID {
	return wm.w.VariantID
}

func LoadWorkspaceManagerByID(ctx context.Context, workspaceID uuid.UUID) (schemamanager.WorkspaceManager, apperrors.Error) {
	if workspaceID == uuid.Nil {
		return nil, ErrInvalidWorkspace
	}
	w, err := db.DB(ctx).GetWorkspace(ctx, workspaceID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrWorkspaceNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load workspace")
		return nil, ErrWorkspaceNotFound
	}
	return &workspaceManager{
		w: *w,
	}, nil
}

func (wm *workspaceManager) Save(ctx context.Context) apperrors.Error {
	err := db.DB(ctx).CreateWorkspace(ctx, &wm.w)
	if err != nil {
		if errors.Is(err, dberror.ErrAlreadyExists) {
			return ErrAlreadyExists.Msg("workspace already exists")
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to create workspace")
		return ErrCatalogError.Msg("unable to create workspace")
	}
	return nil
}

func DeleteWorkspace(ctx context.Context, workspaceID uuid.UUID) apperrors.Error {
	err := db.DB(ctx).DeleteWorkspace(ctx, workspaceID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return ErrWorkspaceNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete workspace")
		return err
	}
	return nil
}
