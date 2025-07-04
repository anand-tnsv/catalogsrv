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
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
)

type workspaceSchema struct {
	Version  string            `json:"version" validate:"requireVersionV1"`
	Kind     string            `json:"kind" validate:"required,kindValidator"`
	Metadata workspaceMetadata `json:"metadata" validate:""`
}

type workspaceMetadata struct {
	Catalog     string `json:"catalog" validate:"omitempty,resourceNameValidator"`
	Variant     string `json:"variant" validate:"omitempty,resourceNameValidator"`
	BaseVersion int    `json:"-"`
	Description string `json:"description"`
	Label       string `json:"label" validate:"omitempty,resourceNameValidator"`
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
		if !schemavalidator.ValidateSchemaName(catalog) {
			return nil, ErrInvalidCatalog
		}
		ws.Metadata.Catalog = catalog
	}

	if variant != "" {
		if !schemavalidator.ValidateSchemaName(variant) {
			return nil, validationerrors.ErrInvalidNameFormat
		}
		ws.Metadata.Variant = variant
	}

	catalogID := common.GetCatalogIdFromContext(ctx)
	variantID := common.GetVariantIdFromContext(ctx)

	if catalogID == uuid.Nil || ws.Metadata.Catalog != common.GetCatalogFromContext(ctx) {
		var err apperrors.Error
		// retrieve the catalogID
		catalogID, err = db.DB(ctx).GetCatalogIDByName(ctx, ws.Metadata.Catalog)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return nil, ErrCatalogNotFound
			}
			log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
			return nil, err
		}
	}

	// retrieve the variantID
	if variantID == uuid.Nil || ws.Metadata.Variant != common.GetVariantFromContext(ctx) {
		var err apperrors.Error
		variantID, err = db.DB(ctx).GetVariantIDFromName(ctx, catalogID, ws.Metadata.Variant)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return nil, ErrVariantNotFound
			}
			log.Ctx(ctx).Error().Err(err).Msg("failed to load variant")
			return nil, err
		}
	}

	// We don't support multiple versions of a variant. But we'll keep the version construct.
	// Therefore the base version is always 1
	ws.Metadata.BaseVersion = 1

	w := models.Workspace{
		Description: ws.Metadata.Description,
		Info:        pgtype.JSONB{Status: pgtype.Null},
		VariantID:   variantID,
		BaseVersion: ws.Metadata.BaseVersion,
		Label:       ws.Metadata.Label,
	}

	return &workspaceManager{
		w: w,
	}, nil
}

func (ws *workspaceSchema) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	if ws.Kind != types.WorkspaceKind {
		ves = append(ves, schemaerr.ErrUnsupportedKind("kind"))
	}
	err := schemavalidator.V().Struct(ws)
	if err == nil {
		return ves
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
		case "nameFormatValidator", "resourceNameValidator":
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

func (wm *workspaceManager) Label() string {
	return wm.w.Label
}

func (wm *workspaceManager) Description() string {
	return wm.w.Description
}

func (wm *workspaceManager) VariantID() uuid.UUID {
	return wm.w.VariantID
}

func (wm *workspaceManager) BaseVersion() int {
	return wm.w.BaseVersion
}

func (wm *workspaceManager) ParametersDir() uuid.UUID {
	return wm.w.ParametersDir
}

func (wm *workspaceManager) CollectionsDir() uuid.UUID {
	return wm.w.CollectionsDir
}

func (wm *workspaceManager) ValuesDir() uuid.UUID {
	return wm.w.ValuesDir
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

func LoadWorkspaceManagerByLabel(ctx context.Context, variantID uuid.UUID, label string) (schemamanager.WorkspaceManager, apperrors.Error) {
	if variantID == uuid.Nil {
		return nil, ErrInvalidVariant
	}
	w, err := db.DB(ctx).GetWorkspaceByLabel(ctx, variantID, label)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrWorkspaceNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load workspace")
		return nil, ErrCatalogError.Msg("unable to load workspace")
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

func (wm *workspaceManager) ToJson(ctx context.Context) ([]byte, apperrors.Error) {
	// Get name of the catalog
	catalog, err := db.DB(ctx).GetCatalogForWorkspace(ctx, wm.w.WorkspaceID)
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
		return nil, ErrCatalogNotFound
	}

	// Get name of the variant
	variant, err := db.DB(ctx).GetVariant(ctx, catalog.CatalogID, wm.w.VariantID, "")
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrVariantNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load variant")
		return nil, ErrVariantNotFound
	}

	ws := &workspaceSchema{
		Version: "v1",
		Kind:    "Workspace",
		Metadata: workspaceMetadata{
			Catalog:     catalog.Name,
			Variant:     variant.Name,
			BaseVersion: wm.w.BaseVersion,
			Description: wm.w.Description,
			Label:       wm.w.Label,
		},
	}

	jsonData, e := json.Marshal(ws)
	if e != nil {
		log.Ctx(ctx).Error().Err(e).Msg("unable to marshal workspace schema")
		return nil, ErrCatalogError.Msg("unable to marshal workspace schema")
	}

	return jsonData, nil
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

type workspaceResource struct {
	name RequestContext
	vm   schemamanager.WorkspaceManager
}

func (wr *workspaceResource) Name() string {
	return wr.name.Catalog
}

func (wr *workspaceResource) ID() uuid.UUID {
	return wr.name.WorkspaceID
}

func (wr *workspaceResource) Location() string {
	return "/workspaces/" + wr.name.WorkspaceID.String()
}

func (wr *workspaceResource) Manager() schemamanager.WorkspaceManager {
	return wr.vm
}

func (wr *workspaceResource) Create(ctx context.Context, rsrcJson []byte) (string, apperrors.Error) {
	workspace, err := NewWorkspaceManager(ctx, rsrcJson, wr.name.Catalog, wr.name.Variant)
	if err != nil {
		return "", err
	}
	err = workspace.Save(ctx)
	if err != nil {
		return "", err
	}
	wr.name.WorkspaceID = workspace.ID()
	wr.name.WorkspaceLabel = workspace.Label()
	wr.vm = workspace
	if wr.name.Catalog == "" {
		wr.name.Catalog = gjson.GetBytes(rsrcJson, "metadata.catalog").String()
	}
	if wr.name.Variant == "" {
		wr.name.Variant = gjson.GetBytes(rsrcJson, "metadata.variant").String()
	}
	return wr.Location(), nil
}

func (wr *workspaceResource) Get(ctx context.Context) ([]byte, apperrors.Error) {
	if wr.name.WorkspaceID != uuid.Nil {
		workspace, err := LoadWorkspaceManagerByID(ctx, wr.name.WorkspaceID)
		if err != nil {
			return nil, err
		}
		return workspace.ToJson(ctx)
	} else if wr.name.WorkspaceLabel != "" {
		workspace, err := LoadWorkspaceManagerByLabel(ctx, wr.name.VariantID, wr.name.WorkspaceLabel)
		if err != nil {
			return nil, err
		}
		return workspace.ToJson(ctx)
	}
	return nil, ErrInvalidWorkspace
}

func (wr *workspaceResource) Delete(ctx context.Context) apperrors.Error {
	id := wr.name.WorkspaceID
	if id == uuid.Nil {
		err := db.DB(ctx).DeleteWorkspaceByLabel(ctx, wr.name.VariantID, wr.name.WorkspaceLabel)
		if err != nil {
			if !errors.Is(err, dberror.ErrNotFound) {
				log.Ctx(ctx).Error().Err(err).Msg("failed to delete workspace")
				return ErrUnableToDeleteObject.Msg("unable to delete workspace")
			}
		}
		return nil
	}
	err := DeleteWorkspace(ctx, id)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to delete workspace")
		return ErrUnableToDeleteObject.Msg("unable to delete workspace")
	}
	return nil
}

func (wr *workspaceResource) Update(ctx context.Context, rsrcJson []byte) apperrors.Error {
	ws := &workspaceSchema{}
	if err := json.Unmarshal(rsrcJson, ws); err != nil {
		return ErrInvalidSchema.Err(err)
	}

	ves := ws.Validate()
	if ves != nil {
		return ErrInvalidSchema.Err(ves)
	}

	var w *models.Workspace
	var err apperrors.Error
	if wr.name.WorkspaceID != uuid.Nil {
		w, err = db.DB(ctx).GetWorkspace(ctx, wr.name.WorkspaceID)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return ErrWorkspaceNotFound
			}
			log.Ctx(ctx).Error().Err(err).Msg("failed to load workspace")
			return ErrUnableToLoadObject.Msg("unable to load workspace")
		}
	} else if wr.name.WorkspaceLabel != "" {
		w, err = db.DB(ctx).GetWorkspaceByLabel(ctx, wr.name.VariantID, wr.name.WorkspaceLabel)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return ErrWorkspaceNotFound
			}
			log.Ctx(ctx).Error().Err(err).Msg("failed to load workspace")
			return ErrUnableToLoadObject.Msg("unable to load workspace")
		}
	} else {
		return ErrInvalidWorkspace
	}

	w.Description = ws.Metadata.Description
	w.Label = ws.Metadata.Label

	err = db.DB(ctx).UpdateWorkspace(ctx, w)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to update workspace")
		return ErrUnableToUpdateObject.Msg("failed to update workspace")
	}

	return nil
}

func NewWorkspaceResource(ctx context.Context, name RequestContext) (schemamanager.ResourceManager, apperrors.Error) {
	if name.Catalog == "" || name.CatalogID == uuid.Nil {
		return nil, ErrInvalidCatalog
	}
	if name.Variant == "" || name.VariantID == uuid.Nil {
		return nil, ErrInvalidVariant
	}
	return &workspaceResource{
		name: name,
	}, nil
}
