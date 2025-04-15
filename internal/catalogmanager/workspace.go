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
	Catalog     string `json:"catalog" validate:"required,resourceNameValidator"`
	Variant     string `json:"variant" validate:"required,resourceNameValidator"`
	BaseVersion int    `json:"base_version"`
	Description string `json:"description"`
	Label       string `json:"label"`
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
		Label:       ws.Metadata.Label,
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

func (wm *workspaceManager) Label() string {
	return wm.w.Label
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

func LoadWorkspaceManagerByLabel(ctx context.Context, catalogID, variantID uuid.UUID, label string) (schemamanager.WorkspaceManager, apperrors.Error) {
	if catalogID == uuid.Nil {
		return nil, ErrInvalidCatalog
	}
	if variantID == uuid.Nil {
		return nil, ErrInvalidVariant
	}
	w, err := db.DB(ctx).GetWorkspaceByLabel(ctx, catalogID, variantID, label)
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
	catalog, err := db.DB(ctx).GetCatalog(ctx, wm.w.CatalogID, "")
	if err != nil {
		if errors.Is(err, dberror.ErrNotFound) {
			return nil, ErrCatalogNotFound
		}
		log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
		return nil, ErrCatalogNotFound
	}

	// Get name of the variant
	variant, err := db.DB(ctx).GetVariant(ctx, wm.w.CatalogID, wm.w.VariantID, "")
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
	name      ResourceName
	catalogID uuid.UUID
	variantID uuid.UUID
	rsrcJson  []byte
	vm        schemamanager.WorkspaceManager
}

func (wr *workspaceResource) Name() string {
	return wr.name.Catalog
}

func (wr *workspaceResource) ID() uuid.UUID {
	return wr.name.WorkspaceID
}

func (wr *workspaceResource) Location() string {
	return wr.name.Catalog + "/variants/" + wr.name.Variant + "/workspaces/" + wr.name.WorkspaceID.String()
}

func (wr *workspaceResource) ResourceJson() []byte {
	return wr.rsrcJson
}

func (wr *workspaceResource) Manager() schemamanager.WorkspaceManager {
	return wr.vm
}

func (wr *workspaceResource) Create(ctx context.Context) (string, apperrors.Error) {
	workspace, err := NewWorkspaceManager(ctx, wr.rsrcJson, wr.name.Catalog, wr.name.Variant)
	if err != nil {
		return "", err
	}
	err = workspace.Save(ctx)
	if err != nil {
		return "", err
	}
	wr.name.WorkspaceID = workspace.ID()
	wr.name.WorkspaceLabel = workspace.Label()
	ws := &workspaceSchema{}
	json.Unmarshal(wr.rsrcJson, ws)
	if wr.name.Catalog == "" {
		wr.name.Catalog = ws.Metadata.Catalog
	}
	if wr.name.Variant == "" {
		wr.name.Variant = ws.Metadata.Variant
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
		workspace, err := LoadWorkspaceManagerByLabel(ctx, wr.catalogID, wr.variantID, wr.name.WorkspaceLabel)
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
		// try to load the workspace by label
		w, err := LoadWorkspaceManagerByLabel(ctx, wr.catalogID, wr.variantID, wr.name.WorkspaceLabel)
		if err != nil {
			if errors.Is(err, ErrWorkspaceNotFound) {
				return nil
			}
			log.Ctx(ctx).Error().Err(err).Msg("failed to delete workspace")
			return ErrUnableToDeleteObject.Msg("unable to delete workspace")
		}
		id = w.ID()
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

	if wr.catalogID == uuid.Nil {
		cid, err := db.DB(ctx).GetCatalogIDByName(ctx, ws.Metadata.Catalog)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return ErrCatalogNotFound
			}
			log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
			return err
		}
		wr.catalogID = cid
	}
	if wr.variantID == uuid.Nil {
		vid, err := db.DB(ctx).GetVariantIDFromName(ctx, wr.catalogID, ws.Metadata.Variant)
		if err != nil {
			if errors.Is(err, dberror.ErrNotFound) {
				return ErrVariantNotFound
			}
			log.Ctx(ctx).Error().Err(err).Msg("failed to load variant")
			return err
		}
		wr.variantID = vid
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
		w, err = db.DB(ctx).GetWorkspaceByLabel(ctx, wr.catalogID, wr.variantID, wr.name.WorkspaceLabel)
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

func NewWorkspaceResource(ctx context.Context, rsrcJson []byte, name ResourceName) (schemamanager.ResourceManager, apperrors.Error) {
	catalogID, variantID := uuid.Nil, uuid.Nil
	if len(rsrcJson) == 0 || (len(name.Catalog) > 0 && len(name.Variant) > 0) {
		if len(name.Catalog) > 0 && schemavalidator.ValidateSchemaName(name.Catalog) {
			var err apperrors.Error
			catalogID, err = db.DB(ctx).GetCatalogIDByName(ctx, name.Catalog)
			if err != nil {
				if errors.Is(err, dberror.ErrNotFound) {
					return nil, ErrCatalogNotFound
				}
				log.Ctx(ctx).Error().Err(err).Msg("failed to load catalog")
				return nil, ErrUnableToLoadObject.Msg("failed to load catalog")
			}
		} else {
			return nil, ErrInvalidCatalog.Msg("invalid catalog name")
		}
		if len(name.Variant) > 0 && schemavalidator.ValidateSchemaName(name.Variant) {
			var err apperrors.Error
			variantID, err = db.DB(ctx).GetVariantIDFromName(ctx, catalogID, name.Variant)
			if err != nil {
				if errors.Is(err, dberror.ErrNotFound) {
					return nil, ErrVariantNotFound
				}
				log.Ctx(ctx).Error().Err(err).Msg("failed to load variant")
				return nil, ErrUnableToLoadObject.Msg("failed to load variant")
			}
		} else {
			return nil, validationerrors.ErrInvalidNameFormat
		}
	}
	id, err := uuid.Parse(name.Workspace)
	if err != nil {
		name.WorkspaceLabel = name.Workspace
	} else {
		name.WorkspaceID = id
	}
	return &workspaceResource{
		name:      name,
		catalogID: catalogID,
		variantID: variantID,
		rsrcJson:  rsrcJson,
	}, nil
}
