package schemamanager

import (
	"encoding/json"
	"reflect"

	"github.com/go-playground/validator/v10"
	schemaerr "github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/errors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schema/schemavalidator"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

// Modifying this struct should also change the Json Marshaler
type SchemaMetadata struct {
	Name        string               `json:"name" validate:"required,resourceNameValidator"`
	Catalog     string               `json:"catalog" validate:"required,resourceNameValidator"`
	Variant     types.NullableString `json:"variant,omitempty" validate:"resourceNameValidator"`
	Namespace   types.NullableString `json:"namespace,omitempty" validate:"omitempty,resourceNameValidator"`
	Path        string               `json:"path,omitempty" validate:"omitempty,resourcePathValidator"`
	Description string               `json:"description"`
}

var _ json.Marshaler = SchemaMetadata{}
var _ json.Marshaler = &SchemaMetadata{}

func (rs *SchemaMetadata) Validate() schemaerr.ValidationErrors {
	var ves schemaerr.ValidationErrors
	err := schemavalidator.V().Struct(rs)
	if err == nil {
		return nil
	}
	ve, ok := err.(validator.ValidationErrors)
	if !ok {
		return append(ves, schemaerr.ErrInvalidSchema)
	}

	value := reflect.ValueOf(rs).Elem()
	typeOfCS := value.Type()

	for _, e := range ve {
		jsonFieldName := schemavalidator.GetJSONFieldPath(value, typeOfCS, e.StructField())
		jsonFieldName = "metadata." + jsonFieldName
		switch e.Tag() {
		case "required":
			ves = append(ves, schemaerr.ErrMissingRequiredAttribute(jsonFieldName))
		case "resourceNameValidator":
			val, _ := e.Value().(string)
			ves = append(ves, schemaerr.ErrInvalidNameFormat(jsonFieldName, val))
		case "resourcePathValidator":
			ves = append(ves, schemaerr.ErrInvalidObjectPath(jsonFieldName))
		case "catalogVersionValidator":
			ves = append(ves, schemaerr.ErrInvalidCatalogVersion(jsonFieldName))
		default:
			ves = append(ves, schemaerr.ErrValidationFailed(jsonFieldName))
		}
	}
	return ves
}

func (s SchemaMetadata) MarshalJSON() ([]byte, error) {
	m := make(map[string]interface{})

	m["name"] = s.Name
	m["catalog"] = s.Catalog
	m["description"] = s.Description

	if s.Variant.Valid {
		m["variant"] = s.Variant.Value
	}
	if s.Namespace.Valid {
		m["namespace"] = s.Namespace.Value
	}
	if s.Path != "" {
		m["path"] = s.Path
	}

	return json.Marshal(m)
}
