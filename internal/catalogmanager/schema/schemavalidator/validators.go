package schemavalidator

import "github.com/go-playground/validator/v10"

const (
	CatalogKind    = "Catalog"
	VariantKind    = "Variant"
	ParameterKind  = "Parameter"
	CollectionKind = "Collection"
)

var validKinds = []string{
	CatalogKind,
	VariantKind,
	ParameterKind,
	CollectionKind,
}

// kindValidator checks if the given kind is a valid resource kind.
func kindValidator(fl validator.FieldLevel) bool {
	kind := fl.Field().String()
	for _, validKind := range validKinds {
		if kind == validKind {
			return true
		}
	}
	return false
}

func init() {
	V().RegisterValidation("kindValidator", kindValidator)
}
