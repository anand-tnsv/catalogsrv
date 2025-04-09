package schemavalidator

import (
	"regexp"
	"strings"

	"github.com/go-playground/validator/v10"
)

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

const nameRegex = `^[A-Za-z0-9_-]+$`

// nameFormatValidator checks if the given name is alphanumeric with underscores and hyphens.
func nameFormatValidator(fl validator.FieldLevel) bool {
	re := regexp.MustCompile(nameRegex)
	return re.MatchString(fl.Field().String())
}

func noSpacesValidator(fl validator.FieldLevel) bool {
	re := regexp.MustCompile(`^[^\s]+$`)
	return re.MatchString(fl.Field().String())
}

// resourcePathValidator checks if the given path is a valid resource path.
func resourcePathValidator(fl validator.FieldLevel) bool {
	path := fl.Field().String()
	// Ensure the path starts with a slash, indicating a root path
	if !strings.HasPrefix(path, "/") {
		return false
	}

	// Split the path by slashes and check each collection name
	collections := strings.Split(path, "/")[1:]
	re := regexp.MustCompile(nameRegex)

	for _, collection := range collections {
		// If a segment is empty, continue (e.g., trailing slash is allowed)
		if collection == "" {
			continue
		}

		// Validate each folder name using the regex
		if !re.MatchString(collection) {
			return false
		}
	}

	return true
}

func ValidateObjectName(name string) bool {
	re := regexp.MustCompile(nameRegex)
	return re.MatchString(name)
}

func init() {
	V().RegisterValidation("kindValidator", kindValidator)
	V().RegisterValidation("nameFormatValidator", nameFormatValidator)
	V().RegisterValidation("noSpacesValidator", noSpacesValidator)
	V().RegisterValidation("resourcePathValidator", resourcePathValidator)
}
