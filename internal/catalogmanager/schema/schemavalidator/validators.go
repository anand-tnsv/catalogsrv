package schemavalidator

import (
	"regexp"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

const (
	CatalogKind    = "Catalog"
	VariantKind    = "Variant"
	WorkspaceKind  = "Workspace"
	ParameterKind  = "Parameter"
	CollectionKind = "Collection"
)

var validKinds = []string{
	CatalogKind,
	VariantKind,
	WorkspaceKind,
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
	var str string
	if ns, ok := fl.Field().Interface().(types.NullableString); ok {
		if ns.IsNil() {
			return true
		}
		str = ns.String()
	} else {
		str = fl.Field().String()
	}
	re := regexp.MustCompile(nameRegex)
	return re.MatchString(str)
}

// notNull checks if a nullable value is not null
func notNull(fl validator.FieldLevel) bool {
	nv, ok := fl.Field().Interface().(types.Nullable)
	if !ok { // not a nullable type
		return true
	}
	return !nv.IsNil()
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

func requireVersionV1(fl validator.FieldLevel) bool {
	version := fl.Field().String()
	return version == "v1"
}

func ValidateObjectName(name string) bool {
	re := regexp.MustCompile(nameRegex)
	return re.MatchString(name)
}

func init() {
	V().RegisterValidation("kindValidator", kindValidator)
	V().RegisterValidation("nameFormatValidator", nameFormatValidator)
	V().RegisterValidation("noSpaces", noSpacesValidator)
	V().RegisterValidation("resourcePathValidator", resourcePathValidator)
	V().RegisterValidation("notNull", notNull)
	V().RegisterValidation("requireVersionV1", requireVersionV1)
}
