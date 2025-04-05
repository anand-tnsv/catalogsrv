package customvalidators

import (
	"regexp"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogapi/schema/schemavalidator"
)

const nameRegex = `^[A-Za-z0-9_-]+$`

// nameFormatValidator checks if the given name is alphanumeric with underscores and hyphens.
func nameFormatValidator(fl validator.FieldLevel) bool {
	re := regexp.MustCompile(nameRegex)
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

func init() {
	schemavalidator.V().RegisterValidation("nameFormatValidator", nameFormatValidator)
	schemavalidator.V().RegisterValidation("resourcePathValidator", resourcePathValidator)
}
