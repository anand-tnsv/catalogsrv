package schemamanager

type ResourceMetadata struct {
	Name    string `json:"name" validate:"required,nameFormatValidator"`
	Catalog string `json:"catalog" validate:"required,nameFormatValidator"`
	Path    string `json:"path" validate:"required,resourcePathValidator"`
}
