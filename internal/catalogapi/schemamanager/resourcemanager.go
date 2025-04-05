package schemamanager

type ResourceManager interface {
	Version() string
	Kind() string
	ParameterManager() ParameterManager
	CollectionManager() CollectionManager
}
