package schemamanager

type CollectionManager interface {
	GetSchema() []byte
	GetMetadata() SchemaMetadata
	GetCollectionSchemaManager() CollectionSchemaManager
}
