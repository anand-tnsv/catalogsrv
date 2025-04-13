package schemastore

import (
	"encoding/json"

	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
)

type SchemaStorageRepresentation struct {
	Version     string                  `json:"version"`
	Type        types.CatalogObjectType `json:"type"`
	Description string                  `json:"description"`
	Schema      json.RawMessage         `json:"schema"`
	Values      json.RawMessage         `json:"values"`
}

// Serialize converts the SchemaStorageRepresentation to a JSON byte array
func (s *SchemaStorageRepresentation) Serialize() ([]byte, error) {
	return json.Marshal(s)
}

// GetHash returns the SHA-512 hash of the normalized SchemaStorageRepresentation
func (s *SchemaStorageRepresentation) GetHash() string {
	sz, err := s.Serialize()
	if err != nil {
		return ""
	}
	// Normalize the JSON, so 2 equivalent representations yield the same hash
	nsz, err := NormalizeJSON(sz)
	if err != nil {
		return ""
	}
	hash := HexEncodedSHA512(nsz)
	return hash
}

// Size returns the approximate size of the SchemaStorageRepresentation in bytes
func (s *SchemaStorageRepresentation) Size() int {
	return len(s.Schema) + len(s.Version) + len(s.Type)
}
