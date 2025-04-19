package catalogmanager

import (
	"context"
	"encoding/json"
	"path"

	"github.com/mugiliam/common/apperrors"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/schemamanager"
	"github.com/mugiliam/hatchcatalogsrv/internal/catalogmanager/validationerrors"
	"github.com/mugiliam/hatchcatalogsrv/pkg/types"
	"github.com/rs/zerolog/log"
)

// preventing undefined use warnings
var _ = canonicalizeMetadata
var _ = getMetadata

func getMetadata(ctx context.Context, rsrcJson []byte) (*schemamanager.SchemaMetadata, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, validationerrors.ErrEmptySchema
	}

	var rs struct {
		VersionHeader
		Metadata schemamanager.SchemaMetadata `json:"metadata"`
	}
	err := json.Unmarshal(rsrcJson, &rs)
	if err != nil {
		log.Ctx(ctx).Error().Err(err).Msg("failed to unmarshal resource schema")
		return nil, validationerrors.ErrSchemaValidation
	}

	return &rs.Metadata, nil
}

func canonicalizeMetadata(rsrcJson []byte, kind string, metadata *schemamanager.SchemaMetadata) ([]byte, *schemamanager.SchemaMetadata, apperrors.Error) {
	if len(rsrcJson) == 0 {
		return nil, nil, validationerrors.ErrEmptySchema
	}

	var fullMap map[string]json.RawMessage // parse only the first level elements
	if err := json.Unmarshal(rsrcJson, &fullMap); err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to unmarshal resource schema")
	}
	var (
		rawMetadata json.RawMessage
		ok          bool
	)
	if rawMetadata, ok = fullMap["metadata"]; !ok {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("missing metadata in resource schema")
	}
	// get metadata in resource json
	var m schemamanager.SchemaMetadata
	err := json.Unmarshal(rawMetadata, &m)
	if err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to unmarshal metadata")
	}
	if metadata != nil {
		// update metadata fields with new values
		if metadata.Name != "" {
			m.Name = metadata.Name
		}
		if metadata.Catalog != "" {
			m.Catalog = metadata.Catalog
		}
		if !metadata.Variant.IsNil() {
			m.Variant = metadata.Variant
		}
		if metadata.Path != "" {
			m.Path = metadata.Path
		}
		if metadata.Description != "" {
			m.Description = metadata.Description
		}
	}

	if m.Variant.IsNil() {
		m.Variant = types.NullableStringFrom(types.DefaultVariant) // set default variant if nil
	}

	if kind == types.CollectionSchemaKind || kind == types.ParameterSchemaKind {
		canonicalizePath(kind, &m)
	}

	// marshal updated metadata back to json
	j, err := json.Marshal(m)
	if err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to marshal metadata")
	}
	fullMap["metadata"] = j

	rs, err := json.Marshal(fullMap)
	if err != nil {
		return nil, nil, validationerrors.ErrSchemaValidation.Msg("failed to marshal resource schema")
	}

	return rs, &m, nil
}

// We morph the paths here, so that these are under the correct namespace.
// Internally, namespaces are separated by pathnames at the root level. Schemas can only exist at the catalog root
// or at the root of a namespace.  However, collections are hierarchically stored.
// For a schema:
// - /my-schema or /my-namespace/my-schema is valid
// - /my-namespace/some-path/my-schema is invalid
// For a collection:
// - /my-collection or /my-namespace/my-collection is valid
// - /my-namespace/some-path/my-collection is valid
func canonicalizePath(kind string, m *schemamanager.SchemaMetadata) {
	if m == nil {
		return
	}
	if kind == types.CollectionSchemaKind || kind == types.ParameterSchemaKind {
		if m.Namespace.IsNil() {
			m.Path = "/"
		} else {
			m.Path = "/" + m.Namespace.Value
		}
	} else if kind == types.CollectionKind {
		if !m.Namespace.IsNil() {
			m.Path = path.Clean("/" + m.Namespace.String() + "/" + m.Path)
		}
	}
}
