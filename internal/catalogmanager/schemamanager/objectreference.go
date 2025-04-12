package schemamanager

import (
	"encoding/json"
	"path"
)

// We'll keep this a struct, so this is extensible in the future
type ObjectReference struct {
	Name string `json:"name"`
}

func (pr ObjectReference) String() string {
	return pr.Name
}

func (pr ObjectReference) ObjectName() string {
	return path.Base(pr.Name)
}

func (pr ObjectReference) Path() string {
	return path.Dir(pr.Name)
}

type ObjectReferences []ObjectReference

func (prs ObjectReferences) Serialize() ([]byte, error) {
	s, err := json.Marshal(prs)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func DeserializeObjectReferences(b []byte) (ObjectReferences, error) {
	prs := ObjectReferences{}
	err := json.Unmarshal(b, &prs)
	return prs, err
}
