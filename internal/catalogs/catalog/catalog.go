package catalog

import "github.com/google/uuid"

type Catalog struct {
	Id          uuid.UUID
	Name        string
	Description string
}
