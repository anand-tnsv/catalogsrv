package catalog

import (
	"github.com/google/uuid"
)

func NewCatalog() *Catalog {
	return &Catalog{
		Id: uuid.New(),
	}
}
