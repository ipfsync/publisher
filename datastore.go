package publisher

import (
	"github.com/dgraph-io/badger"
	"github.com/ipfsync/publisher/resource"
)

// Datastore is a store for saving a resource collection data. Including collection itself and resource items.
// For now it is a struct using BadgerDB. Later on it will be refactored as an interface with multiple database implements.
type Datastore struct {
	db *badger.DB
}

// NewDatastore creates a new Datastore.
func NewDatastore(dbPath string) (*Datastore, error) {
	opts := badger.DefaultOptions
	opts.Dir = dbPath
	opts.ValueDir = dbPath
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Datastore{db: db}, nil
}

func (d *Datastore) UpdateCollection(c *resource.Collection) error {

}
