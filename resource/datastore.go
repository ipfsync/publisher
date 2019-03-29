package resource

import (
	"strings"
	"github.com/dgraph-io/badger"
)

const dbKeySep string = "::"
type dbKey []string

func NewDbKeyFromStr(str string) dbKey {
	parts := strings.Split(str, "::")
	for i := 0; i < len(parts); i++ {
		parts[i] = strings.ReplaceAll(parts[i], "\\:\\:", "::")
	}
	return parts
}

func (k dbKey) String() string {
	escaped := make([]string)
	for _, keyPart := range k {
		escaped.append(strings.ReplaceAll(keyPart, "::", "\\:\\:"))
	}
	
	return strings.Join(escaped, "::")
}


// Datastore is a store for saving resource collections data. Including collections and their resource items.
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

// Close Datastore
func (d *Datastore) Close() error {
	return d.db.Close()
}

// UpdateCollection update collection information
func (d *Datastore) CreateOrUpdateCollection(c *Collection) error {
	err := d.db.Update(func(txn *badger.Txn) error {

		keyPrefix := IPNSAddress

		err := txn.Set([]byte(dbKey{keyPrefix, "Name"}), []byte(c.Name))
		if err != nil {
			return err
		}
		err = txn.Set([]byte("Description"), []byte(c.Description))
		if err != nil {
			return err
		}
		err = txn.Set([]byte("IPNSAddress"), []byte(c.IPNSAddress))
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (d *Datastore) ReadCollection