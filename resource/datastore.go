package resource

import (
	"strings"

	"github.com/dgraph-io/badger"
)

const dbKeySep string = "::"

type dbKey []string

func newDbKeyFromStr(str string) dbKey {
	parts := strings.Split(str, "::")
	for i := 0; i < len(parts); i++ {
		parts[i] = strings.ReplaceAll(parts[i], "\\:\\:", "::")
	}
	return parts
}

func (k dbKey) String() string {
	var escaped []string
	for _, keyPart := range k {
		escaped = append(escaped, strings.ReplaceAll(keyPart, "::", "\\:\\:"))
	}

	return strings.Join(escaped, "::")
}

func (k dbKey) Bytes() []byte {
	return []byte(k.String())
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

// CreateOrUpdateCollection update collection information
func (d *Datastore) CreateOrUpdateCollection(c *Collection) error {
	err := d.db.Update(func(txn *badger.Txn) error {

		p := dbKey{"collection", c.IPNSAddress}

		err := txn.Set(append(p, "name").Bytes(), []byte(c.Name))
		if err != nil {
			return err
		}
		err = txn.Set(append(p, "description").Bytes(), []byte(c.Description))
		if err != nil {
			return err
		}

		return nil
	})
	return err
}

// ReadCollection reads Collection data from database.
func (d *Datastore) ReadCollection(ipns string) (Collection, error) {
	var c Collection
	err := d.db.View(func(txn *badger.Txn) error {
		p := dbKey{"collection", ipns}

		item, err := txn.Get(append(p, "name").Bytes())
		if err != nil {
			return err
		}
		n, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		item, err = txn.Get(append(p, "description").Bytes())
		if err != nil {
			return err
		}
		d, err := item.ValueCopy(n)
		if err != nil {
			return err
		}

		c = Collection{IPNSAddress: ipns, Name: string(n), Description: string(d)}

		return nil
	})

	return c, err
}

func (d *Datastore) dropPrefix(txn *badger.Txn, prefix dbKey) error {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek(prefix.Bytes()); it.ValidForPrefix(prefix.Bytes()); it.Next() {
		item := it.Item()
		err := txn.Delete(item.Key())
		if err != nil {
			return err
		}
	}

	return nil
}

// DelCollection deletes a collection from datastore.
func (d *Datastore) DelCollection(ipns string) error {
	err := d.db.Update(func(txn *badger.Txn) error {
		prefix := dbKey{"collection", ipns}

		return d.dropPrefix(txn, prefix)
	})
	return err
}

// CreateOrUpdateItem update collection information
func (d *Datastore) CreateOrUpdateItem(i *Item) error {
	err := d.db.Update(func(txn *badger.Txn) error {

		p := dbKey{"item", i.CID}

		err := txn.Set(append(p, "name").Bytes(), []byte(i.Name))
		if err != nil {
			return err
		}

		// Delete old tags
		prefix := append(p, "tag")
		err = d.dropPrefix(txn, prefix)
		if err != nil {
			return err
		}

		// Set new tags
		for _, t := range i.Tags {
			return d.addItemTagInTxn(txn, i.CID, t)
		}

		return nil
	})
	return err
}

func (d *Datastore) addItemTagInTxn(txn *badger.Txn, CID string, t Tag) error {
	itemTagKey := dbKey{"item", CID, "tag", t.String()}.Bytes()
	err := txn.Set(itemTagKey, []byte(t.String()))
	if err != nil {
		return err
	}

	tagKey := dbKey{"tag", t.String(), CID}.Bytes()
	err = txn.Set(tagKey, []byte(CID))
	if err != nil {
		return err
	}

	return nil
}

// AddItemTag adds a Tag to an Item. If the tag doesn't exist in database, it will be created.
func (d *Datastore) AddItemTag(CID string, t Tag) error {
	err := d.db.Update(func(txn *badger.Txn) error {
		return d.addItemTagInTxn(txn, CID, t)
	})
	return err
}

// RemoveItemTag removes a Tag from an Item.
func (d *Datastore) RemoveItemTag(CID string, t Tag) error {
	err := d.db.Update(func(txn *badger.Txn) error {
		itemTagKey := dbKey{"item", CID, "tag", t.String()}.Bytes()
		err := txn.Delete(itemTagKey)
		if err != nil {
			return err
		}

		tagKey := dbKey{"tag", t.String(), CID}.Bytes()
		err = txn.Delete(tagKey)
		if err != nil {
			return err
		}

		return nil
	})
	return err
}
