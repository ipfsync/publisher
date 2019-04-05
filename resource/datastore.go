package resource

import (
	"errors"
	"strings"

	"github.com/dgraph-io/badger"
)

var (
	// ErrIPNSNotFound is returned when an IPNS is not found in Datastore.
	ErrIPNSNotFound = errors.New("IPSN not found")

	// ErrCIDNotFound is returned when a CID is not found in Datastore.
	ErrCIDNotFound = errors.New("CID not found")
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

func (d *Datastore) checkIPNS(ipns string) error {
	err := d.db.View(func(txn *badger.Txn) error {
		k := dbKey{"collection", ipns, "name"}
		_, err := txn.Get(k.Bytes())
		return err
	})
	if err == badger.ErrKeyNotFound {
		return ErrIPNSNotFound
	}
	return err
}

func (d *Datastore) checkCID(cid string) error {
	err := d.db.View(func(txn *badger.Txn) error {
		k := dbKey{"item", cid, "name"}
		_, err := txn.Get(k.Bytes())
		return err
	})
	if err == badger.ErrKeyNotFound {
		return ErrCIDNotFound
	}
	return err
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
func (d *Datastore) ReadCollection(ipns string) (*Collection, error) {
	err := d.checkIPNS(ipns)
	if err != nil {
		return nil, err
	}

	var c *Collection
	err = d.db.View(func(txn *badger.Txn) error {
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

		c = &Collection{IPNSAddress: ipns, Name: string(n), Description: string(d)}

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
	err := d.checkIPNS(ipns)
	if err != nil {
		return err
	}

	err = d.db.Update(func(txn *badger.Txn) error {
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
		pTag := append(p, "tag")
		err = d.dropPrefix(txn, pTag)
		if err != nil {
			return err
		}

		// Set new tags
		for _, t := range i.Tags {
			d.addItemTagInTxn(txn, i.CID, t)
		}

		return nil
	})
	return err
}

// ReadItem reads Item from database
func (d *Datastore) ReadItem(cid string) (*Item, error) {
	err := d.checkCID(cid)
	if err != nil {
		return nil, err
	}

	var i *Item
	err = d.db.View(func(txn *badger.Txn) error {
		p := dbKey{"item", cid}

		// Name
		item, err := txn.Get(append(p, "name").Bytes())
		if err != nil {
			return err
		}
		n, err := item.ValueCopy(nil)

		// Tags
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		pTag := append(p, "tag").Bytes()
		var dst []byte
		var tags []Tag
		for it.Seek(pTag); it.ValidForPrefix(pTag); it.Next() {
			item := it.Item()
			v, err := item.ValueCopy(dst)
			if err != nil {
				return err
			}
			tags = append(tags, NewTagFromStr(string(v)))
		}

		i = &Item{CID: cid, Name: string(n), Tags: tags}

		return nil
	})
	return i, err
}

// DelItem deletes an item by its CID.
func (d *Datastore) DelItem(cid string) error {
	item, err := d.ReadItem(cid)
	if err != nil {
		return err
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		// Remove Tag-Item relationship
		for _, t := range item.Tags {
			tagKey := dbKey{"tag", t.String(), cid}.Bytes()
			err := txn.Delete(tagKey)
			if err != nil {
				return err
			}
		}

		p := dbKey{"item", item.CID}
		err = d.dropPrefix(txn, p)
		return err
	})
	return err
}

func (d *Datastore) addItemTagInTxn(txn *badger.Txn, cid string, t Tag) error {
	itemTagKey := dbKey{"item", cid, "tag", t.String()}.Bytes()
	err := txn.Set(itemTagKey, []byte(t.String()))
	if err != nil {
		return err
	}

	tagKey := dbKey{"tag", t.String(), cid}.Bytes()
	err = txn.Set(tagKey, []byte(cid))
	if err != nil {
		return err
	}

	return nil
}

// AddItemTag adds a Tag to an Item. If the tag doesn't exist in database, it will be created.
func (d *Datastore) AddItemTag(cid string, t Tag) error {
	err := d.checkCID(cid)
	if err != nil {
		return err
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		return d.addItemTagInTxn(txn, cid, t)
	})
	return err
}

// RemoveItemTag removes a Tag from an Item.
func (d *Datastore) RemoveItemTag(cid string, t Tag) error {
	err := d.checkCID(cid)
	if err != nil {
		return err
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		itemTagKey := dbKey{"item", cid, "tag", t.String()}.Bytes()
		err := txn.Delete(itemTagKey)
		if err != nil {
			return err
		}

		tagKey := dbKey{"tag", t.String(), cid}.Bytes()
		err = txn.Delete(tagKey)
		if err != nil {
			return err
		}

		return nil
	})
	return err
}

// AddItemToCollection adds an Item to a Collection.
func (d *Datastore) AddItemToCollection(cid string, ipns string) error {
	err := d.checkCID(cid)
	if err != nil {
		return err
	}

	err = d.checkIPNS(ipns)
	if err != nil {
		return err
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		kColl := dbKey{"collection", ipns, "item", cid}
		err := txn.Set(kColl.Bytes(), []byte(cid))
		if err != nil {
			return err
		}

		kItem := dbKey{"item", cid, "collection", ipns}
		err = txn.Set(kItem.Bytes(), []byte(ipns))
		if err != nil {
			return err
		}

		return nil
	})
	return err
}
