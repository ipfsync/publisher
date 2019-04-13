package resource

import (
	"errors"
	"strings"

	"encoding/binary"

	"bytes"
	"encoding/gob"

	"github.com/dgraph-io/badger"
)

var (
	// ErrIPNSNotFound is returned when an IPNS is not found in Datastore.
	ErrIPNSNotFound = errors.New("IPSN not found")

	// ErrCIDNotFound is returned when a CID is not found in Datastore.
	ErrCIDNotFound = errors.New("CID not found")

	// ErrNegativeTagItemCount is returned when the value of tag::[tagStr] in Datastore is negative.
	ErrNegativeTagItemCount = errors.New("Negative tag item count")

	// ErrParentFolderNotExists is returned when parent folder doesn't exist.
	ErrParentFolderNotExists = errors.New("Parent folder doesn't exist")
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

func (k dbKey) IsEmpty() bool {
	return len(k) == 0
}

// Datastore is a store for saving resource collections data. Including collections and their resource items.
// For now it is a struct using BadgerDB. Later on it will be refactored as an interface with multiple database implements.
// Key-Values:
//
// collections::[ipns] = [ipns]
// collection::[ipns]::name
// collection::[ipns]::description
// collection_item::[ipns]::[cid] = [cid]
// folders::[ipns]::[folderPath] = [folderPath]
// folder::[ipns]::[folderPath]::count = [itemCount]
// folder::[ipns]::[folderPath]::children = [listOfChildFolderNames]
// folder::[ipns]::[folderPath]::items = [listOfItemsFolderContains]
// items::[cid] = [cid]
// item::[cid]::name
// item_collection::[cid]::[ipns] = [ipns]
// item_tag::[cid]::[tagStr] = [tagStr]
// item_folder::[cid]::[ipns]::[folderPath] = [folderPath]
// tags::[tagStr] = [tagStr]
// tag::[tagStr].count = [itemCount]
// tag_item::[tagStr]::[cid] = [cid]
type Datastore struct {
	db *badger.DB
}

// NewDatastore creates a new Datastore.
func NewDatastore(dbPath string) (*Datastore, error) {
	if dbPath == "" {
		panic("Invalid dbPath")
	}

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
	if ipns == "" {
		panic("Invalid ipns.")
	}

	err := d.db.View(func(txn *badger.Txn) error {
		k := dbKey{"collections", ipns}
		_, err := txn.Get(k.Bytes())
		return err
	})
	if err == badger.ErrKeyNotFound {
		return ErrIPNSNotFound
	}
	return err
}

func (d *Datastore) checkCID(cid string) error {
	if cid == "" {
		panic("Invalid cid.")
	}

	err := d.db.View(func(txn *badger.Txn) error {
		k := dbKey{"items", cid}
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
	if c.Name == "" || c.IPNSAddress == "" {
		panic("Invalid parameters.")
	}

	// TODO: IPNS Address validate

	err := d.db.Update(func(txn *badger.Txn) error {

		p := dbKey{"collections", c.IPNSAddress}
		err := txn.Set(p.Bytes(), []byte(c.IPNSAddress))
		if err != nil {
			return err
		}

		p = dbKey{"collection", c.IPNSAddress}

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
	if prefix.IsEmpty() {
		panic("Empty prefix.")
	}

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	var dst []byte
	for it.Seek(prefix.Bytes()); it.ValidForPrefix(prefix.Bytes()); it.Next() {
		item := it.Item()
		err := txn.Delete(item.KeyCopy(dst))
		if err != nil {
			return err
		}
	}

	return nil
}

// DelCollection deletes a collection from datastore.
// Deleting a collection won't delete items that belongs to the collection.
func (d *Datastore) DelCollection(ipns string) error {
	err := d.checkIPNS(ipns)
	if err != nil {
		return err
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		k := dbKey{"collections", ipns}
		err := txn.Delete(k.Bytes())
		if err != nil {
			return err
		}

		prefix := dbKey{"collection", ipns}
		err = d.dropPrefix(txn, prefix)
		if err != nil {
			return err
		}

		prefix = dbKey{"collection_item", ipns}
		err = d.dropPrefix(txn, prefix)
		if err != nil {
			return err
		}

		prefix = dbKey{"folders", ipns}
		err = d.dropPrefix(txn, prefix)
		if err != nil {
			return err
		}

		prefix = dbKey{"folder", ipns}
		err = d.dropPrefix(txn, prefix)
		if err != nil {
			return err
		}

		// TODO: Delete item-folder relationship

		return nil
	})
	return err
}

// CreateOrUpdateItem update collection information
func (d *Datastore) CreateOrUpdateItem(i *Item) error {
	if i.CID == "" || i.Name == "" {
		panic("Invalid parameters.")
	}

	iOld, _ := d.ReadItem(i.CID)

	err := d.db.Update(func(txn *badger.Txn) error {

		k := dbKey{"items", i.CID}
		err := txn.Set(k.Bytes(), []byte(i.CID))
		if err != nil {
			return err
		}

		k = dbKey{"item", i.CID, "name"}
		err = txn.Set(k.Bytes(), []byte(i.Name))
		if err != nil {
			return err
		}

		if iOld != nil {
			// Delete old item_tag::[cid]::[tagStr]
			k = dbKey{"item_tag", i.CID}
			err = d.dropPrefix(txn, k)
			if err != nil {
				return err
			}

			// Delete old tag_item::[tagStr]::[cid]
			for _, t := range iOld.Tags {
				tagKey := dbKey{"tag_item", t.String(), i.CID}.Bytes()
				err = txn.Delete(tagKey)
				if err != nil {
					return err
				}

				err = d.updateTagItemCount(txn, t, -1)
				if err != nil {
					return err
				}
			}
		}

		// Set new tags
		for _, t := range i.Tags {
			err = d.addItemTagInTxn(txn, i.CID, t)
			if err != nil {
				return err
			}
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
		k := dbKey{"item", cid, "name"}

		// Name
		item, err := txn.Get(k.Bytes())
		if err != nil {
			return err
		}
		n, err := item.ValueCopy(nil)

		// Tags
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		pTag := dbKey{"item_tag", cid}
		var tags []Tag
		for it.Seek(pTag.Bytes()); it.ValidForPrefix(pTag.Bytes()); it.Next() {
			item := it.Item()
			kTag := newDbKeyFromStr(string(item.Key()))
			tags = append(tags, NewTagFromStr(kTag[len(kTag)-1]))
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
			tagKey := dbKey{"tag_item", t.String(), cid}.Bytes()
			err := txn.Delete(tagKey)
			if err != nil {
				return err
			}
			// Reduce tag::[tagStr] count
			err = d.updateTagItemCount(txn, t, -1)
			if err != nil {
				return err
			}
		}

		// Remove Items from all Collections
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		p := dbKey{"collection_item"}
		for it.Seek(p.Bytes()); it.ValidForPrefix(p.Bytes()); it.Next() {
			item := it.Item()
			k := newDbKeyFromStr(string(item.Key()))
			if k[2] == cid {
				err := txn.Delete(k.Bytes())
				if err != nil {
					return err
				}
			}
		}
		it.Close()

		// TODO: Remove item from folders

		p = dbKey{"items", item.CID}
		err = d.dropPrefix(txn, p)
		if err != nil {
			return err
		}

		p = dbKey{"item", item.CID}
		err = d.dropPrefix(txn, p)
		if err != nil {
			return err
		}

		p = dbKey{"item_collection", item.CID}
		err = d.dropPrefix(txn, p)
		if err != nil {
			return err
		}

		p = dbKey{"item_tag", item.CID}
		err = d.dropPrefix(txn, p)
		return err
	})
	return err
}

func (d *Datastore) addItemTagInTxn(txn *badger.Txn, cid string, t Tag) error {
	if cid == "" || t.IsEmpty() {
		panic("Invalid parameters.")
	}

	tagExist := false

	itemTagKey := dbKey{"item_tag", cid, t.String()}.Bytes()
	// Check existence of the item tag
	_, err := txn.Get(itemTagKey)
	if err != badger.ErrKeyNotFound {
		tagExist = true
	}
	err = txn.Set(itemTagKey, []byte(t.String()))
	if err != nil {
		return err
	}

	tagItemKey := dbKey{"tag_item", t.String(), cid}.Bytes()
	_, err = txn.Get(tagItemKey)
	if (err != badger.ErrKeyNotFound && tagExist == false) ||
		(err == badger.ErrKeyNotFound && tagExist == true) {
		panic("Database integrity error. Maybe you have duplicate tags for an item?")
	}
	err = txn.Set(tagItemKey, []byte(cid))
	if err != nil {
		return err
	}

	if tagExist == false {

		tagsKey := dbKey{"tags", t.String()}.Bytes()
		err = txn.Set(tagsKey, []byte(t.String()))
		if err != nil {
			return err
		}

		err = d.updateTagItemCount(txn, t, 1)
		if err != nil {
			return err
		}
	}

	return nil
}

// updateTagItemCount update count of a tag
func (d *Datastore) updateTagItemCount(txn *badger.Txn, t Tag, diff int) error {
	if t.IsEmpty() || diff == 0 {
		panic("Invalid parameters.")
	}

	tagKey := dbKey{"tag", t.String(), "count"}.Bytes()
	item, err := txn.Get(tagKey)
	var c int
	cBytes := make([]byte, 4)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			c = 1
		} else {
			return err
		}
	} else {
		cBytes, err = item.ValueCopy(cBytes)
		if err != nil {
			return err
		}

		c = int(binary.BigEndian.Uint32(cBytes)) + diff

		if c < 0 {
			return ErrNegativeTagItemCount
		}
	}
	binary.BigEndian.PutUint32(cBytes, uint32(c))
	err = txn.Set(tagKey, cBytes)
	if err != nil {
		return err
	}

	// No item is referring this tag, delete it
	if c == 0 {
		p := dbKey{"tags", t.String()}
		err = d.dropPrefix(txn, p)
		if err != nil {
			return err
		}
		p = dbKey{"tag", t.String()}
		err = d.dropPrefix(txn, p)
		if err != nil {
			return err
		}
	}

	return nil
}

// AddItemTag adds a Tag to an Item. If the tag doesn't exist in database, it will be created.
func (d *Datastore) AddItemTag(cid string, t Tag) error {
	if t.IsEmpty() || cid == "" {
		panic("Invalid parameters.")
	}

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
	if t.IsEmpty() || cid == "" {
		panic("Invalid parameters.")
	}

	err := d.checkCID(cid)
	if err != nil {
		return err
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		itemTagKey := dbKey{"item_tag", cid, t.String()}.Bytes()
		err := txn.Delete(itemTagKey)
		if err != nil {
			return err
		}

		tagKey := dbKey{"tag_item", t.String(), cid}.Bytes()
		err = txn.Delete(tagKey)
		if err != nil {
			return err
		}

		// Reduce tag::[tagStr] count
		err = d.updateTagItemCount(txn, t, -1)
		if err != nil {
			return err
		}

		return nil
	})
	return err
}

// HasTag checks if an Item has a Tag.
func (d *Datastore) HasTag(cid string, t Tag) (bool, error) {
	if t.IsEmpty() || cid == "" {
		panic("Invalid parameters.")
	}

	item, err := d.ReadItem(cid)
	if err != nil {
		return false, err
	}

	exists := false
	for _, tag := range item.Tags {
		if tag.Equals(t) {
			exists = true
			break
		}
	}

	return exists, nil
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
		kColl := dbKey{"collection_item", ipns, cid}
		err := txn.Set(kColl.Bytes(), []byte(cid))
		if err != nil {
			return err
		}

		kItem := dbKey{"item_collection", cid, ipns}
		err = txn.Set(kItem.Bytes(), []byte(ipns))
		if err != nil {
			return err
		}

		return nil
	})
	return err
}

// RemoveItemFromCollection removes an Item from a Collection.
func (d *Datastore) RemoveItemFromCollection(cid string, ipns string) error {
	err := d.checkCID(cid)
	if err != nil {
		return err
	}

	err = d.checkIPNS(ipns)
	if err != nil {
		return err
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		kColl := dbKey{"collection_item", ipns, cid}
		err := txn.Delete(kColl.Bytes())
		if err != nil {
			return err
		}

		kItem := dbKey{"item_collection", cid, ipns}
		err = txn.Delete(kItem.Bytes())
		if err != nil {
			return err
		}

		return nil
	})
	return err

}

// IsItemInCollection checks if an Item belongs to a Collection.
func (d *Datastore) IsItemInCollection(cid string, ipns string) (bool, error) {
	err := d.checkCID(cid)
	if err != nil {
		return false, err
	}

	err = d.checkIPNS(ipns)
	if err != nil {
		return false, err
	}

	var exist bool
	err = d.db.View(func(txn *badger.Txn) error {
		kColl := dbKey{"collection_item", ipns, cid}
		_, err := txn.Get(kColl.Bytes())

		if err == nil {
			exist = true
		} else if err == badger.ErrKeyNotFound {
			err = nil
		}
		return err
	})

	return exist, err
}

// SearchTags searches all available tags with prefix
func (d *Datastore) SearchTags(prefix string) ([]Tag, error) {
	if prefix == "" {
		panic("Invalid prefix.")
	}

	keys := make(map[string]bool)

	err := d.db.View(func(txn *badger.Txn) error {
		p := dbKey{"tags", prefix}
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(p.Bytes()); it.ValidForPrefix(p.Bytes()); it.Next() {
			item := it.Item()
			keyStr := string(item.Key())
			key := newDbKeyFromStr(keyStr)

			keys[key[1]] = true
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	var tags []Tag
	for k := range keys {
		tags = append(tags, NewTagFromStr(k))
	}

	return tags, nil
}

// ReadTagItemCount returns []uint that are item counts of []Tag
func (d *Datastore) ReadTagItemCount(tags []Tag) ([]uint, error) {
	if len(tags) == 0 {
		panic("Invalid tags.")
	}

	var counts []uint

	err := d.db.View(func(txn *badger.Txn) error {
		for _, t := range tags {
			if t.IsEmpty() {
				panic("Invalid tag.")
			}

			k := dbKey{"tag", t.String(), "count"}
			item, err := txn.Get(k.Bytes())
			var c uint
			if err != nil {
				// If a tag is not found in db, count 0 for it
				if err != badger.ErrKeyNotFound {
					return err
				}
			} else {
				v, err := item.Value()
				if err != nil {
					return err
				}
				c = uint(binary.BigEndian.Uint32(v))
			}
			counts = append(counts, c)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return counts, nil
}

// CreateFolder creates a new folder
func (d *Datastore) CreateFolder(folder *Folder) error {
	if folder.Path == "" || folder.IPNSAddress == "" {
		panic("Invalid folder.")
	}

	err := d.checkIPNS(folder.IPNSAddress)
	if err != nil {
		return err
	}

	parts := strings.Split(folder.Path, "/")
	partsLen := len(parts)
	var parentPath string
	if partsLen != 1 {
		parentPath = strings.Join(parts[:partsLen-1], "/")
		// Make sure parent exists
		_, err := d.ReadFolder(folder.IPNSAddress, parentPath)
		if err != nil {
			return ErrParentFolderNotExists
		}
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		p := dbKey{"folders", folder.IPNSAddress, folder.Path}

		// collection_folder::[ipns]::[folderPath] = [itemCount]
		cBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(cBytes, uint32(0))
		err := txn.Set(p.Bytes(), cBytes)
		if err != nil {
			return err
		}

		if parentPath != "" {
			// Add this folder to parent's children list
			// Parent's Children key: collection_folder::[ipns]::[folderPath]::children
			pck := dbKey{"collection_folder", folder.IPNSAddress, parentPath, "children"}
			item, err := txn.Get(pck.Bytes())
		}

		return nil
	})

	return err
}

// ReadFolder reads a folder from Datastore.
func (d *Datastore) ReadFolder(ipns, path string) (*Folder, error) {
	if ipns == "" || path == "" {
		panic("Invalid parameters.")
	}

	var f *Folder

	err := d.db.View(func(txn *badger.Txn) error {
		k := dbKey{"collection_folder", ipns, path}

		// Make sure folder exists in Datastore
		_, err := txn.Get(k.Bytes())
		if err != nil {
			return err
		}

		item, err := txn.Get(append(k, "children").Bytes())
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		var children []string
		if item != nil {
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			buf := bytes.NewBuffer(v)
			dec := gob.NewDecoder(buf)
			err = dec.Decode(&children)
			if err != nil {
				return err
			}
		}

		parts := strings.Split(path, "/")
		partsLen := len(parts)
		var parentPath string
		if partsLen != 1 {
			parentPath = strings.Join(parts[:partsLen-1], "/")
		}

		f = &Folder{Path: path, IPNSAddress: ipns, Parent: parentPath, Children: children}

		return nil
	})

	return f, err
}

// TODO: IsFolderExists() MoveFolder()

// TODO: FilterItems() SearchItems()
// func (d *Datastore) FilterItems(tags []Tag, ipns string) ([]string, error) {

// }
