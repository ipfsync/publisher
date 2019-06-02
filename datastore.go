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
	ErrIPNSNotFound = errors.New("IPNS not found")

	// ErrCIDNotFound is returned when a CID is not found in Datastore.
	ErrCIDNotFound = errors.New("CID not found")

	// ErrNegativeTagItemCount is returned when the value of tag::[tagStr] in Datastore is negative.
	ErrNegativeTagItemCount = errors.New("Negative tag item count")

	// ErrFolderNotExists is returned when folder doesn't exist.
	ErrFolderNotExists = errors.New("Folder doesn't exist")

	// ErrParentFolderNotExists is returned when parent folder doesn't exist.
	ErrParentFolderNotExists = errors.New("Parent folder doesn't exist")

	// ErrItemNotInFolder is returned when the item is not in the folder.
	ErrItemNotInFolder = errors.New("Item is not in the folder")

	// ErrItemInCollection is returned when the item is already in the collection.
	ErrItemInCollection = errors.New("Item is already in the collection")

	// ErrCantDelRootFolder is returned when trying to delete a root folder.
	ErrCantDelRootFolder = errors.New("Root folder can't be deleted")
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
// collections_mine::[ipns] = [ipns]
// collection::[ipns]::name
// collection::[ipns]::description
// collection::[ipns]::ismine
// collection_item::[ipns]::[cid] = [cid]
// folders::[ipns]::[folderPath] = [folderPath] # The folderPath of root folder is ""
// folder::[ipns]::[folderPath]::children = [listOfChildFolderNames]
// folder_item::[ipns]::[folderPath]::[cid] = [cid]
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

		err = txn.Set(append(p, "name").Bytes(), []byte(c.Name))
		if err != nil {
			return err
		}
		err = txn.Set(append(p, "description").Bytes(), []byte(c.Description))
		if err != nil {
			return err
		}
		var ismine string
		if c.IsMine {
			ismine = "1"
			// collections_mine::[ipns] = [ipns]
			err = txn.Set(dbKey{"collections_mine", c.IPNSAddress}.Bytes(), []byte(c.IPNSAddress))
			if err != nil {
				return err
			}
		} else {
			ismine = "0"
		}
		// collection::[ipns]::ismine
		err = txn.Set(append(p, "ismine").Bytes(), []byte(ismine))
		if err != nil {
			return err
		}

		// Create root folder
		err = d.createOrUpdateFolderInTxn(txn, &Folder{IPNSAddress: c.IPNSAddress})
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
		item, err = txn.Get(append(p, "ismine").Bytes())
		if err != nil {
			return err
		}
		ismine := false
		err = item.Value(func(val []byte) error {
			s := string(val)
			if s == "1" {
				ismine = true
			}
			return nil
		})
		if err != nil {
			return err
		}

		c = &Collection{IPNSAddress: ipns, Name: string(n), Description: string(d), IsMine: ismine}

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

		items, err := d.ReadCollectionItems(ipns)
		if err != nil {
			return err
		}

		k := dbKey{"collections", ipns}
		err = txn.Delete(k.Bytes())
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

		// Delete item-folder / item-collection relationship
		for _, v := range items {
			p := dbKey{"item_folder", v, ipns}
			err = d.dropPrefix(txn, p)
			if err != nil {
				return err
			}

			k = dbKey{"item_collection", v, ipns}
			err = txn.Delete(k.Bytes())
			if err != nil {
				return err
			}
		}

		return nil
	})
	return err
}

// TODO: ReadCollections list collections

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

		// Remove item from all folders
		it = txn.NewIterator(opts)
		p = dbKey{"folder_item"}
		for it.Seek(p.Bytes()); it.ValidForPrefix(p.Bytes()); it.Next() {
			item := it.Item()
			k := newDbKeyFromStr(string(item.Key()))
			if k[3] == cid {
				err := txn.Delete(k.Bytes())
				if err != nil {
					return err
				}
			}
		}
		it.Close()

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
		if err != nil {
			return err
		}

		p = dbKey{"item_folder", item.CID}
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
	// Check if the item is already in the collection
	exists, err := d.IsItemInCollection(cid, ipns)
	if err != nil {
		return err
	}
	if exists {
		return ErrItemInCollection
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

	if err != nil {
		return err
	}

	// Add item to root folder
	err = d.AddItemToFolder(cid, &Folder{IPNSAddress: ipns})
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
		return d.removeItemFromCollectionInTxn(txn, cid, ipns)
	})
	return err

}

func (d *Datastore) removeItemFromCollectionInTxn(txn *badger.Txn, cid string, ipns string) error {
	// Remove item from folders of collection
	var paths []string
	p := dbKey{"item_folder", cid, ipns}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)

	for it.Seek(p.Bytes()); it.ValidForPrefix(p.Bytes()); it.Next() {
		item := it.Item()
		keyStr := string(item.Key())
		key := newDbKeyFromStr(keyStr)

		paths = append(paths, key[3])
	}
	it.Close()

	// drop item_folder::[cid]::[ipns]::[folderPath] = [folderPath]
	err := d.dropPrefix(txn, p)
	if err != nil {
		return err
	}

	var k dbKey

	// folder_item::[ipns]::[folderPath]::[cid] = [cid]
	for _, v := range paths {
		k = dbKey{"folder_item", ipns, v, cid}
		err = txn.Delete(k.Bytes())
		if err != nil {
			return err
		}
	}

	k = dbKey{"collection_item", ipns, cid}
	err = txn.Delete(k.Bytes())
	if err != nil {
		return err
	}

	k = dbKey{"item_collection", cid, ipns}
	err = txn.Delete(k.Bytes())
	if err != nil {
		return err
	}

	return nil
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
		kColl := dbKey{"item_collection", cid, ipns}
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
				err := item.Value(func(val []byte) error {
					c = uint(binary.BigEndian.Uint32(val))
					return nil
				})
				if err != nil {
					return err
				}
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

// CreateOrUpdateFolder creates a new folder or updates a folder
func (d *Datastore) CreateOrUpdateFolder(folder *Folder) error {
	if folder.IPNSAddress == "" {
		panic("Invalid folder.")
	}

	err := d.checkIPNS(folder.IPNSAddress)
	if err != nil {
		return err
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		return d.createOrUpdateFolderInTxn(txn, folder)
	})

	return err
}

func (d *Datastore) createOrUpdateFolderInTxn(txn *badger.Txn, folder *Folder) error {
	k := dbKey{"folders", folder.IPNSAddress, folder.Path}
	err := txn.Set(k.Bytes(), []byte(folder.Path))
	if err != nil {
		return err
	}

	isRoot := false

	parentPath := folder.ParentPath()
	if folder.Path == "" && parentPath == "" {
		isRoot = true
	}

	if !isRoot {
		// Make sure parent exists
		err = d.assertParentInTxn(txn, folder)
		if err != nil {
			return err
		}

		// Add this folder to parent's children list
		// Parent's Children key: folder::[ipns]::[folderPath]::children
		pck := dbKey{"folder", folder.IPNSAddress, parentPath, "children"}
		item, err := txn.Get(pck.Bytes())
		var children []string
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		var buf bytes.Buffer
		if item != nil {
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			// Read children
			buf = *bytes.NewBuffer(v)
			dec := gob.NewDecoder(&buf)
			err = dec.Decode(&children)
			if err != nil {
				return err
			}
		}

		// Add folder to children
		children = append(children, folder.Path)

		// Save back
		buf.Reset()
		enc := gob.NewEncoder(&buf)
		err = enc.Encode(children)
		if err != nil {
			return err
		}

		err = txn.Set(pck.Bytes(), buf.Bytes())
		if err != nil {
			return err
		}
	}

	return nil
}

// ReadFolder reads a folder from Datastore.
func (d *Datastore) ReadFolder(ipns, path string) (*Folder, error) {
	if ipns == "" {
		panic("Invalid parameters.")
	}

	// path can be "" as a root folder

	exists, err := d.IsFolderPathExists(ipns, path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrFolderNotExists
	}

	return &Folder{Path: path, IPNSAddress: ipns}, nil
}

// IsFolderPathExists checkes if a folder exists.
func (d *Datastore) IsFolderPathExists(ipns, path string) (bool, error) {

	exists := false

	err := d.db.View(func(txn *badger.Txn) error {
		var err error
		exists, err = d.isFolderPathExistsInTxn(txn, ipns, path)
		return err
	})

	if err != nil {
		return false, err
	}
	return exists, nil
}

func (d *Datastore) isFolderPathExistsInTxn(txn *badger.Txn, ipns, path string) (bool, error) {

	err := d.checkIPNS(ipns)
	if err != nil {
		return false, err
	}

	exists := false

	k := dbKey{"folders", ipns, path}

	_, err = txn.Get(k.Bytes())
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return false, err
		}
	} else {
		exists = true
	}

	return exists, nil
}

// assertParent checks if parent of folder exists. If not, an error will be returned.
// If parent is root, it will create the root folder.
func (d *Datastore) assertParentInTxn(txn *badger.Txn, folder *Folder) error {

	if folder.ParentPath() == "" {
		// Check root folder existence
		rootExists, err := d.isFolderPathExistsInTxn(txn, folder.IPNSAddress, "")
		if err != nil {
			if err == ErrFolderNotExists {
				rootExists = false
			} else {
				return err
			}
		}

		// Create root folder if not exists in Datastore
		if !rootExists {
			root := &Folder{IPNSAddress: folder.IPNSAddress}
			err = d.createOrUpdateFolderInTxn(txn, root)
			if err != nil {
				return err
			}
		}

	} else {
		exists, err := d.isFolderPathExistsInTxn(txn, folder.IPNSAddress, folder.ParentPath())
		if err != nil {
			return err
		}
		if !exists {
			return ErrParentFolderNotExists
		}

	}

	return nil
}

// AddItemToFolder adds an item to a folder
func (d *Datastore) AddItemToFolder(cid string, folder *Folder) error {
	err := d.checkCID(cid)
	if err != nil {
		return err
	}

	exists, err := d.IsFolderPathExists(folder.IPNSAddress, folder.Path)
	if err != nil {
		return err
	}
	if !exists {
		return ErrFolderNotExists
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		// item_folder::[cid]::[ipns]::[folderPath] = [folderPath]
		k := dbKey{"item_folder", cid, folder.IPNSAddress, folder.Path}
		err := txn.Set(k.Bytes(), []byte(folder.Path))
		if err != nil {
			return err
		}

		// folder_item::[ipns]::[folderPath]::[cid] = [cid]
		k = dbKey{"folder_item", folder.IPNSAddress, folder.Path, cid}
		err = txn.Set(k.Bytes(), []byte(cid))
		if err != nil {
			return err
		}

		return nil

	})

	return err
}

// RemoveItemFromFolder removes item from a folder
func (d *Datastore) RemoveItemFromFolder(cid string, folder *Folder) error {
	err := d.checkCID(cid)
	if err != nil {
		return err
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		// item_folder::[cid]::[ipns]::[folderPath] = [folderPath]
		k := dbKey{"item_folder", cid, folder.IPNSAddress, folder.Path}
		_, err := txn.Get(k.Bytes())
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return ErrItemNotInFolder
			}
			return err
		}

		err = txn.Delete(k.Bytes())
		if err != nil {
			return err
		}

		// folder_item::[ipns]::[folderPath]::[cid] = [cid]
		k = dbKey{"folder_item", folder.IPNSAddress, folder.Path, cid}
		err = txn.Delete(k.Bytes())
		if err != nil {
			return err
		}

		return nil
	})

	return err
}

// IsItemInFolder checks if an item is in a folder
func (d *Datastore) IsItemInFolder(cid string, folder *Folder) (bool, error) {
	var inFolder bool
	err := d.db.View(func(txn *badger.Txn) error {
		var err error
		inFolder, err = d.isItemInFolderInTxn(txn, cid, folder)
		return err
	})

	return inFolder, err
}

func (d *Datastore) isItemInFolderInTxn(txn *badger.Txn, cid string, folder *Folder) (bool, error) {
	err := d.checkCID(cid)
	if err != nil {
		return false, err
	}

	exists, err := d.isFolderPathExistsInTxn(txn, folder.IPNSAddress, folder.Path)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, ErrFolderNotExists
	}

	var inFolder bool
	k := dbKey{"item_folder", cid, folder.IPNSAddress, folder.Path}
	_, err = txn.Get(k.Bytes())

	if err == nil {
		inFolder = true
	} else if err == badger.ErrKeyNotFound {
		err = nil
	}

	return inFolder, err
}

// ReadFolderItems returns all items' CID in a folder
func (d *Datastore) ReadFolderItems(folder *Folder) ([]string, error) {
	exists, err := d.IsFolderPathExists(folder.IPNSAddress, folder.Path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrFolderNotExists
	}

	var items []string
	err = d.db.View(func(txn *badger.Txn) error {
		p := dbKey{"folder_item", folder.IPNSAddress, folder.Path}
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(p.Bytes()); it.ValidForPrefix(p.Bytes()); it.Next() {
			item := it.Item()
			keyStr := string(item.Key())
			key := newDbKeyFromStr(keyStr)

			items = append(items, key[3])
		}

		return nil
	})

	return items, err
}

// ReadCollectionItems returns all items' CID in a collection
func (d *Datastore) ReadCollectionItems(ipns string) ([]string, error) {
	err := d.checkIPNS(ipns)
	if err != nil {
		return nil, err
	}

	var items []string
	err = d.db.View(func(txn *badger.Txn) error {
		p := dbKey{"collection_item", ipns}
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(p.Bytes()); it.ValidForPrefix(p.Bytes()); it.Next() {
			item := it.Item()
			keyStr := string(item.Key())
			key := newDbKeyFromStr(keyStr)

			items = append(items, key[2])
		}

		return nil
	})

	return items, err
}

// ReadFolderChildren returns all children (sub-folders) in a folder
func (d *Datastore) ReadFolderChildren(folder *Folder) ([]string, error) {
	exists, err := d.IsFolderPathExists(folder.IPNSAddress, folder.Path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrFolderNotExists
	}

	var children []string
	err = d.db.View(func(txn *badger.Txn) error {
		k := dbKey{"folder", folder.IPNSAddress, folder.Path, "children"}
		i, err := txn.Get(k.Bytes())
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		if i != nil {
			err := i.Value(func(val []byte) error {
				buf := bytes.NewBuffer(val)
				dec := gob.NewDecoder(buf)
				err = dec.Decode(&children)
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}

		}

		return nil
	})

	return children, err
}

// DelFolder deletes a folder and all its children folders. It also remove relationships with items.
// Items won't be deleted. If an item doesn't belong to any folder of the collection, it will be removed from the collection.
func (d *Datastore) DelFolder(folder *Folder) error {
	if folder.Path == "" {
		return ErrCantDelRootFolder
	}

	exists, err := d.IsFolderPathExists(folder.IPNSAddress, folder.Path)
	if err != nil {
		return err
	}
	if !exists {
		return ErrFolderNotExists
	}

	err = d.db.Update(func(txn *badger.Txn) error {

		// Delete folder itself
		err := d.delFolderInTxn(txn, folder)
		if err != nil {
			return err
		}

		// Remove folder from parent's children list
		pck := dbKey{"folder", folder.IPNSAddress, folder.ParentPath(), "children"}
		item, err := txn.Get(pck.Bytes())
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		if item != nil {
			var pChildren []string
			var buf bytes.Buffer
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			// Read children
			buf = *bytes.NewBuffer(v)
			dec := gob.NewDecoder(&buf)
			err = dec.Decode(&pChildren)
			if err != nil {
				return err
			}

			// Remove folder from children
			j := 0
			for _, child := range pChildren {
				if child != folder.Path {
					pChildren[j] = child
					j++
				}
			}
			pChildren = pChildren[:j]

			// Save back
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			err = enc.Encode(pChildren)
			if err != nil {
				return err
			}

			err = txn.Set(pck.Bytes(), buf.Bytes())
			if err != nil {
				return err
			}

		}

		return nil
	})

	return err
}

func (d *Datastore) delFolderInTxn(txn *badger.Txn, folder *Folder) error {

	exists, err := d.IsFolderPathExists(folder.IPNSAddress, folder.Path)
	if err != nil {
		return err
	}
	if !exists {
		// Just skip if folder isn't exist
		return nil
	}

	children, err := d.ReadFolderChildren(folder)
	if err != nil {
		return err
	}
	// Recursively delete children folder
	for _, child := range children {
		err := d.delFolderInTxn(txn, &Folder{IPNSAddress: folder.IPNSAddress, Path: child})
		if err != nil {
			return err
		}
	}

	items, err := d.ReadFolderItems(folder)
	if err != nil {
		return err
	}

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	// item_folder::[cid]::[ipns]::[folderPath]
	for _, cid := range items {
		k := dbKey{"item_folder", cid, folder.IPNSAddress, folder.Path}
		err := txn.Delete(k.Bytes())
		if err != nil {
			return err
		}

		// Check if the item belongs to any other folders of the collection.
		// If not, remove it from collection.
		p := dbKey{"item_folder", cid, folder.IPNSAddress}
		it := txn.NewIterator(opts)

		inFolder := false
		for it.Seek(p.Bytes()); it.ValidForPrefix(p.Bytes()); it.Next() {
			inFolder = true
			break
		}
		it.Close()

		if !inFolder {
			err = d.removeItemFromCollectionInTxn(txn, cid, folder.IPNSAddress)
			if err != nil {
				it.Close()
				return err
			}
		}
	}

	// folder_item::[ipns]::[folderPath]::[cid]
	p := dbKey{"folder_item", folder.IPNSAddress, folder.Path}
	err = d.dropPrefix(txn, p)
	if err != nil {
		return err
	}

	// folder::[ipns]::[folderPath]
	p = dbKey{"folder", folder.IPNSAddress, folder.Path}
	err = d.dropPrefix(txn, p)
	if err != nil {
		return err
	}

	// folders::[ipns]::[folderPath]
	k := dbKey{"folders", folder.IPNSAddress, folder.Path}
	err = txn.Delete(k.Bytes())
	if err != nil {
		return err
	}

	return nil

}

// MoveOrCopyItem moves or copies an item from a folder to another folder
func (d *Datastore) MoveOrCopyItem(cid string, folderFrom, folderTo *Folder, copy bool) error {
	err := d.checkCID(cid)
	if err != nil {
		return err
	}

	exists, err := d.IsItemInFolder(cid, folderFrom)
	if err != nil {
		return err
	}
	if !exists {
		return ErrItemNotInFolder
	}

	exists, err = d.IsFolderPathExists(folderTo.IPNSAddress, folderTo.Path)
	if err != nil {
		return err
	}
	if !exists {
		return ErrFolderNotExists
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		return d.moveOrCopyItemInTxn(txn, cid, folderFrom, folderTo, copy)
	})

	return nil
}

func (d *Datastore) moveOrCopyItemInTxn(txn *badger.Txn, cid string, folderFrom, folderTo *Folder, copy bool) error {
	err := d.checkCID(cid)
	if err != nil {
		return err
	}

	exists, err := d.isItemInFolderInTxn(txn, cid, folderFrom)
	if err != nil {
		return err
	}
	if !exists {
		// Just skip if folder isn't exist
		return nil
	}

	exists, err = d.isFolderPathExistsInTxn(txn, folderTo.IPNSAddress, folderTo.Path)
	if err != nil {
		return err
	}
	if !exists {
		// Just skip if folder isn't exist
		return nil
	}

	// Copy folder_item::[ipns]::[folderPath]::[cid]
	k := dbKey{"folder_item", folderTo.IPNSAddress, folderTo.Path, cid}
	err = txn.Set(k.Bytes(), []byte(cid))
	if err != nil {
		return err
	}

	if !copy {
		k = dbKey{"folder_item", folderFrom.IPNSAddress, folderFrom.Path, cid}
		err = txn.Delete(k.Bytes())
		if err != nil {
			return err
		}
	}

	// Copy item_folder::[cid]::[ipns]::[folderPath]
	k = dbKey{"item_folder", cid, folderTo.IPNSAddress, folderTo.Path}
	err = txn.Set(k.Bytes(), []byte(folderTo.Path))
	if err != nil {
		return err
	}

	if !copy {
		k = dbKey{"item_folder", cid, folderFrom.IPNSAddress, folderFrom.Path, cid}
		err = txn.Delete(k.Bytes())
		if err != nil {
			return err
		}
	}

	if folderFrom.IPNSAddress != folderTo.IPNSAddress {
		// Different collection. Add item to the To collection
		// collection_item::[ipns]::[cid]
		k = dbKey{"collection_item", folderTo.IPNSAddress, cid}
		err = txn.Set(k.Bytes(), []byte(cid))
		if err != nil {
			return err
		}
		// item_collection::[cid]::[ipns]
		k = dbKey{"item_collection", cid, folderTo.IPNSAddress}
		err = txn.Set(k.Bytes(), []byte(folderTo.IPNSAddress))
		if err != nil {
			return err
		}

		if !copy {
			// Remove item from old collection

			// collection_item::[ipns]::[cid]
			k = dbKey{"collection_item", folderFrom.IPNSAddress, cid}
			err = txn.Delete(k.Bytes())
			if err != nil {
				return err
			}
			// item_collection::[cid]::[ipns]
			k = dbKey{"item_collection", cid, folderFrom.IPNSAddress}
			err = txn.Delete(k.Bytes())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// MoveOrCopyFolder moves or copies a folder to destination
func (d *Datastore) MoveOrCopyFolder(folderFrom, folderTo *Folder, copy bool) error {

	exists, err := d.IsFolderPathExists(folderFrom.IPNSAddress, folderFrom.Path)
	if err != nil {
		return err
	}
	if !exists {
		return ErrFolderNotExists
	}

	err = d.checkIPNS(folderTo.IPNSAddress)
	if err != nil {
		return err
	}

	err = d.db.Update(func(txn *badger.Txn) error {
		return d.copyFolderInTxn(txn, folderFrom, folderTo)
	})
	if err != nil {
		return err
	}

	if !copy {
		// Moving folder. Delete from folder
		err = d.DelFolder(folderFrom)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Datastore) copyFolderInTxn(txn *badger.Txn, folderFrom, folderTo *Folder) error {

	// Copy / move folder
	folderToExists, err := d.IsFolderPathExists(folderTo.IPNSAddress, folderTo.Path)
	if err != nil {
		return err
	}

	if !folderToExists {
		err = d.createOrUpdateFolderInTxn(txn, folderTo)
		if err != nil {
			return err
		}
	}

	// Copy / move items in folder
	cids, err := d.ReadFolderItems(folderFrom)
	if err != nil {
		return err
	}

	for _, cid := range cids {
		err := d.moveOrCopyItemInTxn(txn, cid, folderFrom, folderTo, true)
		if err != nil {
			return err
		}
	}

	// Copy / move children folder
	children, err := d.ReadFolderChildren(folderFrom)
	for _, child := range children {
		subFromFolder := &Folder{IPNSAddress: folderFrom.IPNSAddress, Path: child}
		subToPath := folderTo.Path + "/" + subFromFolder.Basename()
		subToFolder := &Folder{IPNSAddress: folderTo.IPNSAddress, Path: subToPath}

		err := d.copyFolderInTxn(txn, subFromFolder, subToFolder)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Datastore) IsCollectionEmpty(ipns string) (bool, error) {
	err := d.checkIPNS(ipns)
	if err != nil {
		return true, err
	}

	empty := true
	p := dbKey{"collection_item", ipns}
	err = d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Seek(p.Bytes())

		if it.ValidForPrefix(p.Bytes()) {
			empty = false
		}

		return nil
	})

	return empty, err
}

// TODO: FilterItems() SearchItems()
// func (d *Datastore) FilterItems(tags []Tag, ipns string) ([]string, error) {

// }
