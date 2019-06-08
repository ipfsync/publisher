package resource

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thoas/go-funk"
)

var testdataDir = filepath.Join(".", "testdata")
var dbPath = filepath.Join(testdataDir, "test.db")

func TestMain(m *testing.M) {
	// Ensure testdata dir exists
	_ = os.MkdirAll(testdataDir, os.ModePerm)
	// Remove old testing datastore
	_ = os.RemoveAll(dbPath)
	retCode := m.Run()
	// Cleanup
	_ = os.RemoveAll(dbPath)
	os.Exit(retCode)
}

func TestDbKey(t *testing.T) {
	want := "hello::world::ab\\:\\:c"
	dbKey := newDbKeyFromStr(want)
	if dbKey[0] != "hello" && dbKey[1] != "world" && dbKey[2] != "ab\\:\\:c" {
		t.Errorf("dbKey string = %s; want %s", dbKey, want)
	}

	want2 := []byte(want)
	if !bytes.Equal(dbKey.Bytes(), want2) {
		t.Error("dbKey Bytes() not correct")
	}

}

func TestDatastore(t *testing.T) {
	ds, err := NewDatastore(dbPath)
	defer ds.Close()

	if err != nil {
		t.Errorf("Unable to create Datastore. Error: %s", err)
	}
	c := &Collection{
		IPNSAddress: "test.com",
		Name:        "Test Collection",
		Description: "Test Descripition",
		IsMine:      true,
	}

	// Create collection
	err = ds.CreateOrUpdateCollection(c)
	if err != nil {
		t.Errorf("Unable to create Collection. Error: %s", err)
	}

	// IsCollectionEmpty
	empty, err := ds.IsCollectionEmpty(c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to check if Collection is empty. Error: %s", err)
	}
	if !empty {
		t.Error("Collection is empty but false returns.")
	}

	cActual, err := ds.ReadCollection(c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to read Collection. Error: %s", err)
	}

	if cActual.IPNSAddress != c.IPNSAddress || cActual.Name != c.Name || cActual.Description != c.Description {
		t.Errorf("Actual read collection is not the same as wanted.")
	}

	// IsMine
	if !cActual.IsMine {
		t.Error("Collection is mine but false returns.")
	}

	// ListCollections - All
	cs, err := ds.ListCollections(FilterAny, FilterAny)
	if err != nil {
		t.Errorf("Unable to list collections. Error: %v", err)
	}

	found := false
	for _, ci := range cs {
		if c.IPNSAddress == ci.IPNSAddress {
			found = true
		}
	}
	if !found {
		t.Error("Collection is not in collection list.")
	}

	// ListCollections - Only mine
	cs, err = ds.ListCollections(FilterOnly, FilterAny)
	if err != nil {
		t.Errorf("Unable to list collections. Error: %v", err)
	}

	found = false
	for _, ci := range cs {
		if c.IPNSAddress == ci.IPNSAddress {
			found = true
		}
	}
	if !found {
		t.Error("Collection is not in collection list.")
	}

	// ListCollections - Only others
	cs, err = ds.ListCollections(FilterNone, FilterAny)
	if err != nil {
		t.Errorf("Unable to list collections. Error: %v", err)
	}

	found = false
	for _, ci := range cs {
		if c.IPNSAddress == ci.IPNSAddress {
			found = true
		}
	}
	if found {
		t.Error("Collection is in collection list.")
	}

	// ListCollections - All empty
	cs, err = ds.ListCollections(FilterAny, FilterOnly)
	if err != nil {
		t.Errorf("Unable to list collections. Error: %v", err)
	}

	found = false
	for _, ci := range cs {
		if c.IPNSAddress == ci.IPNSAddress {
			found = true
		}
	}
	if !found {
		t.Error("Collection is not in collection list.")
	}

	// ListCollections - All my empty
	cs, err = ds.ListCollections(FilterOnly, FilterOnly)
	if err != nil {
		t.Errorf("Unable to list collections. Error: %v", err)
	}

	found = false
	for _, ci := range cs {
		if c.IPNSAddress == ci.IPNSAddress {
			found = true
		}
	}
	if !found {
		t.Error("Collection is not in collection list.")
	}

	// ListCollections - All non-empty
	cs, err = ds.ListCollections(FilterAny, FilterNone)
	if err != nil {
		t.Errorf("Unable to list collections. Error: %v", err)
	}

	found = false
	for _, ci := range cs {
		if c.IPNSAddress == ci.IPNSAddress {
			found = true
		}
	}
	if found {
		t.Error("Collection is in collection list.")
	}

	// ListCollections - All my non-empty
	cs, err = ds.ListCollections(FilterOnly, FilterNone)
	if err != nil {
		t.Errorf("Unable to list collections. Error: %v", err)
	}

	found = false
	for _, ci := range cs {
		if c.IPNSAddress == ci.IPNSAddress {
			found = true
		}
	}
	if found {
		t.Error("Collection is in collection list.")
	}

	// Update collection
	c.Name = "Test Collection2"
	c.IsMine = false
	err = ds.CreateOrUpdateCollection(c)
	if err != nil {
		t.Errorf("Unable to update Collection. Error: %s", err)
	}

	cActual, err = ds.ReadCollection(c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to read Collection. Error: %s", err)
	}

	if cActual.IPNSAddress != c.IPNSAddress || cActual.Name != c.Name || cActual.Description != c.Description {
		t.Errorf("Actual read collection is not the same as wanted.")
	}

	// IsMine
	if cActual.IsMine {
		t.Error("Collection is not mine but true returns.")
	}

	// Create Item
	tag3 := Tag{"tag3"}
	item := &Item{
		CID:  "Qmcpo2iLBikrdf1d6QU6vXuNb6P7hwrbNPW9kLAH8eG67z",
		Name: "Quick Start",
		Tags: []Tag{
			Tag{"tag1a", "tag1b", "tag1c"},
			Tag{"tag2a", "tag2b"},
			tag3,
		},
	}
	err = ds.CreateOrUpdateItem(item)
	if err != nil {
		t.Errorf("Unable to create Item. Error: %s", err)
	}

	// Read Item
	itemActual, err := ds.ReadItem(item.CID)
	if err != nil {
		t.Errorf("Unable to read Item. Error: %s", err)
	}

	if itemActual.CID != item.CID || itemActual.Name != item.Name {
		t.Errorf("Actual read item is not the same as wanted.")
	}

	for _, tag := range item.Tags {
		exists := false
		for _, tagActual := range itemActual.Tags {
			if tagActual.Equals(tag) {
				exists = true
			}
		}
		if !exists {
			t.Errorf("Tag %s doesn't exists in read item", tag)
		}
	}

	// Test tag item count tag::[tagStr]
	tagItemCounts, err := ds.ReadTagItemCount(item.Tags)
	for k, v := range tagItemCounts {
		if v != 1 {
			t.Errorf("Tag %s item count should be 1 but get %d", item.Tags[k], v)
		}
	}

	// Update item
	item.Name = "Quick Start Edited"
	// Tag3 removed
	item.Tags = []Tag{
		Tag{"tag1a", "tag1b", "tag1c"},
		Tag{"tag2a", "tag2b"},
	}

	err = ds.CreateOrUpdateItem(item)
	if err != nil {
		t.Errorf("Unable to update Item. Error: %s", err)
	}

	itemActual, err = ds.ReadItem(item.CID)
	if err != nil {
		t.Errorf("Unable to read Item. Error: %s", err)
	}

	if itemActual.Name != item.Name {
		t.Errorf("Actual read item is not the same as wanted.")
	}

	hasTag, err := ds.HasTag(item.CID, tag3)
	if err != nil {
		t.Errorf("Unable to check if Item has Tag. Error: %s", err)
	}
	if hasTag == true {
		t.Errorf("Item should not has Tag3.")
	}

	// Test tag item count tag::[tagStr]
	tagItemCounts, err = ds.ReadTagItemCount(item.Tags)
	if err != nil {
		t.Errorf("Unable to read tag item count. Error: %s", err)
	}
	for k, v := range tagItemCounts {
		if v != 1 {
			t.Errorf("Tag %s item count should be 1 but get %d", item.Tags[k], v)
		}
	}
	// Tag3 should has count 0
	tagItemCounts, err = ds.ReadTagItemCount([]Tag{tag3})
	if err != nil {
		t.Errorf("Unable to read tag item count. Error: %s", err)
	}
	if tagItemCounts[0] != 0 {
		t.Errorf("Tag3 item count should be 0 but get %d", tagItemCounts[0])
	}

	// Add Tag to Item
	newTag := Tag{"tag4a", "tag4b", "tag4c", "tag4d"}
	err = ds.AddItemTag(item.CID, newTag)
	if err != nil {
		t.Errorf("Unable to add Tag to Item. Error: %s", err)
	}

	hasTag, err = ds.HasTag(item.CID, newTag)
	if err != nil {
		t.Errorf("Unable to check if Item has Tag. Error: %s", err)
	}
	if hasTag == false {
		t.Errorf("Item should has Tag but not.")
	}

	// newTag should has count 1
	tagItemCounts, err = ds.ReadTagItemCount([]Tag{newTag})
	if err != nil {
		t.Errorf("Unable to read tag item count. Error: %s", err)
	}
	if tagItemCounts[0] != 1 {
		t.Errorf("newTag item count should be 1 but get %d", tagItemCounts[0])
	}

	// Remove Tag from Item
	err = ds.RemoveItemTag(item.CID, newTag)
	if err != nil {
		t.Errorf("Unable to remove Tag from Item. Error: %s", err)
	}

	hasTag, err = ds.HasTag(item.CID, newTag)
	if err != nil {
		t.Errorf("Unable to check if Item has Tag. Error: %s", err)
	}
	if hasTag == true {
		t.Errorf("Item should not has Tag but it has.")
	}

	// newTag should has count 0
	tagItemCounts, err = ds.ReadTagItemCount([]Tag{newTag})
	if err != nil {
		t.Errorf("Unable to read tag item count. Error: %s", err)
	}
	if tagItemCounts[0] != 0 {
		t.Errorf("newTag item count should be 0 but get %d", tagItemCounts[0])
	}

	// Add Item to Collection
	err = ds.AddItemToCollection(item.CID, c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to add Item to Collection. Error: %s", err)
	}

	isIn, err := ds.IsItemInCollection(item.CID, c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to check if Item is in Collection. Error: %s", err)
	}

	if isIn == false {
		t.Errorf("Item should be in Collection but not.")
	}

	// IsCollectionEmpty
	empty, err = ds.IsCollectionEmpty(c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to check if Collection is empty. Error: %s", err)
	}
	if empty {
		t.Error("Collection is not empty but true returns.")
	}

	items, err := ds.ReadCollectionItems(c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to check if Item is in Collection. Error: %s", err)
	}

	if !funk.ContainsString(items, item.CID) {
		t.Errorf("Item should be in Collection but not.")
	}

	// Remove Item From Collection
	err = ds.RemoveItemFromCollection(item.CID, c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to remove Item from Collection. Error: %s", err)
	}

	isIn, err = ds.IsItemInCollection(item.CID, c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to check if Item is in Collection. Error: %s", err)
	}

	if isIn == true {
		t.Errorf("Item should not be in Collection but it is.")
	}

	item2 := &Item{
		CID:  "Qmcpo2iLBikrdf1d6QU6vXuNb6P7hwrbNPW9kLAH8eG6dd",
		Name: "Quick Start2",
		Tags: []Tag{
			Tag{"tag1a", "tag1b", "tag1c"},
			Tag{"tag2a", "tag2b"},
			tag3,
		},
	}
	err = ds.CreateOrUpdateItem(item2)
	if err != nil {
		t.Errorf("Unable to create Item2. Error: %s", err)
	}

	err = ds.AddItemToCollection(item2.CID, c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to add Item2 to collection. Error: %s", err)
	}

	// Delete Item
	err = ds.DelItem(item.CID)
	if err != nil {
		t.Errorf("Unable to delete Item. Error: %s", err)
	}

	itemActual, err = ds.ReadItem(item.CID)
	if err != ErrCIDNotFound {
		t.Errorf("Item is not deleted.")
	}

	// Delete collection
	err = ds.DelCollection(c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to delete Collection. Error: %s", err)
	}

	cActual, err = ds.ReadCollection(c.IPNSAddress)
	if err != ErrIPNSNotFound {
		t.Errorf("Collection is not deleted.")
	}

}

func TestSearchTags(t *testing.T) {
	ds, err := NewDatastore(dbPath)
	defer ds.Close()

	if err != nil {
		t.Errorf("Unable to create Datastore. Error: %s", err)
	}

	tag100_1 := Tag{"tag100a", "tag100b", "tag100c"}
	tag100_2 := Tag{"tag100a", "tag100d"}
	tag200 := Tag{"tag200a", "tag200b"}
	tag300 := Tag{"tag300a", "tag300b"}
	tag400_1 := Tag{"tag400a", "tag400b", "tag400c"}
	tag400_2 := Tag{"tag400a", "tag400b"}
	tag400_3 := Tag{"tag400a", "tag400e"}
	item := &Item{
		CID:  "Qmcpo2iLBikrdf1d6QU6vXuNb6P7hwrbNPW9kLAH8eG672",
		Name: "Tag Search Item1",
		Tags: []Tag{
			tag100_1,
			tag200,
			tag300,
		},
	}
	err = ds.CreateOrUpdateItem(item)
	if err != nil {
		t.Errorf("Unable to create Item. Error: %s", err)
	}

	err = ds.AddItemTag(item.CID, tag100_2)
	if err != nil {
		t.Errorf("Unable to add item tag. Error: %s", err)
	}

	item = &Item{
		CID:  "Qmcpo2iLBikrdf1d6QU6vXuNb6P7hwrbNPW9kLAH8e3333",
		Name: "Tag Search Item2",
		Tags: []Tag{
			tag400_1,
			tag400_2,
			tag400_3,
		},
	}
	err = ds.CreateOrUpdateItem(item)
	if err != nil {
		t.Errorf("Unable to create Item. Error: %s", err)
	}

	tags, err := ds.SearchTags("tag100")
	if err != nil {
		t.Errorf("Unable to search tags. Error: %s", err)
	}

	count := len(tags)
	if count != 2 {
		t.Fatalf("Expect 2 result. Actual %d", count)
	}

	tagOKs := make(map[string]bool)
	for _, v := range tags {
		if v.Equals(tag100_1) {
			tagOKs[tag100_1.String()] = true
		}
		if v.Equals(tag100_2) {
			tagOKs[tag100_2.String()] = true
		}
	}

	if v, ok := tagOKs[tag100_1.String()]; !ok || !v {
		t.Errorf("Wrong tag search result. Can't find %s", tag100_1)
	}
	if v, ok := tagOKs[tag100_2.String()]; !ok || !v {
		t.Errorf("Wrong tag search result. Can't find %s", tag100_2)
	}

	tags, err = ds.SearchTags("tag400a:tag400b")
	if err != nil {
		t.Errorf("Unable to search tags. Error: %s", err)
	}

	count = len(tags)
	if count != 2 {
		t.Fatalf("Expect 2 result. Actual %d", count)
	}

	tagOKs = make(map[string]bool)
	for _, v := range tags {
		if v.Equals(tag400_1) {
			tagOKs[tag400_1.String()] = true
		}
		if v.Equals(tag400_2) {
			tagOKs[tag400_2.String()] = true
		}
	}

	if v, ok := tagOKs[tag400_1.String()]; !ok || !v {
		t.Errorf("Wrong tag search result. Can't find %s", tag400_1)
	}
	if v, ok := tagOKs[tag400_2.String()]; !ok || !v {
		t.Errorf("Wrong tag search result. Can't find %s", tag400_2)
	}
}

func TestFolders(t *testing.T) {
	ds, err := NewDatastore(dbPath)
	defer ds.Close()

	if err != nil {
		t.Errorf("Unable to create Datastore. Error: %s", err)
	}

	ipns := "test.com"

	c := &Collection{IPNSAddress: ipns, Name: "Test Collection", Description: "Test Descripition"}

	// Create collection
	err = ds.CreateOrUpdateCollection(c)
	if err != nil {
		t.Errorf("Unable to create Collection. Error: %s", err)
	}

	folder1 := &Folder{Path: "folder1", IPNSAddress: ipns}
	err = ds.CreateOrUpdateFolder(folder1)
	if err != nil {
		t.Errorf("Unable to create folder1. Error: %s", err)
	}

	folder1Actual, err := ds.ReadFolder(ipns, "folder1")
	if err != nil {
		t.Errorf("Unable to read folder1. Error: %s", err)
	}

	if folder1Actual.Path != folder1.Path {
		t.Errorf("Actual folder1 is not wanted")
	}

	// Root folder
	rootActual, err := ds.ReadFolder(ipns, "")
	if err != nil {
		t.Errorf("Unable to read Root folder. Error: %s", err)
	}
	if rootActual.Path != "" {
		t.Errorf("Actual Root folder's path is not wanted")
	}

	children, err := ds.ReadFolderChildren(rootActual)
	if err != nil {
		t.Errorf("Unable to read children of Root folder. Error: %s", err)
	}
	if !funk.ContainsString(children, "folder1") {
		t.Error("folder1 should be in root folder's children")
	}

	// Test parent/child folder
	// folder2 is a child of folder1
	folder2 := &Folder{Path: "folder1/folder2", IPNSAddress: ipns}
	err = ds.CreateOrUpdateFolder(folder2)
	if err != nil {
		t.Errorf("Unable to create folder2. Error: %s", err)
	}

	// folder3 is a child of folder1
	folder3 := &Folder{Path: "folder1/folder3", IPNSAddress: ipns}
	err = ds.CreateOrUpdateFolder(folder3)
	if err != nil {
		t.Errorf("Unable to create folder3. Error: %s", err)
	}

	// folder4 is a child of folder2
	folder4 := &Folder{Path: "folder1/folder2/folder4", IPNSAddress: ipns}
	err = ds.CreateOrUpdateFolder(folder4)
	if err != nil {
		t.Errorf("Unable to create folder3. Error: %s", err)
	}

	folder1Actual, err = ds.ReadFolder(ipns, "folder1")
	if err != nil {
		t.Errorf("Unable to read folder1. Error: %s", err)
	}

	children, err = ds.ReadFolderChildren(folder1Actual)
	if err != nil {
		t.Errorf("Unable to read children of folder1. Error: %s", err)
	}
	if !funk.ContainsString(children, "folder1/folder2") {
		t.Error("folder2 should be in folder1's children")
	}

	if !funk.ContainsString(children, "folder1/folder3") {
		t.Error("folder3 should be in folder1's children")
	}

	folder2Actual, err := ds.ReadFolder(ipns, "folder1/folder2")
	if err != nil {
		t.Errorf("Unable to read folder2. Error: %s", err)
	}

	children, err = ds.ReadFolderChildren(folder2Actual)
	if err != nil {
		t.Errorf("Unable to read children of folder1/folder2. Error: %s", err)
	}
	if !funk.ContainsString(children, "folder1/folder2/folder4") {
		t.Error("folder4 should be in folder2's children")
	}

	// TODO: Test folder update

	item1 := &Item{
		CID:  "Qmcpo2iLBikrdf1d6QU6vXuNb6P7hwrbNPW9kLAH8eG67a",
		Name: "Item1 for folder",
	}
	err = ds.CreateOrUpdateItem(item1)
	if err != nil {
		t.Errorf("Unable to create Item. Error: %s", err)
	}

	item2 := &Item{
		CID:  "Qmcpo2iLBikrdf1d6QU6vXuNb6P7hwrbNPW9kLAH8eG67b",
		Name: "Item2 for folder",
	}
	err = ds.CreateOrUpdateItem(item2)
	if err != nil {
		t.Errorf("Unable to create Item. Error: %s", err)
	}

	item3 := &Item{
		CID:  "Qmcpo2iLBikrdf1d6QU6vXuNb6P7hwrbNPW9kLAH8eG67c",
		Name: "Item3 for folder",
	}
	err = ds.CreateOrUpdateItem(item3)
	if err != nil {
		t.Errorf("Unable to create Item. Error: %s", err)
	}

	err = ds.AddItemToFolder(item1.CID, folder1Actual)
	if err != nil {
		t.Errorf("Unable to add item1 to folder1. Error: %s", err)
	}

	err = ds.AddItemToFolder(item2.CID, folder1Actual)
	if err != nil {
		t.Errorf("Unable to add item2 to folder1. Error: %s", err)
	}

	err = ds.AddItemToFolder(item3.CID, folder1Actual)
	if err != nil {
		t.Errorf("Unable to add item3 to folder1. Error: %s", err)
	}

	folderItems, err := ds.ReadFolderItems(folder1Actual)
	if err != nil {
		t.Errorf("Unable to read folder1 items. Error: %s", err)
	}

	if !funk.ContainsString(folderItems, item1.CID) {
		t.Errorf("folder1 should contain item1 but not.")
	}

	if !funk.ContainsString(folderItems, item2.CID) {
		t.Errorf("folder1 should contain item2 but not.")
	}

	if !funk.ContainsString(folderItems, item3.CID) {
		t.Errorf("folder1 should contain item3 but not.")
	}

	err = ds.RemoveItemFromFolder(item3.CID, folder1Actual)
	if err != nil {
		t.Errorf("Unable to remove item3 from folder1. Error: %s", err)
	}

	folderItems, err = ds.ReadFolderItems(folder1Actual)
	if err != nil {
		t.Errorf("Unable to read folder1 items. Error: %s", err)
	}

	if funk.ContainsString(folderItems, item3.CID) {
		t.Errorf("folder1 should not contain item3.")
	}

	isIn, err := ds.IsItemInFolder(item2.CID, folder1Actual)
	if err != nil {
		t.Errorf("Unable to check if item2 is in folder1. Error: %s", err)
	}
	if !isIn {
		t.Errorf("folder1 should contain item2.")
	}

	// Test copy folder
	err = ds.MoveOrCopyFolder(folder1Actual, &Folder{IPNSAddress: folder1Actual.IPNSAddress, Path: "folder1copy"}, true)
	if err != nil {
		t.Errorf("Unable to copy folder1 to folder1copy. Error: %s", err)
	}

	folder1CopyActual, err := ds.ReadFolder(ipns, "folder1copy")
	if err != nil {
		t.Errorf("Unable to read folder1copy. Error: %s", err)
	}

	_, err = ds.ReadFolder(ipns, "folder1copy/folder2")
	if err != nil {
		t.Errorf("Unable to read folder1copy/folder2. Error: %s", err)
	}

	isIn, err = ds.IsItemInFolder(item2.CID, folder1CopyActual)
	if err != nil {
		t.Errorf("Unable to check if item2 is in folder1copy. Error: %s", err)
	}
	if !isIn {
		fmt.Println(ds.ReadFolderItems(folder1CopyActual))
		t.Errorf("folder1copy should contain item2.")
	}

	err = ds.DelFolder(folder1Actual)
	if err != nil {
		t.Errorf("Unable to delete folder1. Error: %s", err)
	}

	folderExists, err := ds.IsFolderPathExists(ipns, "folder1/folder2")
	if err != nil {
		t.Errorf("Unable to check if folder2 exists. Error: %s", err)
	}

	if folderExists {
		t.Errorf("Folder2 should be deleted but not.")
	}

	folderExists, err = ds.IsFolderPathExists(ipns, "folder1/folder2/folder4")
	if err != nil {
		t.Errorf("Unable to check if folder4 exists. Error: %s", err)
	}

	if folderExists {
		t.Errorf("Folder4 should be deleted but not.")
	}

	inCollection, err := ds.IsItemInCollection(item1.CID, c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to check if item1 is in collection. Error: %s", err)
	}

	if inCollection {
		t.Errorf("Item1 should not be in collection.")
	}

}
