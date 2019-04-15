package resource

import (
	"bytes"
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
	c := &Collection{IPNSAddress: "test.com", Name: "Test Collection", Description: "Test Descripition"}

	// Create collection
	err = ds.CreateOrUpdateCollection(c)
	if err != nil {
		t.Errorf("Unable to create Collection. Error: %s", err)
	}

	cActual, err := ds.ReadCollection(c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to read Collection. Error: %s", err)
	}

	if cActual.IPNSAddress != c.IPNSAddress || cActual.Name != c.Name || cActual.Description != c.Description {
		t.Errorf("Actual read collection is not the same as wanted.")
	}

	// Update collection
	c.Name = "Test Collection2"
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
		CID:  "Qmcpo2iLBikrdf1d6QU6vXuNb6P7hwrbNPW9kLAH8eG672",
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
	err = ds.CreateFolder(folder1)
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
	if !funk.ContainsString(rootActual.Children, "folder1") {
		t.Error("folder1 should be in root folder's children")
	}

	// Test parent/child folder
	// folder2 is a child of folder1
	folder2 := &Folder{Path: "folder1/folder2", IPNSAddress: ipns}
	err = ds.CreateFolder(folder2)
	if err != nil {
		t.Errorf("Unable to create folder2. Error: %s", err)
	}

	// folder3 is a child of folder1
	folder3 := &Folder{Path: "folder1/folder3", IPNSAddress: ipns}
	err = ds.CreateFolder(folder3)
	if err != nil {
		t.Errorf("Unable to create folder3. Error: %s", err)
	}

	// folder4 is a child of folder2
	folder4 := &Folder{Path: "folder1/folder2/folder4", IPNSAddress: ipns}
	err = ds.CreateFolder(folder4)
	if err != nil {
		t.Errorf("Unable to create folder3. Error: %s", err)
	}

	folder1Actual, err = ds.ReadFolder(ipns, "folder1")
	if err != nil {
		t.Errorf("Unable to read folder1. Error: %s", err)
	}

	if !funk.ContainsString(folder1Actual.Children, "folder1/folder2") {
		t.Error("folder2 should be in folder1's children")
	}

	if !funk.ContainsString(folder1Actual.Children, "folder1/folder3") {
		t.Error("folder3 should be in folder1's children")
	}

	folder2Actual, err := ds.ReadFolder(ipns, "folder1/folder2")
	if err != nil {
		t.Errorf("Unable to create folder2. Error: %s", err)
	}

	if !funk.ContainsString(folder2Actual.Children, "folder1/folder2/folder4") {
		t.Error("folder4 should be in folder2's children")
	}
}
