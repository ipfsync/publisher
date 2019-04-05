package resource

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

var testdataDir = filepath.Join(".", "testdata")

func TestMain(m *testing.M) {
	// Ensure testdata dir exists
	_ = os.MkdirAll(testdataDir, os.ModePerm)
	retCode := m.Run()
	dbPath := filepath.Join(testdataDir, "test.db")
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
	dbPath := filepath.Join(testdataDir, "test.db")
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
	item := &Item{
		CID:  "Qmcpo2iLBikrdf1d6QU6vXuNb6P7hwrbNPW9kLAH8eG67z",
		Name: "Quick Start",
		Tags: []Tag{
			Tag{"tag1a", "tag1b", "tag1c"},
			Tag{"tag2a", "tag2b"},
			Tag{"tag3"},
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
