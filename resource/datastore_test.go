package resource

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger"
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

	// Create
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

	// Update
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

	// Delete
	err = ds.DelCollection(c.IPNSAddress)
	if err != nil {
		t.Errorf("Unable to delete Collection. Error: %s", err)
	}

	cActual, err = ds.ReadCollection(c.IPNSAddress)
	if err != badger.ErrKeyNotFound {
		t.Errorf("Unable to delete Collection. Error: %s", err)
	}

}
