package resource

import (
	"reflect"
	"strings"
)

// Collection is a collection of resource Items.
// TODO: Last update timestamp
type Collection struct {
	IPNSAddress string // Can be either a IPNS hash or a DNSLink domain
	Name        string
	Description string
}

// Folder belongs to only one collection. It may have a parent folder and multiple sub folders.
// In one collection, a Folder's name is unique.
// TODO: Total file size of resources that the folder contains. Including subfolders.
// TODO: Last update timestamp
type Folder struct {
	IPNSAddress string
	Path        string
	Parent      string
	Children    []string
}

// Item is one item of any kind of resource.
// TODO: File size
type Item struct {
	CID  string
	Name string
	Tags []Tag
}

// Tag is for tagging Items.
type Tag []string

// NewTagFromStr create new Tag struct from a string.
func NewTagFromStr(str string) Tag {
	return strings.Split(str, ":")
}

// String implements Stringer interface.
func (t Tag) String() string {
	return strings.Join(t, ":")
}

// Equals check if a tag equals to this tag
func (t Tag) Equals(t2 Tag) bool {
	return reflect.DeepEqual(t, t2)
}

// IsEmpty checks if a Tag is empty
func (t Tag) IsEmpty() bool {
	return len(t) == 0
}
