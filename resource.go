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
	IsMine      bool
}

// Folder belongs to only one collection. It may have a parent folder and multiple sub folders.
// In one collection, a Folder's path is unique.
// If path is "", it's the root directory of a collection
// TODO: Total file size of resources that the folder contains. Including subfolders.
// TODO: Last update timestamp
type Folder struct {
	IPNSAddress string
	Path        string
}

// ParentPath return parent paths of the folder
func (f *Folder) ParentPath() string {
	parts := strings.Split(f.Path, "/")
	partsLen := len(parts)
	var parentPath string
	if partsLen != 1 {
		parentPath = strings.Join(parts[:partsLen-1], "/")
	}
	return parentPath
}

// Basename returns the last part of paths (base name)
func (f *Folder) Basename() string {
	parts := strings.Split(f.Path, "/")
	return parts[len(parts)-1]
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
