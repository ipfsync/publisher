package resource

import (
	"reflect"
	"strings"
)

// Collection is a collection of resource Items.
type Collection struct {
	IPNSAddress string // Can be either a IPNS hash or a DNSLink domain
	Name        string
	Description string
}

// Item is one item of any kind of resource.
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
