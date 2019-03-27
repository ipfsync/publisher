package resource

import (
	"strings"
)

// Collection is a collection of resource Items
type Collection struct {
	IPNSAddress string // Can be either a IPNS hash or a DNSLink domain
	Name        string
	Description string
}

// Item is one item of any kind of resource
type Item struct {
	CID  string
	Name string
	Tags []Tag
}

// Tag is for tagging Items
type Tag []string

// String implements Stringer interface
func (t Tag) String() string {
	return strings.Join(t, ":")
}
