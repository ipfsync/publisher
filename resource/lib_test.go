package resource

import "testing"

func TestTagString(t *testing.T) {
	tag := Tag{"movie", "genres", "drama"}
	want := "movie:genres:drama"
	if tag.String() != want {
		t.Errorf("Tag string = %s; want %s", tag, want)
	}
}
