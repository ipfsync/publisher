package resource

import "testing"

func TestNewTagFromStr(t *testing.T) {
	want := "movie:genres:drama"
	tag := NewTagFromStr(want)
	if tag[0] != "movie" || tag[1] != "genres" || tag[2] != "drama" {
		t.Errorf("Tag string = %s; want %s", tag, want)
	}
}

func TestTagString(t *testing.T) {
	tag := Tag{"movie", "genres", "drama"}
	want := "movie:genres:drama"
	if tag.String() != want {
		t.Errorf("Tag string = %s; want %s", tag, want)
	}
}
