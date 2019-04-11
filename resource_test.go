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

func TestTagEquality(t *testing.T) {
	tag := Tag{"movie", "genres", "drama"}
	tag2 := Tag{"movie", "genres", "drama"}
	tag3 := Tag{"movie", "genres", "drama2"}
	tag4 := Tag{"movie", "genres"}

	if !tag.Equals(tag2) {
		t.Error("Tag should equal tag2.")
	}
	if tag.Equals(tag3) {
		t.Error("Tag should not equal tag3.")
	}
	if tag.Equals(tag4) {
		t.Error("Tag should not equal tag4.")
	}
}
