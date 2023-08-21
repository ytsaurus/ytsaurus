package yson

import (
	"reflect"
	"strings"
)

type Tag struct {
	Name string

	Omitempty bool
	Value     bool
	Attr      bool
	Attrs     bool
	Key       bool
}

// ParseTag parses yson annotation for struct field tag.
func ParseTag(fieldName string, fieldTag reflect.StructTag) (tag *Tag, skip bool) {
	tag = &Tag{}

	tagValue, ok := fieldTag.Lookup("yson")
	if !ok {
		tag.Name = fieldName
	}

	parts := strings.Split(tagValue, ",")
	switch parts[0] {
	case "":
		tag.Name = fieldName
	case "-":
		return nil, true
	default:
		tag.Name = parts[0]
	}

	for _, part := range parts[1:] {
		switch part {
		case "omitempty":
			tag.Omitempty = true
		case "key":
			tag.Key = true
		case "value":
			tag.Value = true
			tag.Name = ""
		case "attr":
			tag.Attr = true
		case "attrs":
			tag.Attrs = true
			tag.Name = ""
		}
	}

	return tag, false
}
