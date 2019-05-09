// Package row defines conversions between YT and go data types.
package row

type FieldTag struct {
	Name      string
	OmitEmpty bool
}

func ParseTag(fieldName, tag string) (*FieldTag, error) {
	return nil, nil
}
