package log

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Simple test, that all type of fields are correctly zapified.
// Maybe we also need some test that checks resulting zap.Field type also.
func TestFieldAny(t *testing.T) {
	for typ := FieldType(0); typ < fieldTypeLast; typ++ {
		field := Field{ftype: typ}
		assert.NotPanics(t, func() {
			field.Any()
		})
	}
}

func TestAny(t *testing.T) {
	var v struct{ A int }
	field := Any("test", &v)
	assert.Equal(t, field.ftype, FieldTypeAny)
}

func TestReflect(t *testing.T) {
	field := Reflect("test", 1)
	assert.Equal(t, field.ftype, FieldTypeReflect)
}

func TestRawContext(t *testing.T) {
	field := RawContext(context.TODO())
	assert.Equal(t, field.ftype, FieldTypeRawContext)
}

// TODO: test fields
// TODO: test field converters
