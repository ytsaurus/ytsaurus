package ypath

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPath(t *testing.T) {
	assert.Equal(t, Path("//foo/bar&/@attr/zog"), Root.Child("foo").Child("bar").SuppressSymlink().Attr("attr").Child("zog"))
	assert.Equal(t, Path("//foo/end"), Root.Child("foo").ListEnd())
	assert.Equal(t, Path("//foo/begin"), Root.Child("foo").ListBegin())
}
