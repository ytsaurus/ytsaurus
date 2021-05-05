package guid

import (
	"encoding"
	"testing"

	"github.com/stretchr/testify/require"
)

var _ encoding.TextMarshaler = GUID{}
var _ encoding.TextUnmarshaler = &GUID{}

func TestParseString(t *testing.T) {
	guid, err := ParseString("1-84-3ff012f-be9399a3")
	require.NoError(t, err)

	a, b, c, d := guid.Parts()
	require.Equal(t, uint32(0x1), a)
	require.Equal(t, uint32(0x84), b)
	require.Equal(t, uint32(0x3ff012f), c)
	require.Equal(t, uint32(0xbe9399a3), d)
}

func TestParseInvalidString(t *testing.T) {
	_, err := ParseString("1-84-3ff012f-be9399a3fffff")
	require.Error(t, err)
}
