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
	require.Equal(t, uint32(0x1), d)
	require.Equal(t, uint32(0x84), c)
	require.Equal(t, uint32(0x3ff012f), b)
	require.Equal(t, uint32(0xbe9399a3), a)

	require.Equal(t, GUID{0xa3, 0x99, 0x93, 0xbe, 0x2f, 0x01, 0xff, 0x03, 0x84, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}, guid)
}

func TestParseInvalidString(t *testing.T) {
	_, err := ParseString("1-84-3ff012f-be9399a3fffff")
	require.Error(t, err)
}
