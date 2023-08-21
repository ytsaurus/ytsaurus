package yson

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadListFragment(t *testing.T) {
	for _, input := range []string{"1;{a=1}", "1;{a=1};"} {
		r := NewReaderKindFromBytes([]byte(input), StreamListFragment)

		for i := 0; i < 2; i++ {
			ok, err := r.NextListItem()
			require.True(t, ok)
			require.NoError(t, err)

			_, err = r.NextRawValue()
			require.NoError(t, err)
		}

		ok, err := r.NextListItem()
		require.False(t, ok)
		require.NoError(t, err)
	}
}

func TestReadEmpty(t *testing.T) {
	for _, input := range []string{"", "   "} {
		r := NewReaderKindFromBytes([]byte(input), StreamListFragment)

		ok, err := r.NextListItem()
		require.NoError(t, err)
		require.False(t, ok)
	}
}
