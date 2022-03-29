package ytprof

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExpressionMap(t *testing.T) {
	var metadata Metadata
	types := metadata.Types()
	vars := metadata.Vars()

	fromTypes := make([]string, 0)
	for key := range types {
		fromTypes = append(fromTypes, key)
	}

	fromVars := make([]string, 0)
	for key := range vars {
		fromVars = append(fromVars, key)
	}

	require.Equal(t, fromTypes, fromVars)
}
