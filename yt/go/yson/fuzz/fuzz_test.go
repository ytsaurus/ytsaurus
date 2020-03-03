package fuzz

import (
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFullCrashers(t *testing.T) {
	files, err := ioutil.ReadDir("crashers")
	require.NoError(t, err)

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".output") {
			continue
		}
		if strings.HasSuffix(f.Name(), ".quoted") {
			continue
		}

		t.Run(f.Name(), func(t *testing.T) {
			data, err := ioutil.ReadFile(filepath.Join("crashers", f.Name()))
			require.NoError(t, err)

			Fuzz(data)

			Marshal(data)
		})
	}
}
