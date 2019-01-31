package fuzz

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"
)

func TestFullCrashers(t *testing.T) {
	files, err := ioutil.ReadDir("crashers")
	require.Nil(t, err)

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".output") {
			continue
		}
		if strings.HasSuffix(f.Name(), ".quoted") {
			continue
		}

		t.Run(f.Name(), func(t *testing.T) {
			data, err := ioutil.ReadFile(filepath.Join("crashers", f.Name()))
			require.Nil(t, err)

			Fuzz(data)

			FuzzMarshal(data)
		})
	}
}
