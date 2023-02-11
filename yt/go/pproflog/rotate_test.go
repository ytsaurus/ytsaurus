package pproflog

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRotate(t *testing.T) {
	tmp, err := os.MkdirTemp("", "pprof")
	require.NoError(t, err)

	checkFiles := func(files []string) {
		l, err := os.ReadDir(tmp)
		require.NoError(t, err)

		realFiles := []string{}
		for _, f := range l {
			realFiles = append(realFiles, f.Name())
		}

		sort.Strings(realFiles)
		sort.Strings(files)
		require.Equal(t, files, realFiles)
	}

	makeFile := func(f string) {
		require.NoError(t, os.WriteFile(filepath.Join(tmp, f), nil, 0666))
	}

	opts := Options{Dir: tmp, Keep: 10}
	require.NoError(t, rotateFiles(opts, "cpu"))
	checkFiles([]string{})

	makeFile("cpu.pprof")
	makeFile("cpu.pprof.1")
	makeFile("cpu.pprof.3")

	require.NoError(t, rotateFiles(opts, "cpu"))
	checkFiles([]string{"cpu.pprof.1", "cpu.pprof.2", "cpu.pprof.4"})

	opts.Keep = 1
	require.NoError(t, rotateFiles(opts, "cpu"))
	checkFiles([]string{"cpu.pprof.2"})
}
