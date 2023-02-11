package pproflog

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func rotateFiles(opts Options, name string) error {
	files, err := os.ReadDir(opts.Dir)
	if err != nil {
		return err
	}

	var allFiles []string
	for _, f := range files {
		if strings.HasPrefix(f.Name(), name+".pprof") && !strings.HasSuffix(f.Name(), ".tmp") {
			allFiles = append(allFiles, f.Name())
		}
	}

	fileIndex := func(f string) int {
		index := strings.TrimPrefix(strings.TrimPrefix(f, name+".pprof"), ".")
		if index == "" {
			return 0
		}

		i, _ := strconv.Atoi(index)
		return i
	}

	sort.Slice(allFiles, func(i, j int) bool {
		return fileIndex(allFiles[i]) < fileIndex(allFiles[j])
	})

	for len(allFiles) > opts.Keep {
		f := allFiles[len(allFiles)-1]
		if err := os.Remove(filepath.Join(opts.Dir, f)); err != nil {
			return err
		}

		allFiles = allFiles[:len(allFiles)-1]
	}

	for i := len(allFiles) - 1; i >= 0; i-- {
		from := filepath.Join(opts.Dir, allFiles[i])
		to := filepath.Join(opts.Dir, fmt.Sprintf("%s.pprof.%d", name, fileIndex(allFiles[i])+1))

		if err := os.Rename(from, to); err != nil {
			return err
		}
	}

	return nil
}
