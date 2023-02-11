package selfrotate

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	gzSuffix  = ".gz"
	tmpSuffix = ".tmp"
)

func nextRotation(now time.Time, interval RotateInterval) time.Time {
	return now.Truncate(time.Duration(interval)).Add(time.Duration(interval))
}

type logFile struct {
	Name       string
	Size       int64
	Index      int
	Compressed bool
}

func removeTmpFiles(name string) error {
	dir, pattern := filepath.Split(name)

	files, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, f := range files {
		if !strings.HasSuffix(f.Name(), tmpSuffix) {
			continue
		}

		if _, ok := parseFilename(strings.TrimSuffix(f.Name(), tmpSuffix), pattern); !ok {
			continue
		}

		if err := os.Remove(filepath.Join(dir, f.Name())); err != nil {
			return err
		}
	}

	return nil
}

func formatFilename(pattern string, index int, compressed bool) string {
	name := pattern
	if index > 0 {
		name += fmt.Sprint(".", index)
	}
	if compressed {
		name += gzSuffix
	}
	return name
}

// parseFilename parses log file name in the form of [pattern](.gz)(.\d+)
func parseFilename(name, pattern string) (logFile, bool) {
	var file logFile
	file.Name = name

	if !strings.HasPrefix(name, pattern) {
		return file, false
	}
	name = strings.TrimPrefix(name, pattern)

	if strings.HasSuffix(name, gzSuffix) {
		file.Compressed = true

		name = strings.TrimSuffix(name, gzSuffix)
	}

	if strings.HasPrefix(name, ".") {
		name = strings.TrimPrefix(name, ".")

		var err error
		if file.Index, err = strconv.Atoi(name); err != nil {
			return file, false
		}

		return file, true
	} else if name == "" {
		return file, true
	} else {
		return file, false
	}
}

func listLogs(name string) ([]logFile, error) {
	dir, pattern := filepath.Split(name)

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var logs []logFile
	for _, f := range files {
		if logFile, ok := parseFilename(f.Name(), pattern); ok {
			info, err := f.Info()
			if err != nil {
				return nil, err
			}
			logFile.Size = info.Size()
			logs = append(logs, logFile)
		}
	}

	return logs, nil
}

type fileRename struct {
	from, to string
}

func planRotation(dir string, files []logFile, options Options) (remove []string, rename, compress []fileRename, err error) {
	var stat syscall.Statfs_t

	err = syscall.Statfs(dir, &stat)
	if err != nil {
		return
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Index > files[j].Index
	})

	var totalSize int64
	for _, log := range files {
		totalSize += log.Size
	}

	for len(files) > 1 {
		var shouldRemove bool

		if options.MaxSize != 0 && totalSize > options.MaxSize {
			shouldRemove = true
		}
		if options.MaxKeep != 0 && len(files) > options.MaxKeep {
			shouldRemove = true
		}
		if float64(stat.Bfree)/float64(stat.Blocks) < options.MinFreeSpace {
			shouldRemove = true
		}

		if !shouldRemove {
			break
		}

		removedFile := files[0]
		files = files[1:]

		totalSize -= removedFile.Size
		stat.Bfree += uint64(removedFile.Size) / uint64(stat.Bsize)

		remove = append(remove, removedFile.Name)
	}

	_, pattern := filepath.Split(options.Name)
	for i, log := range files {
		if i+1 == len(files) && options.Compress == CompressDelayed && !log.Compressed {
			rename = append(rename, fileRename{
				from: log.Name,
				to:   formatFilename(pattern, log.Index+1, false),
			})

			compress = append(compress, fileRename{
				from: formatFilename(pattern, log.Index+1, false),
				to:   formatFilename(pattern, log.Index+1, true),
			})
		} else {
			rename = append(rename, fileRename{
				from: log.Name,
				to:   formatFilename(pattern, log.Index+1, log.Compressed),
			})
		}
	}

	return
}
