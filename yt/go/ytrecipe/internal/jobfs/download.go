package jobfs

import (
	"io"
	"os"
	"syscall"
	"time"

	"a.yandex-team.ru/yt/go/yt"
)

type ExitRow struct {
	ExitCode       int            `yson:"exit_code,omitempty"`
	KilledBySignal syscall.Signal `yson:"killed_by_signal,omitempty"`
	IsOOM          bool           `yson:"is_oom,omitempty"`

	StartedAt  time.Time `yson:"started_at"`
	UnpackedAt time.Time `yson:"unpacked_at"`
	FinishedAt time.Time `yson:"finished_at"`
}

type OutputRow struct {
	FileRow
	*ExitRow

	Stderr []byte `yson:"stdout,omitempty"`
	Stdout []byte `yson:"stderr,omitempty"`
}

func ReadOutputTable(r yt.TableReader, transformPath func(string) (string, bool), stdout, stderr io.Writer) (*ExitRow, error) {
	var currentFileName string
	var currentFile *os.File
	var currentFileSize int64

	defer currentFile.Close()

	for r.Next() {
		var row OutputRow
		if err := r.Scan(&row); err != nil {
			return nil, err
		}

		switch {
		case len(row.Stdout) != 0:
			_, err := stdout.Write(row.Stdout)
			if err != nil {
				return nil, err
			}

		case len(row.Stderr) != 0:
			_, err := stderr.Write(row.Stderr)
			if err != nil {
				return nil, err
			}

		case row.ExitRow != nil:
			return row.ExitRow, nil

		case row.IsDir:
			path, ok := transformPath(row.FilePath)
			if !ok {
				continue
			}

			if err := os.MkdirAll(path, 0777); err != nil {
				return nil, err
			}

		case row.FilePath != "":
			path, ok := transformPath(row.FilePath)
			if !ok {
				continue
			}

			var err error
			if path != currentFileName {
				if currentFile != nil {
					if err := currentFile.Close(); err != nil {
						return nil, err
					}
					currentFile = nil
				}

				currentFileSize = 0
				currentFileName = path
				currentFile, err = os.Create(path)
				if err != nil {
					return nil, err
				}
			}

			if row.DataSize == 0 {
				currentFileSize += int64(len(row.Data))
				_, err = currentFile.Write(row.Data)
				if err != nil {
					return nil, err
				}
			} else {
				currentFileSize += row.DataSize
				if err := currentFile.Truncate(currentFileSize); err != nil {
					return nil, err
				}

				_, err := currentFile.Seek(row.DataSize, io.SeekCurrent)
				if err != nil {
					return nil, err
				}
			}

		default:
		}
	}

	return nil, r.Err()
}
