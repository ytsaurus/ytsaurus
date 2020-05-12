package ytrecipe

import (
	"io"
	"os"
	"syscall"

	"a.yandex-team.ru/yt/go/yt"
)

type FileRow struct {
	FilePath  string `yson:"filename,omitempty"`
	IsDir     bool   `yson:"is_dir,omitempty"`
	PartIndex int    `yson:"part_index,omitempty"`
	Data      []byte `yson:"data,omitempty"`

	// For space files. Row with DataSize != 0, represents DataSize zeroes.
	DataSize int64 `yson:"data_size,omitempty"`
}

type OutputRow struct {
	FileRow

	Stderr []byte `yson:"stdout,omitempty"`
	Stdout []byte `yson:"stderr,omitempty"`

	ExitCode       *int           `yson:"exit_code,omitempty"`
	KilledBySignal syscall.Signal `yson:"killed_by_signal,omitempty"`
}

func ReadOutputTable(r yt.TableReader, replacePath func(string) string, stdout, stderr io.Writer) (*OutputRow, error) {
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

		case row.ExitCode != nil:
			return &row, nil

		case row.IsDir:
			if err := os.MkdirAll(replacePath(row.FilePath), 0777); err != nil {
				return nil, err
			}

		case row.FilePath != "":
			var err error
			if replacePath(row.FilePath) != currentFileName {
				if currentFile != nil {
					if err := currentFile.Close(); err != nil {
						return nil, err
					}
					currentFile = nil
				}

				currentFileSize = 0
				currentFileName = replacePath(row.FilePath)
				currentFile, err = os.Create(replacePath(row.FilePath))
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
			// ignore stdout, stderr and exit code rows.
		}
	}

	return nil, r.Err()
}
