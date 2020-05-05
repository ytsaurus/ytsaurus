package ytrecipe

import (
	"context"
	"os"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

const YTRecipeOutput = "/slot/sandbox/ytrecipe_output"

type FileRow struct {
	FilePath  string `yson:"filename,omitempty"`
	IsDir     bool   `yson:"is_dir,omitempty"`
	PartIndex int    `yson:"part_index,omitempty"`
	Data      []byte `yson:"data,omitempty"`
}

type OutputRow struct {
	FileRow

	Stderr   []byte `yson:"stdout,omitempty"`
	Stdout   []byte `yson:"stderr,omitempty"`
	ExitCode *int   `yson:"exit_code,omitempty"`
}

func readResultsAndExit(ctx context.Context, yc yt.Client, outTable ypath.Path) error {
	r, err := yc.ReadTable(ctx, outTable, nil)
	if err != nil {
		return err
	}
	defer r.Close()

	var currentFileName string
	var currentFile *os.File
	defer currentFile.Close()

	for r.Next() {
		var row OutputRow
		if err := r.Scan(&row); err != nil {
			return err
		}

		switch {
		case len(row.Stdout) != 0:
			_, err := os.Stdout.Write(row.Stdout)
			if err != nil {
				return err
			}

		case len(row.Stderr) != 0:
			_, err := os.Stderr.Write(row.Stderr)
			if err != nil {
				return err
			}

		case row.IsDir:
			if err := os.MkdirAll(row.FilePath, 0777); err != nil {
				return err
			}

		case row.ExitCode != nil:
			os.Exit(*row.ExitCode)

		default:
			if row.FilePath != currentFileName {
				if currentFile != nil {
					if err := currentFile.Close(); err != nil {
						return err
					}
					currentFile = nil
				}

				currentFileName = row.FilePath
				currentFile, err = os.Create(row.FilePath)
				if err != nil {
					return err
				}
			}

			_, err = currentFile.Write(row.Data)
			if err != nil {
				return err
			}
		}
	}

	return r.Err()
}
