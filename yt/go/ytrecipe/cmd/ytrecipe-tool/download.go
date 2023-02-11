package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/jobfs"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/ytexec"
)

func init() {
	rootCmd.AddCommand(downloadCmd)

	downloadCmd.Flags().StringVar(&flagPath, "path", "", "cypress path")
	downloadCmd.Flags().StringVar(&flagOutput, "output", "", "output directory")
	downloadCmd.Flags().Bool("update-me-to-v3", false, "dummy flag to check for version compatibility")
	downloadCmd.Flags().BoolVar(&flagSkipYaOutput, "skip-ya-output", false, "")
}

var (
	flagPath         string
	flagOutput       string
	flagSkipYaOutput bool
)

var downloadCmd = &cobra.Command{
	Use: "download",
	Run: wrapRun(doDownload),
}

func doDownload() error {
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:             flagProxy,
		ReadTokenFromFile: true,
	})
	if err != nil {
		return err
	}

	ctx := context.Background()

	var fs jobfs.Config
	if err := yc.GetNode(ctx, ypath.Path(flagPath).Attr("fs_config"), &fs, nil); err != nil {
		return err
	}

	r, err := yc.ReadTable(ctx, ypath.Path(flagPath).Child(ytexec.YTOutputTableName), nil)
	if err != nil {
		return err
	}
	defer r.Close()

	if flagOutput == "" {
		flagOutput = path.Base(flagPath)
		_, _ = fmt.Fprintf(os.Stderr, "downloading to %s\n", flagOutput)
	}

	transformPath := func(s string) (string, bool) {
		for prefix, to := range fs.Download {
			if strings.HasPrefix(s, prefix) {
				return filepath.Join(flagOutput, to, strings.TrimPrefix(s, prefix)), true
			}
		}

		return "", false
	}

	if _, err := jobfs.ReadOutputTable(r, transformPath, io.Discard, io.Discard); err != nil {
		return err
	}

	if flagSkipYaOutput {
		return nil
	}

	r, err = yc.ReadTable(ctx, ypath.Path(flagPath).Child(ytexec.OutputTableName), nil)
	if err != nil {
		return err
	}
	defer r.Close()

	var stdout, stderr io.Writer
	if stdoutPath, ok := transformPath(fs.StdoutFile); ok {
		if err := os.MkdirAll(filepath.Dir(stdoutPath), 0777); err != nil {
			return err
		}

		stdoutFile, err := os.Create(stdoutPath)
		if err != nil {
			return err
		}
		defer stdoutFile.Close()

		stdout = stdoutFile
	} else {
		stdout = io.Discard
	}

	if stderrPath, ok := transformPath(fs.StderrFile); ok {
		if err := os.MkdirAll(filepath.Dir(stderrPath), 0777); err != nil {
			return err
		}

		stderrFile, err := os.Create(stderrPath)
		if err != nil {
			return err
		}
		defer stderrFile.Close()

		stderr = stderrFile
	} else {
		stderr = io.Discard
	}

	_, err = jobfs.ReadOutputTable(r, transformPath, stdout, stderr)
	return err
}
