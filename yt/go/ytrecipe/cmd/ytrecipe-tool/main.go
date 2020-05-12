package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/go/ytrecipe"
)

func init() {
	rootCmd.AddCommand(downloadCmd)

	downloadCmd.Flags().StringVar(&flagProxy, "proxy", "", "cluster name")
	downloadCmd.Flags().StringVar(&flagPath, "path", "", "cypress path")
	downloadCmd.Flags().StringVar(&flagOutput, "output", "", "output directory")
	downloadCmd.Flags().StringVar(&flagTrimPrefix, "trim-prefix", "", "trim prefix from all paths")
	downloadCmd.Flags().BoolVar(&flagSkipYaOutput, "skip-ya-output", false, "")
}

var rootCmd = &cobra.Command{}

var (
	flagProxy        string
	flagPath         string
	flagOutput       string
	flagTrimPrefix   string
	flagSkipYaOutput bool
)

var downloadCmd = &cobra.Command{
	Use: "download",
	Run: func(cmd *cobra.Command, args []string) {
		if err := doDownload(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
			os.Exit(1)
		}
	},
}

func doDownload() error {
	yc, err := ythttp.NewClientCli(flagProxy)
	if err != nil {
		return err
	}

	ctx := context.Background()
	r, err := yc.ReadTable(ctx, ypath.Path(flagPath).Child(ytrecipe.YTRecipeOutput), nil)
	if err != nil {
		return err
	}
	defer r.Close()

	if flagOutput == "" {
		flagOutput = path.Base(flagPath)
		_, _ = fmt.Fprintf(os.Stderr, "downloading to %s\n", flagOutput)
	}

	replacePath := func(s string) string {
		return filepath.Join(flagOutput, strings.TrimPrefix(s, flagTrimPrefix))
	}

	if _, err := ytrecipe.ReadOutputTable(r, replacePath, ioutil.Discard, ioutil.Discard); err != nil {
		return err
	}

	if flagSkipYaOutput {
		return nil
	}

	r, err = yc.ReadTable(ctx, ypath.Path(flagPath).Child(ytrecipe.OutputTableName), nil)
	if err != nil {
		return err
	}
	defer r.Close()

	if err := os.MkdirAll(filepath.Join(flagOutput, "testing_out_stuff"), 0777); err != nil {
		return err
	}

	stdoutFile, err := os.Create(filepath.Join(flagOutput, "testing_out_stuff", "stdout"))
	if err != nil {
		return err
	}
	defer stdoutFile.Close()

	stderrFile, err := os.Create(filepath.Join(flagOutput, "testing_out_stuff", "stderr"))
	if err != nil {
		return err
	}
	defer stderrFile.Close()

	_, err = ytrecipe.ReadOutputTable(r, replacePath, stdoutFile, stderrFile)
	return err
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
