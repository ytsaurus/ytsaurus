package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.PersistentFlags().StringVar(&flagProxy, "proxy", "", "cluster name")
}

var rootCmd = &cobra.Command{}

var (
	flagProxy string
)

func wrapRun(run func() error) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		if err := run(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
			os.Exit(1)
		}
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
