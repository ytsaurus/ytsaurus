package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var (
	flagService  string
	flagCluster  string
	flagPod      string
	flagType     string
	flagDuration int
)

type service struct {
	port int
}

var services = map[string]service{
	"master":           {port: 1004},
	"node":             {port: 1004},
	"tab_node":         {port: 1004},
	"dat_node":         {port: 1004},
	"scheduler":        {port: 1004},
	"controller_agent": {port: 1004},
	"http_proxy":       {port: 1004},
	"rpc_proxy":        {port: 1004},
}

func serviceList() string {
	var list []string
	for s := range services {
		list = append(list, s)
	}
	return strings.Join(list, ", ")
}

func init() {
	rootCmd.Flags().StringVarP(&flagService, "service", "s", "", "one of: "+serviceList())
	rootCmd.MarkFlagRequired("service")

	rootCmd.Flags().StringVarP(&flagCluster, "cluster", "c", "", "cluster name")
	rootCmd.MarkFlagRequired("cluster")

	rootCmd.Flags().StringVarP(&flagPod, "pod", "p", "", "host name, short or long")
	rootCmd.MarkFlagRequired("pod")

	rootCmd.Flags().StringVarP(&flagType, "type", "t", "", "one of: (cpu, heap, peak, allocation, fragmentation)")
	rootCmd.MarkFlagRequired("type")

	rootCmd.Flags().IntVarP(&flagDuration, "duration", "d", 0, "profile collection duration")
}

var rootCmd = &cobra.Command{
	Use:   "ytprof -c cluster -s service -p pod -t type [-d duration]",
	Short: "Fetch profile of YT component",
	Long:  ``,
	Example: `
  # record cpu profile from hahn master for 60 seconds
  ytprof -c hahn -s master -p m005 -t cpu -d 60

  # get peak memory usage of node vla0-1352
  ytprof -c arnold -s node -p vla0-1352 -t peak`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(1)
	}
}
