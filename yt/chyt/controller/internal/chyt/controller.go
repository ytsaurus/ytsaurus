package chyt

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/chyt/controller/internal/strawberry"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
)

type Controller struct {
	ytc               yt.Client
	l                 log.Logger
	clusterConnection map[string]interface{}
	root              ypath.Path
	cluster           string
}

func (c *Controller) Prepare(ctx context.Context, oplet *strawberry.Oplet) (
	spec map[string]interface{}, description map[string]interface{}, annotations map[string]interface{}, err error) {
	alias := oplet.Alias()

	description = buildDescription(c.cluster, alias)

	var speclet Speclet
	err = yson.Unmarshal(oplet.Speclet(), &speclet)
	if err != nil {
		return
	}

	var filePaths []ypath.Rich

	// Populate resources.
	err = c.populateResources(&speclet)
	if err != nil {
		return
	}

	// Build artifacts.
	err = c.appendArtifacts(ctx, &speclet, &filePaths, &description)
	if err != nil {
		return
	}

	// Build configs.
	err = c.appendConfigs(ctx, oplet, &speclet, &filePaths)
	if err != nil {
		return
	}

	// Prepare runtime stuff: stderr/core-table, etc.
	runtimePaths, err := c.prepareRuntime(ctx, speclet.RuntimeDataPathOrDefault().Child(alias), alias, oplet.NextIncarnationIndex())
	if err != nil {
		return
	}

	// Build command.
	var args []string
	args = append(args, "./clickhouse-trampoline", "./ytserver-clickhouse")
	args = append(args, "--monitoring-port", "10142", "--log-tailer-monitoring-port", "10242")
	if speclet.EnableGeoDataOrDefault() {
		args = append(args, "--prepare-geodata")
	}
	args = append(args, "--log-tailer-bin", "./ytserver-log-tailer")
	command := strings.Join(args, " ")

	spec = map[string]interface{}{
		"tasks": map[string]interface{}{
			"instances": map[string]interface{}{
				"command":                            command,
				"job_count":                          speclet.Resources.InstanceCount,
				"file_paths":                         filePaths,
				"memory_limit":                       speclet.Resources.InstanceMemory.totalMemory(),
				"cpu_limit":                          speclet.Resources.InstanceCPU,
				"port_count":                         5,
				"max_stderr_size":                    1024 * 1024 * 1024,
				"user_job_memory_digest_lower_bound": 1.0,
				"restart_completed_jobs":             true,
				"interruption_signal":                "SIGINT",
			},
		},
		"max_failed_job_count": 10 * 1000,
		"max_stderr_count":     150,
		"stderr_table_path":    runtimePaths.StderrTable,
		"core_table_path":      runtimePaths.CoreTable,
		"title":                "CHYT clique *" + alias,
	}
	annotations = map[string]interface{}{
		"is_clique": true,
		"expose":    true,
	}
	return
}

func (c *Controller) Family() string {
	return "chyt"
}

func (c Controller) NeedRestartOnSpecletChange(oldSpecletYson, newSpecletYson yson.RawValue) bool {
	var oldSpeclet, newSpeclet Speclet
	err := yson.Unmarshal(oldSpecletYson, &oldSpeclet)
	if err != nil {
		c.l.Error("error parsing old speclet", log.Error(err), log.String("old_speclet", string(oldSpecletYson)))
		return false
	}
	err = yson.Unmarshal(newSpecletYson, &newSpeclet)
	if err != nil {
		c.l.Error("error parsing new speclet", log.Error(err), log.String("new_speclet", string(newSpecletYson)))
		return false
	}
	return !reflect.DeepEqual(oldSpeclet, newSpeclet)
}

func NewController(l log.Logger, ytc yt.Client, root ypath.Path, cluster string, config yson.RawValue) strawberry.Controller {
	c := &Controller{
		l:       l,
		ytc:     ytc,
		root:    root,
		cluster: cluster,
	}
	err := ytc.GetNode(context.Background(), ypath.Path("//sys/@cluster_connection"), &c.clusterConnection, nil)
	if err != nil {
		panic(err)
	}
	if _, ok := c.clusterConnection["block_cache"]; ok {
		panic(fmt.Errorf("chyt: cluster connection contains block_cache section; looks like a misconfiguration"))
	}
	return c
}
