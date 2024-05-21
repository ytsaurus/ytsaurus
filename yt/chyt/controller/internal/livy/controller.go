package livy

import (
	"context"
	"fmt"
	"strings"

	"golang.org/x/exp/slices"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

const (
	LivyPort = 27135
)

type Config struct {
}

type Controller struct {
	ytc     yt.Client
	l       log.Logger
	root    ypath.Path
	cluster string
	config  Config
}

func (c *Controller) UpdateState() (changed bool, err error) {
	return false, nil
}

func getDiscoveryServerAddresses(ctx context.Context, ytc yt.Client) (discoveryAddresses string, err error) {
	var rawAddresses []string
	err = ytc.ListNode(ctx, ypath.Path("//sys/discovery_servers"), &rawAddresses, nil)
	if yterrors.ContainsResolveError(err) {
		err = nil
	}
	addressesBytes, err := yson.Marshal(rawAddresses)
	if err != nil {
		return
	}
	return string(addressesBytes[:]), nil
}

func (c *Controller) buildCommand(ctx context.Context, speclet *Speclet, alias string, sparkDistribName string,
	globalConf *SparkGlobalConf, conf *SparkLaunchConf) (command string, env map[string]string, err error) {

	ipV6Enabled, found := conf.SparkConf["spark.hadoop.yt.preferenceIpv6.enabled"]
	if !found {
		ipV6Enabled = "false"
	}
	javaOpts := []string{
		"-Xmx512m",
		"-cp spyt-package/conf/:spyt-package/jars/*:spark/jars/*",
		"-Dlog4j.loglevel=INFO",
		fmt.Sprintf("-Djava.net.preferIPv6Addresses=%[1]v -Dspark.hadoop.yt.preferenceIpv6.enabled=%[1]v", ipV6Enabled),
	}
	livyOpts := []string{
		fmt.Sprintf("--port %v", LivyPort),
		fmt.Sprintf("--driver-cores %v", speclet.DriverCPUOrDefault()),
		fmt.Sprintf("--driver-memory %vm", speclet.DriverMemoryOrDefault()/(1024*1024)),
		fmt.Sprintf("--max-sessions %v", speclet.MaxSessionsOrDefault()),
	}
	if speclet.SparkMasterAddress != nil {
		livyOpts = append(livyOpts, fmt.Sprintf("--master-address %v", *speclet.SparkMasterAddress))
	}
	envCommand := fmt.Sprintf("./setup-spyt-env.sh --spark-home . --enable-livy --spark-distributive %v", sparkDistribName)
	launcherCommand := "$JAVA_HOME/bin/java " + strings.Join(javaOpts[:], " ") +
		" tech.ytsaurus.spark.launcher.LivyLauncher " + strings.Join(livyOpts[:], " ")
	cmd := envCommand + " && " + launcherCommand

	discoveryAddresses, err := getDiscoveryServerAddresses(ctx, c.ytc)
	if err != nil {
		return
	}
	masterGroupID := alias
	if speclet.masterGroupID != nil {
		masterGroupID = *speclet.masterGroupID
	}
	envPatch := map[string]string{
		"LIVY_HOME":                       "$HOME/./livy",
		"SPARK_DISCOVERY_GROUP_ID":        alias,
		"SPARK_MASTER_DISCOVERY_GROUP_ID": masterGroupID,
		"SPARK_HOME":                      "$HOME/./spark",
		"SPARK_YT_RPC_JOB_PROXY_ENABLED":  "True",
		"SPARK_YT_SOLOMON_ENABLED":        "False",
		"SPARK_YT_TCP_PROXY_ENABLED":      "False",
		"SPYT_CLUSTER_VERSION":            *speclet.Version,
		"SPYT_HOME":                       "$HOME/./spyt-package",
		"YT_DISCOVERY_ADDRESSES":          discoveryAddresses,
		"YT_PROXY":                        c.cluster,
	}
	env = make(map[string]string)
	for k, v := range globalConf.Environment {
		env[k] = v
	}
	for k, v := range envPatch {
		env[k] = v
	}
	return cmd, env, nil
}

func VersionType(version string) string {
	if strings.Contains(version, "SNAPSHOT") {
		return "snapshots"
	}
	return "releases"
}

type SparkGlobalConf struct {
	Environment map[string]string `yson:"environment"`
}

type SparkLaunchConf struct {
	FilePaths  []string          `yson:"file_paths"`
	LayerPaths []string          `yson:"layer_paths"`
	SparkConf  map[string]string `yson:"spark_conf"`
}

func (c *Controller) ParseGlobalConf(ctx context.Context) (conf SparkGlobalConf, err error) {
	err = c.ytc.GetNode(ctx, ypath.Path("//home/spark/conf/global"), &conf, nil)
	return
}

func (c *Controller) ParseVersionConf(ctx context.Context, speclet *Speclet) (conf SparkLaunchConf, err error) {
	err = c.ytc.GetNode(
		ctx,
		ypath.Path("//home/spark/conf").Child(VersionType(*speclet.Version)).Child(*speclet.Version).Child("spark-launch-conf"),
		&conf,
		nil)
	return
}

func (c *Controller) GetSparkDistrib(ctx context.Context, speclet *Speclet) (name string, path string, err error) {
	var distribFiles []string
	distribRootPath := ypath.Path("//home/spark/distrib").Child(strings.ReplaceAll(speclet.SparkVersionOrDefault(), ".", "/"))
	err = c.ytc.ListNode(ctx, distribRootPath, &distribFiles, nil)
	if err != nil {
		return
	}
	idx := slices.IndexFunc(distribFiles, func(s string) bool { return strings.HasSuffix(s, ".tgz") })
	if idx == -1 {
		return "", "", yterrors.Err("No .tgz distributive found")
	}
	distribName := distribFiles[idx]
	return distribName, distribRootPath.Child(distribName).String(), nil
}

func (c *Controller) Prepare(ctx context.Context, oplet *strawberry.Oplet) (
	spec map[string]any, description map[string]any, annotations map[string]any, err error) {
	alias := oplet.Alias()

	speclet := oplet.ControllerSpeclet().(Speclet)

	if speclet.Version == nil {
		return nil, nil, nil, yterrors.Err("SPYT version is not provided")
	}

	globalConf, err := c.ParseGlobalConf(ctx)
	if err != nil {
		return
	}

	versionConf, err := c.ParseVersionConf(ctx, &speclet)
	if err != nil {
		return
	}

	description = map[string]any{}

	sparkDistribName, sparkDistribPath, err := c.GetSparkDistrib(ctx, &speclet)
	if err != nil {
		return
	}

	command, env, err := c.buildCommand(ctx, &speclet, alias, sparkDistribName, &globalConf, &versionConf)
	if err != nil {
		return
	}

	cpuLimit := 1 + speclet.DriverCPUOrDefault()*speclet.MaxSessionsOrDefault()
	memoryLimit := 1024*1024*1024 + speclet.DriverMemoryOrDefault()*speclet.MaxSessionsOrDefault()
	filePaths := append(versionConf.FilePaths, "//home/spark/livy/livy.tgz", sparkDistribPath)
	spec = map[string]any{
		"tasks": map[string]any{
			"livy": map[string]any{
				"command":                       command,
				"job_count":                     1,
				"cpu_limit":                     cpuLimit,
				"memory_limit":                  memoryLimit,
				"environment":                   env,
				"file_paths":                    filePaths,
				"layer_paths":                   versionConf.LayerPaths,
				"restart_completed_jobs":        true,
				"memory_reserve_factor":         1.0,
				"enable_rpc_proxy_in_job_proxy": true,
			},
		},
		"max_failed_job_count": 1000,
		"title":                "SPYT Livy Server *" + alias,
	}
	annotations = map[string]any{
		"is_spark": true,
	}

	return
}

func (c *Controller) Family() string {
	return "livy"
}

func (c *Controller) Root() ypath.Path {
	return c.root
}

func (c *Controller) ParseSpeclet(specletYson yson.RawValue) (any, error) {
	var speclet Speclet
	err := yson.Unmarshal(specletYson, &speclet)
	if err != nil {
		return nil, yterrors.Err("failed to parse speclet", err)
	}
	return speclet, nil
}

func (c *Controller) DescribeOptions(parsedSpeclet any) []strawberry.OptionGroupDescriptor {
	return nil
}

func (c *Controller) GetOpBriefAttributes(parsedSpeclet any) map[string]any {
	return nil
}

func parseConfig(rawConfig yson.RawValue) Config {
	var controllerConfig Config
	if rawConfig != nil {
		if err := yson.Unmarshal(rawConfig, &controllerConfig); err != nil {
			panic(err)
		}
	}
	return controllerConfig
}

func NewController(l log.Logger, ytc yt.Client, root ypath.Path, cluster string, rawConfig yson.RawValue) strawberry.Controller {
	c := &Controller{
		l:       l,
		ytc:     ytc,
		root:    root,
		cluster: cluster,
		config:  parseConfig(rawConfig),
	}
	return c
}
