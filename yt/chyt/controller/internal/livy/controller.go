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
	globalConf *SparkGlobalConf, conf *SparkLaunchConf, networkProject *string) (command string, env map[string]string, err error) {

	ipV6Enabled, found := conf.SparkConf["spark.hadoop.yt.preferenceIpv6.enabled"]
	if !found {
		ipV6Enabled = "false"
	}
	var classPath string
	if speclet.EnableSquashfsOrDefault() {
		classPath = "/usr/lib/spyt/conf/:/usr/lib/spyt/jars/*:/usr/lib/spark/jars/*"
	} else {
		classPath = "spyt-package/conf/:spyt-package/jars/*:spark/jars/*"
	}

	javaOpts := []string{
		"-Xmx512m",
		"-cp",
		classPath,
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
	if networkProject != nil {
		livyOpts = append(livyOpts, fmt.Sprintf("--network-project %v", *networkProject))
	}
	var envCommand string
	var sparkHome string
	var spytHome string
	var livyHome string
	livyWorkDir := "$HOME/./livy"
	if speclet.EnableSquashfsOrDefault() {
		envCommand = "./setup-spyt-env.sh --use-squashfs"
		livyOpts = append(livyOpts, "--enable-squashfs")
		sparkHome = "/usr/lib/spark"
		spytHome = "/usr/lib/spyt"
		livyHome = "/usr/lib/livy"
	} else {
		envCommand = fmt.Sprintf("./setup-spyt-env.sh --spark-home . --enable-livy --spark-distributive %v", sparkDistribName)
		sparkHome = "$HOME/./spark"
		spytHome = "$HOME/./spyt-package"
		livyHome = livyWorkDir
	}
	launcherCommand := "$JAVA_HOME/bin/java " + strings.Join(javaOpts[:], " ") +
		" tech.ytsaurus.spark.launcher.LivyLauncher " + strings.Join(livyOpts[:], " ")
	cmd := envCommand + " && " + launcherCommand

	discoveryAddresses, err := getDiscoveryServerAddresses(ctx, c.ytc)
	if err != nil {
		return
	}
	masterGroupID := alias
	if speclet.MasterGroupID != nil {
		masterGroupID = *speclet.MasterGroupID
	}
	envPatch := map[string]string{
		"LIVY_HOME":                       livyHome,
		"LIVY_WORK_DIR":                   livyWorkDir,
		"SPARK_DISCOVERY_GROUP_ID":        alias,
		"SPARK_MASTER_DISCOVERY_GROUP_ID": masterGroupID,
		"SPARK_HOME":                      sparkHome,
		"SPARK_YT_RPC_JOB_PROXY_ENABLED":  "True",
		"SPARK_YT_SOLOMON_ENABLED":        "False",
		"SPARK_YT_TCP_PROXY_ENABLED":      "False",
		"SPYT_CLUSTER_VERSION":            *speclet.Version,
		"SPYT_HOME":                       spytHome,
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
	if strings.Contains(version, "-alpha-") ||
		strings.Contains(version, "-beta-") ||
		strings.Contains(version, "-rc-") {
		return "pre-releases"
	}
	return "releases"
}

type SparkGlobalConf struct {
	Environment map[string]string `yson:"environment"`
}

type SparkLaunchConf struct {
	FilePaths          []string          `yson:"file_paths"`
	LayerPaths         []string          `yson:"layer_paths"`
	SquashfsLayerPaths []string          `yson:"squashfs_layer_paths"`
	SparkConf          map[string]string `yson:"spark_conf"`
	BasePath           string            `yson:"spark_yt_base_path"`
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

	command, env, err := c.buildCommand(ctx, &speclet, alias, sparkDistribName, &globalConf, &versionConf,
		oplet.StrawberrySpeclet().NetworkProject)
	if err != nil {
		return
	}

	cpuLimit := 1 + speclet.DriverCPUOrDefault()*speclet.MaxSessionsOrDefault()
	memoryLimit := 1024*1024*1024 + speclet.DriverMemoryOrDefault()*speclet.MaxSessionsOrDefault()

	filePaths := versionConf.FilePaths
	var layerPaths []string

	if speclet.EnableSquashfsOrDefault() {
		sparkSquashfsPath := strings.Replace(sparkDistribPath, ".tgz", ".squashfs", 1)
		spytSquashfsPath := versionConf.BasePath + "/spyt-package.squashfs"
		layerPaths = append(layerPaths, "//home/spark/livy/livy.squashfs", sparkSquashfsPath, spytSquashfsPath)
		layerPaths = append(layerPaths, versionConf.SquashfsLayerPaths...)
	} else {
		layerPaths = versionConf.LayerPaths
		filePaths = append(filePaths, "//home/spark/livy/livy.tgz", sparkDistribPath)
	}

	spec = map[string]any{
		"tasks": map[string]any{
			"livy": map[string]any{
				"command":                       command,
				"job_count":                     1,
				"cpu_limit":                     cpuLimit,
				"memory_limit":                  memoryLimit,
				"environment":                   env,
				"file_paths":                    filePaths,
				"layer_paths":                   layerPaths,
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

func (c *Controller) GetScalerTarget(ctx context.Context, opletInfo strawberry.OpletInfoForScaler) (*strawberry.ScalerTarget, error) {
	return nil, nil
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
