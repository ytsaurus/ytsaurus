package ytrecipe

import (
	"os"
	"time"

	"a.yandex-team.ru/library/go/test/yatest"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/ytexec"
)

type ResourceLimits struct {
	MemoryLimit int `yson:"memory_limit"`
	CPULimit    int `yson:"cpu_limit"`
	TmpfsSize   int `yson:"tmpfs_size"`
}

type Config struct {
	Cluster string `yson:"cluster"`
	Pool    string `yson:"pool"`

	CachePath        ypath.Path `yson:"cache_path"`
	CacheTTLHours    int        `yson:"cache_ttl_hours"`
	CoordinateUpload bool       `yson:"coordinate_upload"`

	OutputPath     ypath.Path `yson:"output_path"`
	OutputTTLHours int        `yson:"output_ttl_hours"`

	UploadTimeoutSeconds int `yson:"upload_timeout_seconds"`
	JobTimeoutSeconds    int `yson:"job_timeout_seconds"`

	TmpPath ypath.Path `yson:"tmp_path"`

	ResourceLimits ResourceLimits `yson:"resource_limits"`

	UploadBinaries []string `yson:"upload_binaries"`
	UploadWorkFile []string `yson:"upload_workfile"`
	UploadWorkDir  []string `yson:"upload_workdir"`

	SpecPatch interface{} `yson:"spec_patch"`
	TaskPatch interface{} `yson:"task_patch"`
}

func (c *Config) CacheTTL() time.Duration {
	return time.Duration(c.CacheTTLHours) * time.Hour
}

func (c *Config) UploadTimeout() time.Duration {
	return time.Duration(c.UploadTimeoutSeconds) * time.Second
}

func (c *Config) FillConfig(e *ytexec.Config) error {
	e.Operation.Pool = c.Pool
	e.Operation.Cluster = c.Cluster
	e.Operation.CPULimit = float64(c.ResourceLimits.CPULimit)
	e.Operation.MemoryLimit = c.ResourceLimits.MemoryLimit
	e.Operation.CypressRoot = c.CachePath
	e.Operation.CoordinateUpload = c.CoordinateUpload
	e.Operation.EnablePorto = true
	e.Operation.EnableNetwork = false
	e.Operation.OutputTTL = time.Duration(c.OutputTTLHours) * time.Hour
	e.Operation.BlobTTL = time.Duration(c.CacheTTLHours) * time.Hour
	e.Operation.Timeout = time.Hour
	e.Operation.SpecPatch = c.SpecPatch
	e.Operation.TaskPatch = c.TaskPatch

	for _, path := range c.UploadBinaries {
		e.FS.UploadFile = append(e.FS.UploadFile, yatest.BuildPath(path))
	}

	e.Cmd.SIGUSR2Timeout = time.Duration(c.JobTimeoutSeconds) * time.Second
	e.Cmd.SIGQUITTimeout = (time.Duration(c.JobTimeoutSeconds) * time.Second) + time.Minute
	e.Cmd.SIGKILLTimeout = (time.Duration(c.JobTimeoutSeconds) * time.Second) + time.Minute + time.Second

	for _, path := range c.UploadWorkDir {
		realPath, err := os.Readlink(yatest.WorkPath(path))
		if err != nil {
			return err
		}

		e.FS.UploadTarDir = append(e.FS.UploadTarDir, realPath)
	}

	return nil
}

var DefaultConfig = Config{
	OutputPath:     "//tmp",
	OutputTTLHours: 1,

	CachePath:     "//tmp",
	CacheTTLHours: 1,

	UploadTimeoutSeconds: 180,

	TmpPath: "//tmp",

	ResourceLimits: ResourceLimits{
		MemoryLimit: 512 * (1 << 20),
	},
}
