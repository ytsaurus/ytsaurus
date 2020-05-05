package ytrecipe

import "a.yandex-team.ru/yt/go/ypath"

type ResourceLimits struct {
	MemoryLimit int `yson:"memory_limit"`
	CPULimit    int `yson:"cpu_limit"`
	TmpfsSize   int `yson:"tmpfs_size"`
}

type Config struct {
	Cluster string `yson:"cluster"`
	Pool    string `yson:"pool"`

	CachePath     ypath.Path `yson:"cache_path"`
	CacheTTLHours int        `yson:"cache_ttl_hours"`

	OutputPath     ypath.Path `yson:"output_path"`
	OutputTTLHours int        `yson:"output_ttl_hours"`

	TmpPath ypath.Path `yson:"tmp_path"`

	ResourceLimits ResourceLimits `yson:"resource_limits"`

	UploadBinaries []string `yson:"upload_binaries"`
	UploadDirs     []string `yson:"upload_dirs"`
}

const (
	jobMemoryReserve = 128 * (1 << 20)
)

var DefaultConfig = Config{
	OutputPath:     "//tmp",
	OutputTTLHours: 1,

	CachePath:     "//tmp",
	CacheTTLHours: 1,

	TmpPath: "//tmp",

	ResourceLimits: ResourceLimits{
		MemoryLimit: 512 * (1 << 20),
	},
}
