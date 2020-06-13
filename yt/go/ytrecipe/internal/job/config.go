package job

import (
	"time"

	"a.yandex-team.ru/yt/go/ypath"
)

const (
	DefaultBaseLayer     = ypath.Path("//porto_layers/base/xenial/porto_layer_search_ubuntu_xenial_app_lastest.tar.gz")
	MemoryReserve        = 128 * (1 << 20)
	OperationTimeReserve = time.Minute * 5
)

type OperationConfig struct {
	Cluster string `json:"cluster"`
	Pool    string `json:"pool"`
	Title   string `json:"title"`

	CypressRoot      ypath.Path    `json:"cypress_root"`
	OutputTTL        time.Duration `json:"output_ttl"`
	BlobTTL          time.Duration `json:"blob_ttl"`
	CoordinateUpload bool          `json:"coordinate_upload"`

	CPULimit    float64 `json:"cpu_limit"`
	MemoryLimit int     `json:"memory_limit"`

	Timeout time.Duration `json:"timeout"`

	EnablePorto   bool `json:"enable_porto"`
	EnableNetwork bool `json:"enable_network"`

	SpecPatch interface{} `json:"spec_patch"`
	TaskPatch interface{} `json:"task_patch"`
}

const (
	OutputDir = "output"
	CacheDir  = "cache"
	TmpDir    = "tmp"
)

func (c *OperationConfig) TmpDir() ypath.Path {
	return c.CypressRoot.Child(TmpDir)
}

func (c *OperationConfig) CacheDir() ypath.Path {
	return c.CypressRoot.Child(CacheDir)
}

func (c *OperationConfig) OutputDir() ypath.Path {
	return c.CypressRoot.Child(OutputDir)
}

type Cmd struct {
	Args    []string `json:"args"`
	Cwd     string   `json:"cwd"`
	Environ []string `json:"environ"`

	SIGUSR2Timeout time.Duration `json:"sigusr2_timeout"`
	SIGQUITTimeout time.Duration `json:"sigquit_timeout"`
	SIGKILLTimeout time.Duration `json:"sigkill_timeout"`
}
