package jupyt

type Speclet struct {
	CPU    *uint64 `yson:"cpu"`
	Memory *uint64 `yson:"memory"`

	JupyterDockerImage string `yson:"jupyter_docker_image"`
}

const (
	gib           = 1024 * 1024 * 1024
	DefaultCPU    = 2
	DefaultMemory = 8 * gib
)

func (speclet *Speclet) CPUOrDefault() uint64 {
	if speclet.CPU != nil {
		return *speclet.CPU
	}
	return DefaultCPU
}

func (speclet *Speclet) MemoryOrDefault() uint64 {
	if speclet.Memory != nil {
		return *speclet.Memory
	}
	return DefaultMemory
}
