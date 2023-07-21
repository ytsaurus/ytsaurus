package jupyt

type Speclet struct {
	CPU    *uint64 `yson:"cpu"`
	Memory *uint64 `yson:"memory"`

	PythonVersion  string  `yson:"python_version"`
	TrampolinePath *string `yson:"trampoline_path""`
	VenvPath       *string `yson:"venv_path""`
}

const (
	DefaultCPU            = 2
	DefaultMemory         = 8 * 1024 * 1024 * 1024
	DefaultTrampolinePath = "jupyter-trampoline.sh"
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

func (speclet *Speclet) TrampolinePathOrDefault() string {
	if speclet.TrampolinePath != nil {
		return *speclet.TrampolinePath
	}
	return DefaultTrampolinePath
}
