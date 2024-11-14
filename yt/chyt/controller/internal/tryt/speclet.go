package tryt

type Speclet struct {
	CPU    *uint64 `yson:"cpu"`
	Memory *uint64 `yson:"memory"`

	DockerImage string `yson:"docker_image"`

	TransferType      string `yson:"transfer_type"`
	SourceType        string `yson:"src_type"`
	SourceParams      string `yson:"src_params"`
	DestinationType   string `yson:"dst_type"`
	DestinationParams string `yson:"dst_params"`
}

const (
	gib           = 1024 * 1024 * 1024
	DefaultCPU    = 2
	DefaultMemory = 8 * gib
)

func (s *Speclet) CPUOrDefault() uint64 {
	if s.CPU != nil {
		return *s.CPU
	}
	return DefaultCPU
}

func (s *Speclet) MemoryOrDefault() uint64 {
	if s.Memory != nil {
		return *s.Memory
	}
	return DefaultMemory
}
