package agent

type GPUInfo struct {
	Index uint32
	Minor uint32
	UUID  string
	Model string

	DriverVersion string
	CUDAVersion   string

	PowerLimit uint32 // milli watts
	PowerUsage uint32 // milli watts

	MemoryTotal           uint64 // bytes
	MemoryUsed            uint64 // bytes
	MemoryFree            uint64 // bytes
	MemoryTemperature     uint32 // Celcius
	MemoryMaxTemperature  uint32 // Celcius  (0 if not supported)
	MemoryUtilizationRate uint32 // %

	GPUTemperature     uint32 // Celcius
	GPUMaxTemperature  uint32 // Celcius
	GPUUtilizationRate uint32 // %

	ClockSM    uint32 // MHz
	ClockMaxSM uint32 // MHz

	Slowdowns map[SlowdownType]bool
}

type SlowdownType int

const (
	SlowdownType_HW           = 1
	SlowdownType_HWPowerBrake = 2
	SlowdownType_HWThermal    = 3
	SlowdownType_SWThermal    = 4
)

type GPUProvider interface {
	Init() error
	Shutdown()

	GetGPUDevices() ([]GPUInfo, error)
}
