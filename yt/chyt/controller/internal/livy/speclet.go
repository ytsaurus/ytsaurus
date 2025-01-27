package livy

type Speclet struct {
	DriverCPU    *uint64 `yson:"driver_cores"`
	DriverMemory *uint64 `yson:"driver_memory"`

	MaxSessions *uint64 `yson:"max_sessions"`

	MasterGroupID *string `yson:"master_group_id"`

	SparkMasterAddress *string `yson:"spark_master_address"`

	SparkVersion *string `yson:"spark_version"`
	Version      *string `yson:"spyt_version"`

	EnableSquashfs *bool `yson:"enable_squashfs"`
}

const (
	DefaultDriverCPU    = 1
	DefaultDriverMemory = 1 * 1024 * 1024 * 1024

	DefaultMaxSessions = 3

	DefaultSparkVersion = "3.2.2"

	DefaultEnableSquashfs = false
)

func (speclet *Speclet) DriverCPUOrDefault() uint64 {
	if speclet.DriverCPU != nil {
		return *speclet.DriverCPU
	}
	return DefaultDriverCPU
}

func (speclet *Speclet) DriverMemoryOrDefault() uint64 {
	if speclet.DriverMemory != nil {
		return *speclet.DriverMemory
	}
	return DefaultDriverMemory
}

func (speclet *Speclet) MaxSessionsOrDefault() uint64 {
	if speclet.MaxSessions != nil {
		return *speclet.MaxSessions
	}
	return DefaultMaxSessions
}

func (speclet *Speclet) SparkVersionOrDefault() string {
	if speclet.SparkVersion != nil {
		return *speclet.SparkVersion
	}
	return DefaultSparkVersion
}

func (speclet *Speclet) EnableSquashfsOrDefault() bool {
	if speclet.EnableSquashfs != nil {
		return *speclet.EnableSquashfs
	}
	return DefaultEnableSquashfs
}
