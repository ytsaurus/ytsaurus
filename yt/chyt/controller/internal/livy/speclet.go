package livy

type Speclet struct {
	DriverCPU    *uint64 `yson:"driver_cores"`
	DriverMemory *uint64 `yson:"driver_memory"`

	MaxSessions *uint64 `yson:"max_sessions"`

	masterGroupID *string `yson:"master_group_id"`

	SparkMasterAddress *string `yson:"spark_master_address"`

	SparkVersion *string `yson:"spark_version"`
	Version      *string `yson:"spyt_version"`
}

const (
	DefaultDriverCPU    = 1
	DefaultDriverMemory = 1 * 1024 * 1024 * 1024

	DefaultMaxSessions = 3

	DefaultSparkVersion = "3.2.2"
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
