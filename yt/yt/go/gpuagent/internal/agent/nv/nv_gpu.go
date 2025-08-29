package nv

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/NVIDIA/go-nvml/pkg/nvml"

	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/yt/go/gpuagent/internal/agent"
)

type NVGPUProvider interface {
	agent.GPUProvider
}

func New(l *logzap.Logger) (NVGPUProvider, error) {
	return &nvGPUProvider{l: l}, nil
}

type semVer struct {
	major int
	minor int
	patch int
	tail  string
}

type nvGPUProvider struct {
	l             *logzap.Logger
	driverVersion string
	driverSemVer  semVer
	cudaVersion   string
}

func (p *nvGPUProvider) Init() error {
	p.l.Info("Init NVML...")
	ret := nvml.Init()
	if ret != nvml.SUCCESS {
		return fmt.Errorf("Unable to initialize NVML: %v", nvml.ErrorString(ret))
	}

	p.driverVersion, ret = nvml.SystemGetDriverVersion()
	if ret != nvml.SUCCESS {
		return fmt.Errorf("Unable to get driver version: %v", nvml.ErrorString(ret))
	}

	var err error
	p.driverSemVer, err = newSemVer(p.driverVersion)
	if err != nil {
		return fmt.Errorf("Unable to parse driver version: %v", nvml.ErrorString(ret))
	}

	cudaVersion, ret := nvml.SystemGetCudaDriverVersion()
	if ret != nvml.SUCCESS {
		return fmt.Errorf("Unable to get CUDA driver version: %v", nvml.ErrorString(ret))
	}
	p.cudaVersion = fmt.Sprintf("%d", cudaVersion)

	p.l.Infof("Driver: %s, CUDA: %s", p.driverVersion, p.cudaVersion)

	return nil
}

func (p *nvGPUProvider) Shutdown() {
	ret := nvml.Shutdown()
	if ret != nvml.SUCCESS {
		p.l.Errorf("Unable to shutdown NVML: %v", nvml.ErrorString(ret))
	}
}

func (p *nvGPUProvider) GetGPUDevices() ([]agent.GPUInfo, error) {
	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("Unable to get device count: %v", nvml.ErrorString(ret))
	}
	p.l.Debugf("Found %d devices", count)

	devices := make([]agent.GPUInfo, 0, count)

	for i := range count {
		device, err := p.getGPUDevice(i)
		if err != nil {
			p.l.Errorf("%v", err)
		} else {
			devices = append(devices, *device)
		}
	}

	return devices, nil
}

func (p *nvGPUProvider) getGPUDevice(idx int) (*agent.GPUInfo, error) {
	d := &agent.GPUInfo{
		Index:         uint32(idx),
		DriverVersion: p.driverVersion,
		CUDAVersion:   p.cudaVersion,
	}

	device, ret := nvml.DeviceGetHandleByIndex(idx)
	if ret != nvml.SUCCESS {
		return nil, fmt.Errorf("Unable to get device at index %d: %v", idx, nvml.ErrorString(ret))
	}

	{
		minor, ret := device.GetMinorNumber()
		if ret != nvml.SUCCESS {
			return nil, formatError("minor number", idx, ret)
		}
		d.Minor = uint32(minor)
	}

	{
		d.UUID, ret = device.GetUUID()
		if ret != nvml.SUCCESS {
			return nil, formatError("uuid", idx, ret)
		}
	}

	{
		d.Model, ret = device.GetName()
		if ret != nvml.SUCCESS {
			return nil, formatError("name", idx, ret)
		}
	}

	{
		d.PowerLimit, ret = device.GetPowerManagementLimit()
		if ret != nvml.SUCCESS {
			err := formatError("power management limit", idx, ret)
			if isNotSupported(ret) {
				p.l.Debugf("%v", err)
				d.PowerLimit = 0
			} else {
				return nil, err
			}
		}
	}

	{
		d.PowerUsage, ret = device.GetPowerUsage()
		if ret != nvml.SUCCESS {
			err := formatError("power usage", idx, ret)
			if isNotSupported(ret) {
				p.l.Debugf("%v", err)
				d.PowerUsage = 0
			} else {
				return nil, err
			}
		}
	}

	{
		mi2, ret := device.GetMemoryInfo_v2()
		if ret != nvml.SUCCESS {
			mi, ret := device.GetMemoryInfo()
			if ret != nvml.SUCCESS {
				return nil, formatError("memory info", idx, ret)
			}
			d.MemoryTotal = mi.Total
			d.MemoryUsed = mi.Used
			d.MemoryFree = mi.Free
		} else {
			d.MemoryTotal = mi2.Total
			d.MemoryUsed = mi2.Used
			d.MemoryFree = mi2.Free
		}
	}

	{
		vals := []nvml.FieldValue{{FieldId: nvml.FI_DEV_MEMORY_TEMP}}
		ret = device.GetFieldValues(vals)
		if ret != nvml.SUCCESS {
			if isNotSupported(ret) {
				p.l.Debugf("%v", formatError("memory temp", idx, ret))
				d.MemoryTemperature = 0
			} else {
				return nil, formatError("memory temp", idx, ret)
			}
		} else {
			if nvml.Return(vals[0].NvmlReturn) != nvml.SUCCESS {
				p.l.Debugf("%v", formatError("FI_DEV_MEMORY_TEMP", idx, nvml.Return(vals[0].NvmlReturn)))
			} else {
				d.MemoryTemperature = binary.LittleEndian.Uint32(vals[0].Value[0:4])
			}
		}
	}

	{
		d.MemoryMaxTemperature, ret = device.GetTemperatureThreshold(nvml.TEMPERATURE_THRESHOLD_MEM_MAX)
		if ret != nvml.SUCCESS {
			err := formatError("max memory temp", idx, ret)
			if isNotSupported(ret) {
				p.l.Debugf("%v", err)
			} else {
				return nil, err
			}
		}
	}

	{
		util, ret := device.GetUtilizationRates()
		if ret != nvml.SUCCESS {
			err := formatError("utilization rates", idx, ret)
			if isNotSupported(ret) {
				p.l.Debugf("%v", err)
			} else {
				return nil, err
			}
		} else {
			d.MemoryUtilizationRate = util.Memory
			d.GPUUtilizationRate = util.Gpu
		}

	}

	{
		d.GPUTemperature, ret = device.GetTemperature(nvml.TEMPERATURE_GPU)
		if ret != nvml.SUCCESS {
			err := formatError("gpu temperature", idx, ret)
			if isNotSupported(ret) {
				p.l.Debugf("%v", err)
				d.GPUTemperature = 0
			} else {
				return nil, err
			}
		}
	}

	{
		d.GPUMaxTemperature, ret = device.GetTemperatureThreshold(nvml.TEMPERATURE_THRESHOLD_GPU_MAX)
		if ret != nvml.SUCCESS {
			err := formatError("gpu max temperature", idx, ret)
			if isNotSupported(ret) {
				p.l.Debugf("%v", err)
				d.GPUMaxTemperature = 0
			} else {
				return nil, err
			}
		}
	}

	{
		d.ClockSM, ret = device.GetClockInfo(nvml.CLOCK_SM)
		if ret != nvml.SUCCESS {
			err := formatError("clock sm", idx, ret)
			if isNotSupported(ret) {
				p.l.Debugf("%v", err)
				d.ClockSM = 0
			} else {
				return nil, err
			}
		}
	}

	{
		d.ClockMaxSM, ret = device.GetMaxClockInfo(nvml.CLOCK_SM)
		if ret != nvml.SUCCESS {
			err := formatError("clock max sm", idx, ret)
			if isNotSupported(ret) {
				p.l.Debugf("%v", err)
				d.ClockMaxSM = 0
			} else {
				return nil, err
			}
		}
	}

	{
		var eventReason uint64 = 0
		if p.isClocksEventReasonsSupported() {
			eventReason, ret = device.GetCurrentClocksEventReasons()
			p.l.Debugf("clocks event reasons: %d", eventReason)
			if ret != nvml.SUCCESS {
				err := formatError("clocks event reasons", idx, ret)
				if isNotSupported(ret) {
					p.l.Debugf("%v", err)
					eventReason = 0
				} else {
					return nil, err
				}
			}
		} else {
			p.l.Debug("ClocksEventReasons NOT Supported")
		}

		d.Slowdowns = make(map[agent.SlowdownType]bool)
		d.Slowdowns[agent.SlowdownType_HW] = eventReason&nvml.ClocksThrottleReasonHwSlowdown != 0
		d.Slowdowns[agent.SlowdownType_HWPowerBrake] = eventReason&nvml.ClocksThrottleReasonHwPowerBrakeSlowdown != 0
		d.Slowdowns[agent.SlowdownType_HWThermal] = eventReason&nvml.ClocksThrottleReasonHwThermalSlowdown != 0
		d.Slowdowns[agent.SlowdownType_SWThermal] = eventReason&nvml.ClocksEventReasonSwThermalSlowdown != 0
	}

	return d, nil
}

func (p *nvGPUProvider) isClocksEventReasonsSupported() bool {
	// nvmlDeviceGetCurrentClocksEventReasons has been introduced in driver version 535+
	return p.driverSemVer.major >= 535
}

func isNotSupported(ret nvml.Return) bool {
	return ret == nvml.ERROR_NOT_SUPPORTED
}

func formatError(what string, idx int, ret nvml.Return) error {
	return fmt.Errorf("Unable to get %s of device at index %d: %v", what, idx, nvml.ErrorString(ret))
}

func newSemVer(v string) (semVer, error) {
	sv := semVer{}
	parts := strings.SplitN(v, ".", 4)

	if len(parts) == 0 {
		return sv, fmt.Errorf("Unsupported version format %q", v)
	}

	x, err := strconv.Atoi(parts[0])
	if err != nil {
		return sv, fmt.Errorf("Unsupported version format %q", v)
	}
	sv.major = x

	if len(parts) > 1 {
		x, err := strconv.Atoi(parts[1])
		if err != nil {
			return sv, fmt.Errorf("Unsupported version format %q", v)
		}
		sv.minor = x
	}

	if len(parts) > 2 {
		x, err := strconv.Atoi(parts[2])
		if err != nil {
			return sv, fmt.Errorf("Unsupported version format %q", v)
		}
		sv.patch = x
	}

	if len(parts) > 3 {
		sv.tail = parts[3]
	}

	return sv, nil
}
