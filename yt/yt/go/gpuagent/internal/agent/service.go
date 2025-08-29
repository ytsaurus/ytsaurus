package agent

import (
	"context"

	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	"go.ytsaurus.tech/yt/yt/go/gpuagent/internal/pb"
)

type GPUAgentService interface {
	pb.GpuAgentServer
}

type gpuAgentService struct {
	pb.UnimplementedGpuAgentServer

	p GPUProvider
	l *logzap.Logger
}

var _ pb.GpuAgentServer = (*gpuAgentService)(nil)

func New(p GPUProvider, l *logzap.Logger) (GPUAgentService, error) {
	return &gpuAgentService{
		p: p,
		l: l,
	}, nil
}

func (s *gpuAgentService) ListGpuDevices(context.Context, *pb.ListGpuDevicesRequest) (*pb.ListGpuDevicesResponse, error) {
	devices, err := s.p.GetGPUDevices()
	if err != nil {
		return nil, err
	}

	ret := &pb.ListGpuDevicesResponse{
		Devices: make([]*pb.GpuDevice, 0, len(devices)),
	}

	for _, d := range devices {
		s.l.Debugf("device: %+v", d)
		ret.Devices = append(ret.Devices, &pb.GpuDevice{
			Index:                 d.Index,
			Minor:                 d.Minor,
			Uuid:                  d.UUID,
			Model:                 d.Model,
			DriverVersion:         d.DriverVersion,
			CudaVersion:           d.CUDAVersion,
			PowerLimit:            d.PowerLimit,
			PowerUsage:            d.PowerUsage,
			MemoryTotal:           d.MemoryTotal,
			MemoryUsed:            d.MemoryUsed,
			MemoryFree:            d.MemoryFree,
			MemoryTemperature:     d.MemoryTemperature,
			MemoryMaxTemperature:  d.MemoryMaxTemperature,
			MemoryUtilizationRate: d.MemoryUtilizationRate,
			GpuTemperature:        d.GPUTemperature,
			GpuMaxTemperature:     d.GPUMaxTemperature,
			GpuUtilizationRate:    d.GPUUtilizationRate,
			ClockSm:               d.ClockSM,
			ClockMaxSm:            d.ClockMaxSM,
			Slowdowns:             convertSlowdowns(&d),
		})
	}

	return ret, nil
}

func convertSlowdowns(gpu *GPUInfo) []pb.SlowdownType {
	ret := make([]pb.SlowdownType, 0, len(gpu.Slowdowns))
	for t, v := range gpu.Slowdowns {
		if v {
			ret = append(ret, convertSlowdownType(t))
		}
	}
	return ret
}

func convertSlowdownType(t SlowdownType) pb.SlowdownType {
	switch t {
	case SlowdownType_HW:
		return pb.SlowdownType_HW
	case SlowdownType_HWPowerBrake:
		return pb.SlowdownType_HWPowerBrake
	case SlowdownType_HWThermal:
		return pb.SlowdownType_HWThermal
	case SlowdownType_SWThermal:
		return pb.SlowdownType_SWThermal
	}
	return pb.SlowdownType_SLOWDOWN_TYPE_UNSPECIFIED
}
