/*
 * Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dgxb200

import (
	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/NVIDIA/go-nvml/pkg/nvml/mock/gpus"
	"github.com/NVIDIA/go-nvml/pkg/nvml/mock/server"
)

func New() *server.Server {
	return NewWithGPUs(gpus.Multiple(8, gpus.B200_SXM5_180GB)...)
}

func NewWithGPUs(gpus ...gpus.Config) *server.Server {
	s, _ := server.New(
		server.WithGPUs(gpus...),
		server.WithDriverVersion("560.28.03"),
		server.WithNVMLVersion("12.560.28.03"),
		server.WithCUDADriverVersion(12060),
	)
	return s
}

func NewDevice(index int) *server.Device {
	return server.NewDeviceFromConfig(gpus.B200_SXM5_180GB, index)
}

func NewGpuInstance(info nvml.GpuInstanceInfo) *server.GpuInstance {
	return server.NewGpuInstanceFromInfo(info, gpus.B200_SXM5_180GB.MIGProfiles)
}

func NewComputeInstance(info nvml.ComputeInstanceInfo) *server.ComputeInstance {
	return server.NewComputeInstanceFromInfo(info)
}

// Legacy globals for backward compatibility - expose the internal data
var (
	MIGProfiles = struct {
		GpuInstanceProfiles     map[int]nvml.GpuInstanceProfileInfo
		ComputeInstanceProfiles map[int]map[int]nvml.ComputeInstanceProfileInfo
	}{
		GpuInstanceProfiles:     gpus.B200_SXM5_180GB.MIGProfiles.GpuInstanceProfiles,
		ComputeInstanceProfiles: gpus.B200_SXM5_180GB.MIGProfiles.ComputeInstanceProfiles,
	}

	MIGPlacements = struct {
		GpuInstancePossiblePlacements     map[int][]nvml.GpuInstancePlacement
		ComputeInstancePossiblePlacements map[int]map[int][]nvml.ComputeInstancePlacement
	}{
		GpuInstancePossiblePlacements:     gpus.B200_SXM5_180GB.MIGProfiles.GpuInstancePlacements,
		ComputeInstancePossiblePlacements: gpus.B200_SXM5_180GB.MIGProfiles.ComputeInstancePlacements,
	}
)
