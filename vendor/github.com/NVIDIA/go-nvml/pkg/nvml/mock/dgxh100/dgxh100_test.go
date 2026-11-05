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

package dgxh100

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/NVIDIA/go-nvml/pkg/nvml/mock/server"
)

// Compile-time interface checks
var _ nvml.Interface = (*server.Server)(nil)
var _ nvml.ExtendedInterface = (*server.Server)(nil)

func TestH100Server(t *testing.T) {
	server := New()

	count, ret := server.DeviceGetCount()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 8, count)

	device, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, device)

	name, ret := device.GetName()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, "NVIDIA H100 80GB HBM3", name)

	arch, ret := device.GetArchitecture()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, nvml.DeviceArchitecture(nvml.DEVICE_ARCH_HOPPER), arch)

	major, minor, ret := device.GetCudaComputeCapability()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 9, major)
	require.Equal(t, 0, minor)

	memory, ret := device.GetMemoryInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint64(81920*1024*1024), memory.Total) // 80GB

	// Test H100 supports P2P in MIG (IsP2pSupported should be 1)
	profileInfo, ret := device.GetGpuInstanceProfileInfo(nvml.GPU_INSTANCE_PROFILE_1_SLICE)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(1), profileInfo.IsP2pSupported)

	// Test MIG functionality
	gpuInstance, ret := device.CreateGpuInstance(&profileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, gpuInstance)

	giInfo, ret := gpuInstance.GetInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(0), giInfo.Id)
	require.Equal(t, uint32(nvml.GPU_INSTANCE_PROFILE_1_SLICE), giInfo.ProfileId)
}
