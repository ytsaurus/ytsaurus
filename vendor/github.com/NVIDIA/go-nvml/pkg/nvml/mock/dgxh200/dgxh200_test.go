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

package dgxh200

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/NVIDIA/go-nvml/pkg/nvml/mock/server"
)

// Compile-time interface checks
var _ nvml.Interface = (*server.Server)(nil)
var _ nvml.ExtendedInterface = (*server.Server)(nil)

func TestH200Server(t *testing.T) {
	server := New()

	count, ret := server.DeviceGetCount()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 8, count)

	device, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, device)

	name, ret := device.GetName()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, "NVIDIA H200 141GB HBM3e", name)

	arch, ret := device.GetArchitecture()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, nvml.DeviceArchitecture(nvml.DEVICE_ARCH_HOPPER), arch)

	major, minor, ret := device.GetCudaComputeCapability()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 9, major)
	require.Equal(t, 0, minor)

	memory, ret := device.GetMemoryInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint64(144384*1024*1024), memory.Total) // 141GB

	// Test H200 supports P2P in MIG (IsP2pSupported should be 1)
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

	// Test compute instance creation
	ciProfileInfo, ret := gpuInstance.GetComputeInstanceProfileInfo(nvml.COMPUTE_INSTANCE_PROFILE_1_SLICE, nvml.COMPUTE_INSTANCE_ENGINE_PROFILE_SHARED)
	require.Equal(t, nvml.SUCCESS, ret)

	computeInstance, ret := gpuInstance.CreateComputeInstance(&ciProfileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, computeInstance)

	ciInfo, ret := computeInstance.GetInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(0), ciInfo.Id)
	require.Equal(t, uint32(nvml.COMPUTE_INSTANCE_PROFILE_1_SLICE), ciInfo.ProfileId)
}

func TestH200Device(t *testing.T) {
	device := NewDevice(3)

	index, ret := device.GetIndex()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 3, index)

	minor, ret := device.GetMinorNumber()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 3, minor)

	uuid, ret := device.GetUUID()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Contains(t, uuid, "GPU-")

	brand, ret := device.GetBrand()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, nvml.BRAND_NVIDIA, brand)

	pciInfo, ret := device.GetPciInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(0x233310DE), pciInfo.PciDeviceId)
}

func TestH200MIGProfiles(t *testing.T) {
	device := NewDevice(0)

	// Test all GPU instance profiles
	testCases := []struct {
		profile    int
		sliceCount uint32
		memoryMB   uint64
		multiproc  uint32
	}{
		{nvml.GPU_INSTANCE_PROFILE_1_SLICE, 1, 18432, 16},
		{nvml.GPU_INSTANCE_PROFILE_2_SLICE, 2, 35840, 32},
		{nvml.GPU_INSTANCE_PROFILE_3_SLICE, 3, 72704, 48},
		{nvml.GPU_INSTANCE_PROFILE_4_SLICE, 4, 72704, 64},
		{nvml.GPU_INSTANCE_PROFILE_7_SLICE, 7, 144384, 112},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("profile_%d_slice", tc.sliceCount), func(t *testing.T) {
			profileInfo, ret := device.GetGpuInstanceProfileInfo(tc.profile)
			require.Equal(t, nvml.SUCCESS, ret)
			require.Equal(t, uint32(tc.profile), profileInfo.Id)
			require.Equal(t, tc.sliceCount, profileInfo.SliceCount)
			require.Equal(t, tc.memoryMB, profileInfo.MemorySizeMB)
			require.Equal(t, tc.multiproc, profileInfo.MultiprocessorCount)
			require.Equal(t, uint32(1), profileInfo.IsP2pSupported) // H200 supports P2P
		})
	}
}

func TestH200MIGInstanceCreation(t *testing.T) {
	device := NewDevice(0)

	// Test creating multiple GPU instances of different profiles
	profileInfo1, ret := device.GetGpuInstanceProfileInfo(nvml.GPU_INSTANCE_PROFILE_1_SLICE)
	require.Equal(t, nvml.SUCCESS, ret)

	gi1, ret := device.CreateGpuInstance(&profileInfo1)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, gi1)

	gi2, ret := device.CreateGpuInstance(&profileInfo1)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, gi2)

	// Verify they have different IDs
	gi1Info, ret := gi1.GetInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	gi2Info, ret := gi2.GetInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotEqual(t, gi1Info.Id, gi2Info.Id)

	// Test that we can create compute instances on each GPU instance
	ciProfileInfo, ret := gi1.GetComputeInstanceProfileInfo(nvml.COMPUTE_INSTANCE_PROFILE_1_SLICE, nvml.COMPUTE_INSTANCE_ENGINE_PROFILE_SHARED)
	require.Equal(t, nvml.SUCCESS, ret)

	ci1, ret := gi1.CreateComputeInstance(&ciProfileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, ci1)

	ci2, ret := gi2.CreateComputeInstance(&ciProfileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, ci2)
}
