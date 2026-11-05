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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/NVIDIA/go-nvml/pkg/nvml/mock/server"
)

// Compile-time interface checks
var _ nvml.Interface = (*server.Server)(nil)
var _ nvml.ExtendedInterface = (*server.Server)(nil)

func TestB200Server(t *testing.T) {
	server := New()

	count, ret := server.DeviceGetCount()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 8, count)

	device, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, device)

	name, ret := device.GetName()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, "NVIDIA B200 180GB HBM3e", name)

	arch, ret := device.GetArchitecture()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, nvml.DeviceArchitecture(nvml.DEVICE_ARCH_BLACKWELL), arch)

	major, minor, ret := device.GetCudaComputeCapability()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 10, major)
	require.Equal(t, 0, minor)

	memory, ret := device.GetMemoryInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint64(184320*1024*1024), memory.Total) // 180GB

	// Test B200 supports P2P in MIG (IsP2pSupported should be 1)
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

// TODO: This should be moved to the server / gpus packages.
// func TestB200Device(t *testing.T) {
// 	device := NewDevice(5)

// 	index, ret := device.GetIndex()
// 	require.Equal(t, nvml.SUCCESS, ret)
// 	require.Equal(t, 5, index)

// 	minor, ret := device.GetMinorNumber()
// 	require.Equal(t, nvml.SUCCESS, ret)
// 	require.Equal(t, 5, minor)

// 	uuid, ret := device.GetUUID()
// 	require.Equal(t, nvml.SUCCESS, ret)
// 	require.Contains(t, uuid, "GPU-")

// 	brand, ret := device.GetBrand()
// 	require.Equal(t, nvml.SUCCESS, ret)
// 	require.Equal(t, nvml.BRAND_NVIDIA, brand)

// 	pciInfo, ret := device.GetPciInfo()
// 	require.Equal(t, nvml.SUCCESS, ret)
// 	require.Equal(t, uint32(0x2B0010DE), pciInfo.PciDeviceId)
// }

func TestB200MIGProfiles(t *testing.T) {
	device := NewDevice(0)

	// Test all GPU instance profiles
	testCases := []struct {
		profile    int
		sliceCount uint32
		memoryMB   uint64
		multiproc  uint32
		encoders   uint32
		jpegs      uint32
		ofas       uint32
	}{
		{nvml.GPU_INSTANCE_PROFILE_1_SLICE, 1, 23552, 18, 0, 0, 0},
		{nvml.GPU_INSTANCE_PROFILE_2_SLICE, 2, 46080, 36, 1, 1, 1},
		{nvml.GPU_INSTANCE_PROFILE_3_SLICE, 3, 92160, 54, 2, 2, 2},
		{nvml.GPU_INSTANCE_PROFILE_4_SLICE, 4, 92160, 72, 2, 2, 2},
		{nvml.GPU_INSTANCE_PROFILE_7_SLICE, 7, 184320, 126, 4, 4, 4},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("profile_%d_slice", tc.sliceCount), func(t *testing.T) {
			profileInfo, ret := device.GetGpuInstanceProfileInfo(tc.profile)
			require.Equal(t, nvml.SUCCESS, ret)
			require.Equal(t, uint32(tc.profile), profileInfo.Id)
			require.Equal(t, tc.sliceCount, profileInfo.SliceCount)
			require.Equal(t, tc.memoryMB, profileInfo.MemorySizeMB)
			require.Equal(t, tc.multiproc, profileInfo.MultiprocessorCount)
			require.Equal(t, tc.encoders, profileInfo.EncoderCount)
			require.Equal(t, tc.jpegs, profileInfo.JpegCount)
			require.Equal(t, tc.ofas, profileInfo.OfaCount)
			require.Equal(t, uint32(1), profileInfo.IsP2pSupported) // B200 supports P2P
		})
	}
}

func TestB200AdvancedFeatures(t *testing.T) {
	device := NewDevice(0)

	// Test that B200 has enhanced encoder/decoder capabilities compared to H100/H200
	profileInfo, ret := device.GetGpuInstanceProfileInfo(nvml.GPU_INSTANCE_PROFILE_7_SLICE)
	require.Equal(t, nvml.SUCCESS, ret)

	// B200 should have more advanced multimedia engines
	require.Equal(t, uint32(4), profileInfo.EncoderCount) // More encoders than H100/H200
	require.Equal(t, uint32(4), profileInfo.JpegCount)    // JPEG engines
	require.Equal(t, uint32(4), profileInfo.OfaCount)     // OFA engines

	// Test GPU instance creation with advanced profile
	gpuInstance, ret := device.CreateGpuInstance(&profileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, gpuInstance)

	// Test compute instance with 7-slice profile
	ciProfileInfo, ret := gpuInstance.GetComputeInstanceProfileInfo(nvml.COMPUTE_INSTANCE_PROFILE_7_SLICE, nvml.COMPUTE_INSTANCE_ENGINE_PROFILE_SHARED)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(126), ciProfileInfo.MultiprocessorCount) // High multiprocessor count for B200
}

func TestB200MIGInstanceManagement(t *testing.T) {
	device := NewDevice(0)

	// Test creating and destroying instances
	profileInfo, ret := device.GetGpuInstanceProfileInfo(nvml.GPU_INSTANCE_PROFILE_2_SLICE)
	require.Equal(t, nvml.SUCCESS, ret)

	// Create GPU instance
	gi, ret := device.CreateGpuInstance(&profileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, gi)

	// Create compute instance
	ciProfileInfo, ret := gi.GetComputeInstanceProfileInfo(nvml.COMPUTE_INSTANCE_PROFILE_2_SLICE, nvml.COMPUTE_INSTANCE_ENGINE_PROFILE_SHARED)
	require.Equal(t, nvml.SUCCESS, ret)

	ci, ret := gi.CreateComputeInstance(&ciProfileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, ci)

	// Verify compute instance info
	ciInfo, ret := ci.GetInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(nvml.COMPUTE_INSTANCE_PROFILE_2_SLICE), ciInfo.ProfileId)

	// Test destruction
	ret = ci.Destroy()
	require.Equal(t, nvml.SUCCESS, ret)

	ret = gi.Destroy()
	require.Equal(t, nvml.SUCCESS, ret)
}
