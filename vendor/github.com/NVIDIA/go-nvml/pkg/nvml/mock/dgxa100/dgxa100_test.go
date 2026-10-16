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

package dgxa100

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
	"github.com/NVIDIA/go-nvml/pkg/nvml/mock/gpus"
)

// TestServerCreation verifies server creation and basic properties
func TestServerCreation(t *testing.T) {
	server := New()
	require.NotNil(t, server)

	// Test device count
	count, ret := server.DeviceGetCount()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 8, count)

	// Test system information
	driver, ret := server.SystemGetDriverVersion()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, "550.54.15", driver)

	nvmlVer, ret := server.SystemGetNVMLVersion()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, "12.550.54.15", nvmlVer)

	cudaVer, ret := server.SystemGetCudaDriverVersion()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 12040, cudaVer)
}

// TestDeviceHandling verifies device access and indexing
func TestDeviceHandling(t *testing.T) {
	server := New()

	// Test valid device indices
	for i := 0; i < 8; i++ {
		device, ret := server.DeviceGetHandleByIndex(i)
		require.Equal(t, nvml.SUCCESS, ret)
		require.NotNil(t, device)

		// Test device index
		index, ret := device.GetIndex()
		require.Equal(t, nvml.SUCCESS, ret)
		require.Equal(t, i, index)

		// Test minor number
		minor, ret := device.GetMinorNumber()
		require.Equal(t, nvml.SUCCESS, ret)
		require.Equal(t, i, minor)
	}

	// Test invalid device indices
	_, ret := server.DeviceGetHandleByIndex(-1)
	require.Equal(t, nvml.ERROR_INVALID_ARGUMENT, ret)

	_, ret = server.DeviceGetHandleByIndex(8)
	require.Equal(t, nvml.ERROR_INVALID_ARGUMENT, ret)
}

// TestDeviceProperties verifies all device properties
func TestDeviceProperties(t *testing.T) {
	server := New()
	device, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, device)

	// Test device name
	name, ret := device.GetName()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, "Mock NVIDIA A100-SXM4-40GB", name)

	// Test architecture
	arch, ret := device.GetArchitecture()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, nvml.DeviceArchitecture(nvml.DEVICE_ARCH_AMPERE), arch)

	// Test brand
	brand, ret := device.GetBrand()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, nvml.BRAND_NVIDIA, brand)

	// Test CUDA compute capability
	major, minor, ret := device.GetCudaComputeCapability()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 8, major)
	require.Equal(t, 0, minor)

	// Test memory info (40GB)
	memory, ret := device.GetMemoryInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint64(42949672960), memory.Total)

	// Test PCI device ID
	pciInfo, ret := device.GetPciInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(0x20B010DE), pciInfo.PciDeviceId)

	// Test UUID is set
	uuid, ret := device.GetUUID()
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotEmpty(t, uuid)
	require.Contains(t, uuid, "GPU-")
}

// TestDeviceAccessByUUID verifies UUID-based device access
func TestDeviceAccessByUUID(t *testing.T) {
	server := New()

	// Get device by index and its UUID
	originalDevice, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)

	uuid, ret := originalDevice.GetUUID()
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotEmpty(t, uuid)

	// Get device by UUID
	deviceByUUID, ret := server.DeviceGetHandleByUUID(uuid)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, originalDevice, deviceByUUID)

	// Test invalid UUID
	_, ret = server.DeviceGetHandleByUUID("invalid-uuid")
	require.Equal(t, nvml.ERROR_INVALID_ARGUMENT, ret)
}

// TestDeviceAccessByPciBusId verifies PCI bus ID-based device access
func TestDeviceAccessByPciBusId(t *testing.T) {
	server := New()

	// Test each device's PCI bus ID
	for i := 0; i < 8; i++ {
		originalDevice, ret := server.DeviceGetHandleByIndex(i)
		require.Equal(t, nvml.SUCCESS, ret)

		expectedPciBusID := fmt.Sprintf("0000:%02x:00.0", i)

		// Get device by PCI bus ID
		deviceByPci, ret := server.DeviceGetHandleByPciBusId(expectedPciBusID)
		require.Equal(t, nvml.SUCCESS, ret)
		require.Equal(t, originalDevice, deviceByPci)
	}

	// Test invalid PCI bus ID
	_, ret := server.DeviceGetHandleByPciBusId("invalid-pci-id")
	require.Equal(t, nvml.ERROR_INVALID_ARGUMENT, ret)
}

// TestMIGModeOperations verifies MIG mode handling
func TestMIGModeOperations(t *testing.T) {
	server := New()
	device, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)

	// Initially MIG should be disabled
	current, pending, ret := device.GetMigMode()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 0, current)
	require.Equal(t, 0, pending)

	// Enable MIG mode
	currentRet, pendingRet := device.SetMigMode(1)
	require.Equal(t, nvml.SUCCESS, currentRet)
	require.Equal(t, nvml.SUCCESS, pendingRet)

	// Verify MIG is enabled
	current, pending, ret = device.GetMigMode()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 1, current)
	require.Equal(t, 1, pending)

	// Disable MIG mode
	currentRet, pendingRet = device.SetMigMode(0)
	require.Equal(t, nvml.SUCCESS, currentRet)
	require.Equal(t, nvml.SUCCESS, pendingRet)

	// Verify MIG is disabled
	current, pending, ret = device.GetMigMode()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 0, current)
	require.Equal(t, 0, pending)
}

// TestMIGProfilesExist verifies MIG profile configuration exists
func TestMIGProfilesExist(t *testing.T) {
	// Test that MIGProfiles variable is accessible
	require.NotNil(t, MIGProfiles)
	require.NotNil(t, MIGProfiles.GpuInstanceProfiles)
	require.NotNil(t, MIGProfiles.ComputeInstanceProfiles)

	// Test that MIGPlacements variable is accessible
	require.NotNil(t, MIGPlacements)
	require.NotNil(t, MIGPlacements.GpuInstancePossiblePlacements)
	require.NotNil(t, MIGPlacements.ComputeInstancePossiblePlacements)

	// Test expected profile types exist
	expectedProfiles := []int{
		nvml.GPU_INSTANCE_PROFILE_1_SLICE,
		nvml.GPU_INSTANCE_PROFILE_1_SLICE_REV1,
		nvml.GPU_INSTANCE_PROFILE_1_SLICE_REV2,
		nvml.GPU_INSTANCE_PROFILE_2_SLICE,
		nvml.GPU_INSTANCE_PROFILE_3_SLICE,
		nvml.GPU_INSTANCE_PROFILE_4_SLICE,
		nvml.GPU_INSTANCE_PROFILE_7_SLICE,
	}

	for _, profileId := range expectedProfiles {
		profile, exists := MIGProfiles.GpuInstanceProfiles[profileId]
		require.True(t, exists, "Profile %d should exist", profileId)
		require.Equal(t, uint32(profileId), profile.Id)
		require.Greater(t, profile.MemorySizeMB, uint64(0))
	}
}

// TestGpuInstanceProfileInfo verifies GPU instance profile access
func TestGpuInstanceProfileInfo(t *testing.T) {
	server := New()
	device, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)

	// Test valid profile access
	profileInfo, ret := device.GetGpuInstanceProfileInfo(nvml.GPU_INSTANCE_PROFILE_1_SLICE)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(nvml.GPU_INSTANCE_PROFILE_1_SLICE), profileInfo.Id)
	require.Equal(t, uint32(1), profileInfo.SliceCount)
	require.Equal(t, uint64(4864), profileInfo.MemorySizeMB) // 1g.5gb

	// Test 7-slice profile
	profileInfo7, ret := device.GetGpuInstanceProfileInfo(nvml.GPU_INSTANCE_PROFILE_7_SLICE)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(nvml.GPU_INSTANCE_PROFILE_7_SLICE), profileInfo7.Id)
	require.Equal(t, uint32(7), profileInfo7.SliceCount)
	require.Equal(t, uint64(40192), profileInfo7.MemorySizeMB) // 7g.40gb

	// Test invalid profile
	_, ret = device.GetGpuInstanceProfileInfo(-1)
	require.Equal(t, nvml.ERROR_INVALID_ARGUMENT, ret)

	// Test unsupported profile (use a valid range but unsupported profile)
	_, ret = device.GetGpuInstanceProfileInfo(5) // Valid range but not in MIGProfiles
	require.Equal(t, nvml.ERROR_NOT_SUPPORTED, ret)
}

// TestGpuInstancePlacements verifies GPU instance placement access
func TestGpuInstancePlacements(t *testing.T) {
	server := New()
	device, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)

	// Test 1-slice placements
	profileInfo, ret := device.GetGpuInstanceProfileInfo(nvml.GPU_INSTANCE_PROFILE_1_SLICE)
	require.Equal(t, nvml.SUCCESS, ret)

	placements, ret := device.GetGpuInstancePossiblePlacements(&profileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Len(t, placements, 7) // Should have 7 possible placements for 1-slice

	// Test 7-slice placements
	profileInfo7, ret := device.GetGpuInstanceProfileInfo(nvml.GPU_INSTANCE_PROFILE_7_SLICE)
	require.Equal(t, nvml.SUCCESS, ret)

	placements7, ret := device.GetGpuInstancePossiblePlacements(&profileInfo7)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Len(t, placements7, 1) // Should have 1 placement for 7-slice (full GPU)
	require.Equal(t, uint32(0), placements7[0].Start)
	require.Equal(t, uint32(8), placements7[0].Size)
}

// TestGpuInstanceLifecycle verifies complete GPU instance lifecycle
func TestGpuInstanceLifecycle(t *testing.T) {
	server := New()
	device, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)

	// Get 1-slice profile
	profileInfo, ret := device.GetGpuInstanceProfileInfo(nvml.GPU_INSTANCE_PROFILE_1_SLICE)
	require.Equal(t, nvml.SUCCESS, ret)

	// Create GPU instance
	gi, ret := device.CreateGpuInstance(&profileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, gi)

	// Test GPU instance info
	giInfo, ret := gi.GetInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, device, giInfo.Device)
	require.Equal(t, profileInfo.Id, giInfo.ProfileId)
	require.Equal(t, uint32(0), giInfo.Id) // First instance should have ID 0

	// Get GPU instances for this profile
	instances, ret := device.GetGpuInstances(&profileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Len(t, instances, 1)
	require.Equal(t, gi, instances[0])

	// Destroy GPU instance
	ret = gi.Destroy()
	require.Equal(t, nvml.SUCCESS, ret)

	// Verify instance is removed
	instances, ret = device.GetGpuInstances(&profileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Len(t, instances, 0)
}

// TestGpuInstanceWithPlacement verifies GPU instance creation with placement
func TestGpuInstanceWithPlacement(t *testing.T) {
	server := New()
	device, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)

	// Get profile and placement
	profileInfo, ret := device.GetGpuInstanceProfileInfo(nvml.GPU_INSTANCE_PROFILE_1_SLICE)
	require.Equal(t, nvml.SUCCESS, ret)

	placements, ret := device.GetGpuInstancePossiblePlacements(&profileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotEmpty(t, placements)

	// Create GPU instance with specific placement
	gi, ret := device.CreateGpuInstanceWithPlacement(&profileInfo, &placements[0])
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, gi)

	// Verify placement in instance info
	giInfo, ret := gi.GetInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, placements[0], giInfo.Placement)

	// Clean up
	ret = gi.Destroy()
	require.Equal(t, nvml.SUCCESS, ret)
}

// TestComputeInstanceLifecycle verifies complete compute instance lifecycle
func TestComputeInstanceLifecycle(t *testing.T) {
	server := New()
	device, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)

	// Create GPU instance first
	giProfileInfo, ret := device.GetGpuInstanceProfileInfo(nvml.GPU_INSTANCE_PROFILE_1_SLICE)
	require.Equal(t, nvml.SUCCESS, ret)

	gi, ret := device.CreateGpuInstance(&giProfileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, gi)

	// Get compute instance profile
	ciProfileInfo, ret := gi.GetComputeInstanceProfileInfo(
		nvml.COMPUTE_INSTANCE_PROFILE_1_SLICE,
		nvml.COMPUTE_INSTANCE_ENGINE_PROFILE_SHARED,
	)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(nvml.COMPUTE_INSTANCE_PROFILE_1_SLICE), ciProfileInfo.Id)

	// Test invalid engine profile
	_, ret = gi.GetComputeInstanceProfileInfo(
		nvml.COMPUTE_INSTANCE_PROFILE_1_SLICE,
		999, // Invalid engine profile
	)
	require.Equal(t, nvml.ERROR_NOT_SUPPORTED, ret)

	// Get compute instance placements
	_, ret = gi.GetComputeInstancePossiblePlacements(&ciProfileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	// Note: Original implementation has empty placements (TODO comment)

	// Create compute instance
	ci, ret := gi.CreateComputeInstance(&ciProfileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.NotNil(t, ci)

	// Test compute instance info
	ciInfo, ret := ci.GetInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, device, ciInfo.Device)
	require.Equal(t, gi, ciInfo.GpuInstance)
	require.Equal(t, ciProfileInfo.Id, ciInfo.ProfileId)
	require.Equal(t, uint32(0), ciInfo.Id) // First instance should have ID 0

	// Get compute instances for this profile
	instances, ret := gi.GetComputeInstances(&ciProfileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Len(t, instances, 1)
	require.Equal(t, ci, instances[0])

	// Destroy compute instance
	ret = ci.Destroy()
	require.Equal(t, nvml.SUCCESS, ret)

	// Verify compute instance is removed
	instances, ret = gi.GetComputeInstances(&ciProfileInfo)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Len(t, instances, 0)

	// Destroy GPU instance
	ret = gi.Destroy()
	require.Equal(t, nvml.SUCCESS, ret)
}

// TestInitShutdownLifecycle verifies init/shutdown behavior
func TestInitShutdownLifecycle(t *testing.T) {
	server := New()

	// Test init
	ret := server.Init()
	require.Equal(t, nvml.SUCCESS, ret)

	// Test lookup symbol
	err := server.LookupSymbol("nvmlInit")
	require.NoError(t, err)

	// Test extensions
	ext := server.Extensions()
	require.NotNil(t, ext)
	require.Equal(t, server, ext)

	// Test shutdown
	ret = server.Shutdown()
	require.Equal(t, nvml.SUCCESS, ret)
}

// TestMultipleDevices verifies all devices are unique and correctly indexed
func TestMultipleDevices(t *testing.T) {
	server := New()

	devices := make([]nvml.Device, 8)
	uuids := make(map[string]bool)

	// Get all devices and verify uniqueness
	for i := 0; i < 8; i++ {
		device, ret := server.DeviceGetHandleByIndex(i)
		require.Equal(t, nvml.SUCCESS, ret)
		require.NotNil(t, device)

		devices[i] = device

		// Verify UUID is unique
		uuid, ret := device.GetUUID()
		require.Equal(t, nvml.SUCCESS, ret)
		require.NotEmpty(t, uuid)
		require.False(t, uuids[uuid], "UUID %s should be unique", uuid)
		uuids[uuid] = true

		// Verify device properties are consistent
		index, ret := device.GetIndex()
		require.Equal(t, nvml.SUCCESS, ret)
		require.Equal(t, i, index)

		minor, ret := device.GetMinorNumber()
		require.Equal(t, nvml.SUCCESS, ret)
		require.Equal(t, i, minor)

		name, ret := device.GetName()
		require.Equal(t, nvml.SUCCESS, ret)
		require.Equal(t, "Mock NVIDIA A100-SXM4-40GB", name)
	}

	// Verify all devices are distinct objects
	for i := 0; i < 8; i++ {
		for j := i + 1; j < 8; j++ {
			require.NotEqual(t, devices[i], devices[j], "Devices %d and %d should be different objects", i, j)
		}
	}
}

// TestA100SpecificCharacteristics tests A100-specific values
func TestA100SpecificCharacteristics(t *testing.T) {
	server := New()
	device, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)

	// Test A100 doesn't support P2P in MIG (IsP2pSupported should be 0)
	profileInfo, ret := device.GetGpuInstanceProfileInfo(nvml.GPU_INSTANCE_PROFILE_1_SLICE)
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(0), profileInfo.IsP2pSupported)

	// Test A100 memory values are correct
	profile1 := MIGProfiles.GpuInstanceProfiles[nvml.GPU_INSTANCE_PROFILE_1_SLICE]
	require.Equal(t, uint64(4864), profile1.MemorySizeMB) // 1g.5gb

	profile7 := MIGProfiles.GpuInstanceProfiles[nvml.GPU_INSTANCE_PROFILE_7_SLICE]
	require.Equal(t, uint64(40192), profile7.MemorySizeMB) // 7g.40gb

	// Test A100 architecture
	arch, ret := device.GetArchitecture()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, nvml.DeviceArchitecture(nvml.DEVICE_ARCH_AMPERE), arch)

	// Test A100 CUDA compute capability
	major, minor, ret := device.GetCudaComputeCapability()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 8, major) // Ampere
	require.Equal(t, 0, minor)

	// Test A100 PCI device ID
	pciInfo, ret := device.GetPciInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(0x20B010DE), pciInfo.PciDeviceId) // A100-SXM4-40GB
}

// TestHeterogeneousGPUs verifies creation of servers with mixed GPU types
func TestHeterogeneousGPUs(t *testing.T) {
	// Create server with mix of A100 40GB and 80GB GPUs
	server := NewWithGPUs(
		gpus.A100_SXM4_40GB,
		gpus.A100_SXM4_80GB,
		gpus.A100_SXM4_40GB,
		gpus.A100_PCIE_80GB,
	)
	require.NotNil(t, server)

	// Verify device count
	count, ret := server.DeviceGetCount()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, 4, count)

	// Verify first device is 40GB SXM4
	dev0, ret := server.DeviceGetHandleByIndex(0)
	require.Equal(t, nvml.SUCCESS, ret)
	name0, ret := dev0.GetName()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, "Mock NVIDIA A100-SXM4-40GB", name0)
	mem0, ret := dev0.GetMemoryInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint64(40*1024*1024*1024), mem0.Total)

	// Verify second device is 80GB SXM4
	dev1, ret := server.DeviceGetHandleByIndex(1)
	require.Equal(t, nvml.SUCCESS, ret)
	name1, ret := dev1.GetName()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, "NVIDIA A100-SXM4-80GB", name1)
	mem1, ret := dev1.GetMemoryInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint64(80*1024*1024*1024), mem1.Total)

	// Verify third device is 40GB SXM
	dev2, ret := server.DeviceGetHandleByIndex(2)
	require.Equal(t, nvml.SUCCESS, ret)
	name2, ret := dev2.GetName()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, "Mock NVIDIA A100-SXM4-40GB", name2)

	// Verify fourth device is 80GB PCIe
	dev3, ret := server.DeviceGetHandleByIndex(3)
	require.Equal(t, nvml.SUCCESS, ret)
	name3, ret := dev3.GetName()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, "NVIDIA A100-PCIE-80GB", name3)
	pci3, ret := dev3.GetPciInfo()
	require.Equal(t, nvml.SUCCESS, ret)
	require.Equal(t, uint32(0x20B510DE), pci3.PciDeviceId) // A100-PCIE-80GB
}
