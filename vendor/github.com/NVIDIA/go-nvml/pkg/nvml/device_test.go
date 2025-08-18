/**
# Copyright 2024 NVIDIA CORPORATION
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
**/

package nvml

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetTopologyCommonAncestor(t *testing.T) {
	type wrappedDevice struct {
		Device
	}

	type wrappedWrappedDevice struct {
		wrappedDevice
	}

	testCases := []struct {
		description string
		device      Device
	}{
		{
			description: "nvmlDevice",
			device:      nvmlDevice{},
		},
		{
			description: "pointer to nvmlDevice",
			device:      &nvmlDevice{},
		},
		{
			description: "wrapped device",
			device: wrappedDevice{
				Device: nvmlDevice{},
			},
		},
		{
			description: "pointer to wrapped device",
			device: &wrappedDevice{
				Device: nvmlDevice{},
			},
		},
		{
			description: "nested wrapped device",
			device: wrappedWrappedDevice{
				wrappedDevice: wrappedDevice{
					Device: nvmlDevice{},
				},
			},
		},
		{
			description: "non-device fields included",
			device: struct {
				name string
				Name string
				Device
			}{
				Device: wrappedDevice{
					Device: nvmlDevice{},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			defer setNvmlDeviceGetTopologyCommonAncestorStubForTest(SUCCESS)()

			_, ret := nvmlDevice{}.GetTopologyCommonAncestor(tc.device)
			require.Equal(t, SUCCESS, ret)
		})
	}
}

func setNvmlDeviceGetTopologyCommonAncestorStubForTest(ret Return) func() {
	original := nvmlDeviceGetTopologyCommonAncestorStub

	nvmlDeviceGetTopologyCommonAncestorStub = func(Device1, Device2 nvmlDevice, PathInfo *GpuTopologyLevel) Return {
		return ret
	}
	return func() {
		nvmlDeviceGetTopologyCommonAncestorStub = original
	}
}
