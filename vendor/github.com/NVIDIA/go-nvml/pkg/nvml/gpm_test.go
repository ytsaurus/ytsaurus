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

func TestGpmMetricsGet(t *testing.T) {
	overrideMetrics := [210]GpmMetric{
		{
			Value: 99,
		},
	}
	defer setNvmlGpmMetricsGetStubForTest(func(metricsGet *nvmlGpmMetricsGetType) Return {
		metricsGet.Metrics = overrideMetrics
		return SUCCESS
	})()

	metrics := GpmMetricsGetType{
		Sample1: nvmlGpmSample{},
		Sample2: nvmlGpmSample{},
	}
	ret := GpmMetricsGet(&metrics)

	require.Equal(t, SUCCESS, ret)
	require.EqualValues(t, GPM_METRICS_GET_VERSION, metrics.Version)

	require.EqualValues(t, overrideMetrics, metrics.Metrics)
}

func TestGpmMetricsGetV(t *testing.T) {
	overrideMetrics := [210]GpmMetric{
		{
			Value: 99,
		},
	}
	defer setNvmlGpmMetricsGetStubForTest(func(metricsGet *nvmlGpmMetricsGetType) Return {
		metricsGet.Metrics = overrideMetrics
		return SUCCESS
	})()

	metrics := GpmMetricsGetType{
		Sample1: nvmlGpmSample{},
		Sample2: nvmlGpmSample{},
	}

	ret := GpmMetricsGetV(&metrics).V1()

	require.Equal(t, SUCCESS, ret)
	require.EqualValues(t, GPM_METRICS_GET_VERSION, metrics.Version)

	require.EqualValues(t, overrideMetrics, metrics.Metrics)
}

func setNvmlGpmMetricsGetStubForTest(mock func(metricsGet *nvmlGpmMetricsGetType) Return) func() {
	original := nvmlGpmMetricsGetStub

	nvmlGpmMetricsGetStub = mock
	return func() {
		nvmlGpmMetricsGetStub = original
	}
}
