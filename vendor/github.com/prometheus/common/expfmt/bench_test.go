// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expfmt

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"testing"

	"google.golang.org/protobuf/encoding/protodelim"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

var parser TextParser

// Benchmarks to show how much penalty text format parsing actually inflicts.
//
// Example results on Linux 3.13.0, Intel(R) Core(TM) i7-4700MQ CPU @ 2.40GHz, go1.4.
//
// BenchmarkParseText          1000           1188535 ns/op          205085 B/op       6135 allocs/op
// BenchmarkParseTextGzip      1000           1376567 ns/op          246224 B/op       6151 allocs/op
// BenchmarkParseProto        10000            172790 ns/op           52258 B/op       1160 allocs/op
// BenchmarkParseProtoGzip     5000            324021 ns/op           94931 B/op       1211 allocs/op
// BenchmarkParseProtoMap     10000            187946 ns/op           58714 B/op       1203 allocs/op
//
// CONCLUSION: The overhead for the map is negligible. Text format needs ~5x more allocations.
// Without compression, it needs ~7x longer, but with compression (the more relevant scenario),
// the difference becomes less relevant, only ~4x.
//
// The test data contains 248 samples.

// BenchmarkParseText benchmarks the parsing of a text-format scrape into metric
// family DTOs.
func BenchmarkParseText(b *testing.B) {
	b.StopTimer()
	data, err := os.ReadFile("testdata/text")
	require.NoError(b, err)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := parser.TextToMetricFamilies(bytes.NewReader(data))
		require.NoError(b, err)
	}
}

// BenchmarkParseTextGzip benchmarks the parsing of a gzipped text-format scrape
// into metric family DTOs.
func BenchmarkParseTextGzip(b *testing.B) {
	b.StopTimer()
	data, err := os.ReadFile("testdata/text.gz")
	require.NoError(b, err)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		in, err := gzip.NewReader(bytes.NewReader(data))
		require.NoError(b, err)
		_, err = parser.TextToMetricFamilies(in)
		require.NoError(b, err)
	}
}

// BenchmarkParseProto benchmarks the parsing of a protobuf-format scrape into
// metric family DTOs. Note that this does not build a map of metric families
// (as the text version does), because it is not required for Prometheus
// ingestion either. (However, it is required for the text-format parsing, as
// the metric family might be sprinkled all over the text, while the
// protobuf-format guarantees bundling at one place.)
func BenchmarkParseProto(b *testing.B) {
	b.StopTimer()
	data, err := os.ReadFile("testdata/protobuf")
	require.NoError(b, err)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		family := &dto.MetricFamily{}
		in := bufio.NewReader(bytes.NewReader(data))
		unmarshaler := protodelim.UnmarshalOptions{
			MaxSize: -1,
		}
		for {
			family.Reset()
			if err := unmarshaler.UnmarshalFrom(in, family); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkParseProtoGzip is like BenchmarkParseProto above, but parses gzipped
// protobuf format.
func BenchmarkParseProtoGzip(b *testing.B) {
	b.StopTimer()
	data, err := os.ReadFile("testdata/protobuf.gz")
	require.NoError(b, err)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		family := &dto.MetricFamily{}
		gz, err := gzip.NewReader(bytes.NewReader(data))
		require.NoError(b, err)
		in := bufio.NewReader(gz)
		unmarshaler := protodelim.UnmarshalOptions{
			MaxSize: -1,
		}
		for {
			family.Reset()
			if err := unmarshaler.UnmarshalFrom(in, family); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkParseProtoMap is like BenchmarkParseProto but DOES put the parsed
// metric family DTOs into a map. This is not happening during Prometheus
// ingestion. It is just here to measure the overhead of that map creation and
// separate it from the overhead of the text format parsing.
func BenchmarkParseProtoMap(b *testing.B) {
	b.StopTimer()
	data, err := os.ReadFile("testdata/protobuf")
	require.NoError(b, err)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		families := map[string]*dto.MetricFamily{}
		in := bufio.NewReader(bytes.NewReader(data))
		unmarshaler := protodelim.UnmarshalOptions{
			MaxSize: -1,
		}
		for {
			family := &dto.MetricFamily{}
			if err := unmarshaler.UnmarshalFrom(in, family); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				b.Fatal(err)
			}
			families[family.GetName()] = family
		}
	}
}
