package flow

import (
	"bytes"
	"context"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	pprofprofile "github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/guid"
)

func TestCPUTimeByJob(t *testing.T) {
	first := guid.FromHalves(1, 2)
	second := guid.FromHalves(3, 4)
	profile := &pprofprofile.Profile{
		SampleType: []*pprofprofile.ValueType{
			{Type: "samples", Unit: "count"},
			{Type: "cpu", Unit: "nanoseconds"},
		},
		Sample: []*pprofprofile.Sample{
			{Value: []int64{1, 20}, Label: map[string][]string{cpuJobLabel: {first.String()}}},
			{Value: []int64{1, 30}, Label: map[string][]string{cpuJobLabel: {first.String()}}},
			{Value: []int64{1, 70}, Label: map[string][]string{cpuJobLabel: {second.String()}}},
			{Value: []int64{1, 90}},
			{Value: []int64{1, 100}, Label: map[string][]string{cpuJobLabel: {"malformed"}}},
		},
	}

	var encoded bytes.Buffer
	require.NoError(t, profile.Write(&encoded))

	parsed, err := pprofprofile.ParseData(encoded.Bytes())
	require.NoError(t, err)
	got, err := cpuTimeByJob(parsed)
	require.NoError(t, err)
	require.Equal(t, map[guid.GUID]int64{first: 50, second: 70}, got)
}

func TestCPUTimeByJobRequiresCPUSamples(t *testing.T) {
	_, err := cpuTimeByJob(&pprofprofile.Profile{
		SampleType: []*pprofprofile.ValueType{{Type: "samples", Unit: "count"}},
	})
	require.ErrorContains(t, err, "cpu/nanoseconds")
}

func TestRuntimeCPUProfileKeepsJobLabelOnChildGoroutine(t *testing.T) {
	jobID := guid.FromHalves(1, 2)
	var encoded bytes.Buffer
	require.NoError(t, pprof.StartCPUProfile(&encoded))
	t.Cleanup(pprof.StopCPUProfile)

	var accumulator uint64
	withJobCPU(context.Background(), jobID, func(context.Context) {
		done := make(chan struct{})
		go func() {
			defer close(done)
			deadline := time.Now().Add(250 * time.Millisecond)
			for time.Now().Before(deadline) {
				accumulator = accumulator*1664525 + 1013904223
			}
		}()
		<-done
	})
	runtime.KeepAlive(accumulator)
	pprof.StopCPUProfile()

	profile, err := pprofprofile.ParseData(encoded.Bytes())
	require.NoError(t, err)
	cpuByJob, err := cpuTimeByJob(profile)
	require.NoError(t, err)
	require.Positive(t, cpuByJob[jobID])
}
