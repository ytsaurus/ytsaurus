package flow

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	pprofprofile "github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/nop"
	"go.ytsaurus.tech/yt/go/guid"
)

func TestMemoryProfileFromPprofAttributesCompleteMarkers(t *testing.T) {
	first := uint32(0x123456)
	second := uint32(0xabcdef)
	profile := &pprofprofile.Profile{
		SampleType: []*pprofprofile.ValueType{{Type: "inuse_space", Unit: "bytes"}},
		Sample: []*pprofprofile.Sample{
			memoryProfileSample(first, 100),
			memoryProfileSample(first, 50),
			memoryProfileSample(second, 200),
			{Value: []int64{400}},
		},
	}

	got, err := memoryProfileFromPprof(profile)
	require.NoError(t, err)
	require.EqualValues(t, 750, got.totalInUse)
	require.Equal(t, map[uint32]int64{first: 150, second: 200}, got.inUseBySlot)
}

func TestMemoryProfileRequiresInUseBytes(t *testing.T) {
	_, err := memoryProfileFromPprof(&pprofprofile.Profile{
		SampleType: []*pprofprofile.ValueType{{Type: "alloc_space", Unit: "bytes"}},
	})
	require.ErrorContains(t, err, "inuse_space/bytes")
}

func TestDistributeMemorySeparatesAttributedAndSharedRSS(t *testing.T) {
	first := guid.FromHalves(1, 2)
	second := guid.FromHalves(3, 4)
	profile := memoryProfile{
		inUseBySlot: map[uint32]int64{1: 100, 2: 300},
		totalInUse:  800,
	}

	got := distributeMemory(800, profile, []memoryJobShare{
		{id: first, slot: 1, hasSlot: true},
		{id: second, slot: 2, hasSlot: true},
	})

	require.Equal(t, map[guid.GUID]int64{first: 300, second: 500}, got)
}

func TestDistributeMemorySharesRSSWithoutHeapWeights(t *testing.T) {
	first := guid.FromHalves(1, 2)
	second := guid.FromHalves(3, 4)

	got := distributeMemory(101, memoryProfile{}, []memoryJobShare{{id: first}, {id: second}})

	require.EqualValues(t, 101, got[first]+got[second])
	require.InDelta(t, got[first], got[second], 1)
}

func TestGoPreservesMemoryMarker(t *testing.T) {
	for digit := uint32(0); digit < 16; digit++ {
		slot := digit * 0x111111
		t.Run(fmt.Sprintf("%06x", slot), func(t *testing.T) {
			ctx := context.WithValue(context.Background(), memorySlotContextKey{}, slot)
			result := make(chan struct {
				slot uint32
				ok   bool
			}, 1)

			Go(ctx, func(context.Context) {
				stack := make([]uintptr, 64)
				count := runtime.Callers(0, stack)
				got, ok := memorySlotFromStack(stack[:count])
				result <- struct {
					slot uint32
					ok   bool
				}{slot: got, ok: ok}
			})

			got := <-result
			require.True(t, got.ok)
			require.Equal(t, slot, got.slot)
		})
	}
}

func TestRuntimeHeapProfileAttributesTaggedAllocation(t *testing.T) {
	const (
		slot = uint32(0x654321)
		size = 64 << 20
	)
	var retained []byte
	withMemorySlot(slot, func() {
		retained = make([]byte, size)
		for index := 0; index < len(retained); index += os.Getpagesize() {
			retained[index] = 1
		}
	})
	runtime.GC()
	runtime.GC()

	profile, err := (runtimeMemoryProbe{}).ReadHeapProfile()
	require.NoError(t, err)
	require.Greater(t, profile.inUseBySlot[slot], int64(size/2))
	runtime.KeepAlive(retained)
}

func TestMemoryTrackerReportsRSSWindowPeak(t *testing.T) {
	cache := newJobCache()
	first := guid.FromHalves(1, 2)
	second := guid.FromHalves(3, 4)
	cache.Put(cachedTestJob(t, first))
	cache.Put(cachedTestJob(t, second))
	probe := &fakeMemoryProbe{rss: 100, gcCycles: 1}
	tracker := newTestMemoryTracker(cache, probe)
	tracker.WithJob(context.Background(), first, func(context.Context) {})
	tracker.WithJob(context.Background(), second, func(context.Context) {})

	tracker.mu.Lock()
	firstSlot := tracker.trackedJobs[first].slot
	secondSlot := tracker.trackedJobs[second].slot
	tracker.mu.Unlock()
	probe.profile = memoryProfile{
		inUseBySlot: map[uint32]int64{firstSlot: 1, secondSlot: 1},
		totalInUse:  2,
	}

	state := memoryTrackerState{}
	start := time.Unix(100, 0)
	tracker.poll(&state, start)
	probe.rss = 300
	tracker.poll(&state, start.Add(500*time.Millisecond))
	probe.rss = 200
	tracker.poll(&state, start.Add(time.Second))

	require.EqualValues(t, 150, tracker.Usage(first))
	require.EqualValues(t, 150, tracker.Usage(second))
}

func TestMemoryTrackerForcesGCOnlyAfterMaterialGrowth(t *testing.T) {
	cache := newJobCache()
	jobID := guid.FromHalves(1, 2)
	cache.Put(cachedTestJob(t, jobID))
	probe := &fakeMemoryProbe{rss: 100 << 20, gcCycles: 1, profile: memoryProfile{}}
	tracker := newTestMemoryTracker(cache, probe)

	state := memoryTrackerState{}
	start := time.Unix(100, 0)
	tracker.poll(&state, start)
	require.Equal(t, 1, probe.profileReads)

	probe.rss = 150 << 20
	tracker.poll(&state, start.Add(11*time.Second))
	require.Zero(t, probe.forcedGCs)

	probe.rss = 170 << 20
	tracker.poll(&state, start.Add(12*time.Second))
	require.Equal(t, 1, probe.forcedGCs)
	require.Equal(t, 2, probe.profileReads)
}

func TestMemoryTrackerPrunesJobsAndDeduplicatesRSSFailures(t *testing.T) {
	cache := newJobCache()
	jobID := guid.FromHalves(1, 2)
	cache.Put(cachedTestJob(t, jobID))
	probe := &fakeMemoryProbe{rssErr: fmt.Errorf("RSS is unavailable")}
	logger := &warningRecorder{Structured: (&nop.Logger{}).Structured()}
	tracker := newMemoryTracker(cache, logger, probe, defaultMemoryTrackerConfig)
	tracker.WithJob(context.Background(), jobID, func(context.Context) {})
	cache.Delete(jobID)

	state := memoryTrackerState{}
	start := time.Unix(100, 0)
	tracker.poll(&state, start)
	tracker.poll(&state, start.Add(time.Second))

	require.Len(t, logger.warnings, 1)
	tracker.mu.Lock()
	_, tracked := tracker.trackedJobs[jobID]
	tracker.mu.Unlock()
	require.False(t, tracked)

	probe.rssErr = nil
	tracker.poll(&state, start.Add(1250*time.Millisecond))
	probe.rssErr = fmt.Errorf("RSS is unavailable again")
	tracker.poll(&state, start.Add(1500*time.Millisecond))
	require.Len(t, logger.warnings, 2)
}

func memoryProfileSample(slot uint32, inUse int64) *pprofprofile.Sample {
	locations := make([]*pprofprofile.Location, 0, 6)
	for level := 0; level < 6; level++ {
		digit := (slot >> (4 * level)) & 0xf
		locations = append(locations, &pprofprofile.Location{
			Line: []pprofprofile.Line{{Function: &pprofprofile.Function{
				Name: fmt.Sprintf("go.ytsaurus.tech/yt/go/flow.memoryMarker%x%x", level, digit),
			}}},
		})
	}
	return &pprofprofile.Sample{Value: []int64{inUse}, Location: locations}
}

type fakeMemoryProbe struct {
	rss          int64
	rssErr       error
	gcCycles     uint64
	profile      memoryProfile
	profileReads int
	forcedGCs    int
}

func (p *fakeMemoryProbe) ReadRSS() (int64, error) {
	return p.rss, p.rssErr
}

func (p *fakeMemoryProbe) GCCycles() (uint64, error) {
	return p.gcCycles, nil
}

func (p *fakeMemoryProbe) ReadHeapProfile() (memoryProfile, error) {
	p.profileReads++
	return p.profile, nil
}

func (p *fakeMemoryProbe) ForceGC() {
	p.forcedGCs++
	p.gcCycles++
}

func newTestMemoryTracker(cache *jobCache, probe memoryProbe) *memoryTracker {
	return newMemoryTracker(cache, (&nop.Logger{}).Structured(), probe, defaultMemoryTrackerConfig)
}

type warningRecorder struct {
	log.Structured
	warnings []string
}

func (l *warningRecorder) Warn(message string, _ ...log.Field) {
	l.warnings = append(l.warnings, message)
}
