package flow

import (
	"bytes"
	"context"
	"math"
	"os"
	"runtime"
	runtimemetrics "runtime/metrics"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pprofprofile "github.com/google/pprof/profile"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/guid"
)

const (
	memorySlotCount = 1 << 24

	memoryRSSSampleInterval = 250 * time.Millisecond
	memoryReportWindow      = time.Second
	memoryProfileMinPeriod  = time.Second
	memoryProfileMaxAge     = 10 * time.Second
	memoryProfileMinGrowth  = 64 << 20
)

type memorySlotContextKey struct{}

// Go starts fn in a new goroutine associated with the current Flow job.
// The caller must wait for fn to finish before its computation handler returns.
func Go(ctx context.Context, fn func(context.Context)) {
	go func() {
		pprof.SetGoroutineLabels(ctx)
		slot, ok := ctx.Value(memorySlotContextKey{}).(uint32)
		if !ok {
			fn(ctx)
			return
		}
		withMemorySlot(slot, func() {
			fn(ctx)
		})
	}()
}

func withMemorySlot(slot uint32, fn func()) {
	// The selected stack frames encode the slot for the heap profiler.
	memoryMarkers5[(slot>>20)&0xf](slot, fn)
}

type memoryProfile struct {
	inUseBySlot map[uint32]int64
	totalInUse  int64
}

type memoryProbe interface {
	ReadRSS() (int64, error)
	GCCycles() (uint64, error)
	ReadHeapProfile() (memoryProfile, error)
	ForceGC()
}

type runtimeMemoryProbe struct{}

func (runtimeMemoryProbe) ReadRSS() (int64, error) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, xerrors.Errorf("read /proc/self/statm: %w", err)
	}
	fields := bytes.Fields(data)
	if len(fields) < 2 {
		return 0, xerrors.Errorf("parse /proc/self/statm: got %d fields", len(fields))
	}
	pages, err := strconv.ParseInt(string(fields[1]), 10, 64)
	if err != nil {
		return 0, xerrors.Errorf("parse resident pages: %w", err)
	}
	pageSize := int64(os.Getpagesize())
	if pages < 0 || pages > math.MaxInt64/pageSize {
		return 0, xerrors.Errorf("resident page count %d is out of range", pages)
	}
	return pages * pageSize, nil
}

func (runtimeMemoryProbe) GCCycles() (uint64, error) {
	samples := []runtimemetrics.Sample{{Name: "/gc/cycles/total:gc-cycles"}}
	runtimemetrics.Read(samples)
	if samples[0].Value.Kind() != runtimemetrics.KindUint64 {
		return 0, xerrors.Errorf("read Go GC cycles: unexpected metric kind %v", samples[0].Value.Kind())
	}
	return samples[0].Value.Uint64(), nil
}

func (runtimeMemoryProbe) ReadHeapProfile() (memoryProfile, error) {
	profile := pprof.Lookup("heap")
	if profile == nil {
		return memoryProfile{}, xerrors.Errorf("flow: Go heap profile is unavailable")
	}
	var encoded bytes.Buffer
	if err := profile.WriteTo(&encoded, 0); err != nil {
		return memoryProfile{}, xerrors.Errorf("write Go heap profile: %w", err)
	}
	parsed, err := pprofprofile.ParseData(encoded.Bytes())
	if err != nil {
		return memoryProfile{}, xerrors.Errorf("parse Go heap profile: %w", err)
	}
	return memoryProfileFromPprof(parsed)
}

func (runtimeMemoryProbe) ForceGC() {
	runtime.GC()
}

func memoryProfileFromPprof(profile *pprofprofile.Profile) (memoryProfile, error) {
	inUseIndex := -1
	for index, sampleType := range profile.SampleType {
		if sampleType.Type == "inuse_space" && sampleType.Unit == "bytes" {
			inUseIndex = index
			break
		}
	}
	if inUseIndex == -1 {
		return memoryProfile{}, xerrors.Errorf("flow: heap profile has no inuse_space/bytes sample type")
	}

	result := memoryProfile{inUseBySlot: make(map[uint32]int64)}
	for _, sample := range profile.Sample {
		if inUseIndex >= len(sample.Value) || sample.Value[inUseIndex] <= 0 {
			continue
		}
		inUse := sample.Value[inUseIndex]
		result.totalInUse += inUse
		if slot, ok := memorySlotFromProfileSample(sample); ok {
			result.inUseBySlot[slot] += inUse
		}
	}
	return result, nil
}

func memorySlotFromProfileSample(sample *pprofprofile.Sample) (uint32, bool) {
	return memorySlotFromFunctionNames(func(yield func(string)) {
		for _, location := range sample.Location {
			for _, line := range location.Line {
				if line.Function != nil {
					yield(line.Function.Name)
				}
			}
		}
	})
}

func memorySlotFromStack(stack []uintptr) (uint32, bool) {
	return memorySlotFromFunctionNames(func(yield func(string)) {
		frames := runtime.CallersFrames(stack)
		for {
			frame, more := frames.Next()
			yield(frame.Function)
			if !more {
				return
			}
		}
	})
}

func memorySlotFromFunctionNames(visit func(func(string))) (uint32, bool) {
	digits := [6]int{-1, -1, -1, -1, -1, -1}
	visit(func(name string) {
		marker := strings.LastIndex(name, ".memoryMarker")
		if marker == -1 {
			return
		}
		suffix := name[marker+len(".memoryMarker"):]
		if len(suffix) != 2 || suffix[0] < '0' || suffix[0] > '5' {
			return
		}
		digit, ok := memoryHexDigit(suffix[1])
		if ok {
			digits[suffix[0]-'0'] = digit
		}
	})

	var slot uint32
	for level, digit := range digits {
		if digit < 0 {
			return 0, false
		}
		slot |= uint32(digit) << (4 * level)
	}
	return slot, true
}

func memoryHexDigit(value byte) (int, bool) {
	switch {
	case value >= '0' && value <= '9':
		return int(value - '0'), true
	case value >= 'a' && value <= 'f':
		return int(value-'a') + 10, true
	default:
		return 0, false
	}
}

type memoryTrackerConfig struct {
	rssSampleInterval time.Duration
	reportWindow      time.Duration
	profileMinPeriod  time.Duration
	profileMaxAge     time.Duration
	profileMinGrowth  int64
}

var defaultMemoryTrackerConfig = memoryTrackerConfig{
	rssSampleInterval: memoryRSSSampleInterval,
	reportWindow:      memoryReportWindow,
	profileMinPeriod:  memoryProfileMinPeriod,
	profileMaxAge:     memoryProfileMaxAge,
	profileMinGrowth:  memoryProfileMinGrowth,
}

type trackedMemoryJob struct {
	slot     uint32
	hasSlot  bool
	inFlight int
	usage    int64
}

type memoryTracker struct {
	jobs   *jobCache
	logger log.Structured
	probe  memoryProbe
	config memoryTrackerConfig

	mu             sync.Mutex
	trackedJobs    map[guid.GUID]*trackedMemoryJob
	nextSlot       uint32
	slotsExhausted bool
	cancel         context.CancelFunc
	done           chan struct{}
}

func newMemoryTracker(jobs *jobCache, logger log.Structured, probe memoryProbe, config memoryTrackerConfig) *memoryTracker {
	return &memoryTracker{
		jobs:        jobs,
		logger:      logger,
		probe:       probe,
		config:      config,
		trackedJobs: make(map[guid.GUID]*trackedMemoryJob),
	}
}

func (t *memoryTracker) Start() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.done != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.cancel = cancel
	t.done = done
	go t.run(ctx, done)
}

func (t *memoryTracker) Stop() {
	t.mu.Lock()
	cancel := t.cancel
	done := t.done
	t.mu.Unlock()
	if done == nil {
		return
	}
	cancel()
	<-done

	t.mu.Lock()
	if t.done == done {
		t.cancel = nil
		t.done = nil
	}
	t.mu.Unlock()
}

func (t *memoryTracker) WithJob(ctx context.Context, jobID guid.GUID, fn func(context.Context)) {
	slot, ok := t.beginJob(jobID)
	defer t.endJob(jobID)
	if !ok {
		fn(ctx)
		return
	}
	withMemorySlot(slot, func() {
		fn(context.WithValue(ctx, memorySlotContextKey{}, slot))
	})
}

func (t *memoryTracker) Usage(jobID guid.GUID) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if job := t.trackedJobs[jobID]; job != nil {
		return job.usage
	}
	return 0
}

func (t *memoryTracker) beginJob(jobID guid.GUID) (uint32, bool) {
	t.mu.Lock()
	job := t.ensureJobLocked(jobID)
	job.inFlight++
	slot := job.slot
	hasSlot := job.hasSlot
	reportExhaustion := !hasSlot && !t.slotsExhausted
	if reportExhaustion {
		t.slotsExhausted = true
	}
	t.mu.Unlock()

	if reportExhaustion {
		t.logger.Warn("flow: companion memory accounting exhausted its job slots")
	}
	return slot, hasSlot
}

func (t *memoryTracker) endJob(jobID guid.GUID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if job := t.trackedJobs[jobID]; job != nil {
		job.inFlight--
	}
}

func (t *memoryTracker) ensureJobLocked(jobID guid.GUID) *trackedMemoryJob {
	if job := t.trackedJobs[jobID]; job != nil {
		return job
	}
	job := &trackedMemoryJob{}
	// Old heap profiles can outlive an expired job, so slots are never reused.
	if t.nextSlot < memorySlotCount {
		job.slot = t.nextSlot
		job.hasSlot = true
		t.nextSlot++
	}
	t.trackedJobs[jobID] = job
	return job
}

type memoryTrackerState struct {
	windowStart        time.Time
	peakRSS            int64
	reportedRSSFailure bool

	profile          memoryProfile
	hasProfile       bool
	profiledGCCycles uint64
	lastProfileTime  time.Time
	lastProfileRSS   int64
}

func (t *memoryTracker) run(ctx context.Context, done chan<- struct{}) {
	defer close(done)

	state := memoryTrackerState{}
	t.poll(&state, time.Now())
	ticker := time.NewTicker(t.config.rssSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			t.poll(&state, now)
		}
	}
}

func (t *memoryTracker) poll(state *memoryTrackerState, now time.Time) {
	rss, err := t.probe.ReadRSS()
	if err != nil {
		if !state.reportedRSSFailure {
			t.logger.Warn("flow: cannot read companion RSS", log.Error(err))
			state.reportedRSSFailure = true
		}
	} else {
		state.reportedRSSFailure = false
	}
	if state.windowStart.IsZero() {
		state.windowStart = now
	}
	if err == nil {
		state.peakRSS = max(state.peakRSS, rss)

		gcCycles, gcErr := t.probe.GCCycles()
		if gcErr != nil {
			t.logger.Warn("flow: cannot read Go GC cycles", log.Error(gcErr))
		} else {
			naturalRefresh := !state.hasProfile ||
				(gcCycles != state.profiledGCCycles && now.Sub(state.lastProfileTime) >= t.config.profileMinPeriod)
			forcedRefresh := state.hasProfile && gcCycles == state.profiledGCCycles &&
				now.Sub(state.lastProfileTime) >= t.config.profileMaxAge &&
				materialMemoryGrowth(state.peakRSS, state.lastProfileRSS, t.config.profileMinGrowth) &&
				t.jobs.Len() != 0
			if forcedRefresh {
				t.probe.ForceGC()
				gcCycles, gcErr = t.probe.GCCycles()
				if gcErr != nil {
					t.logger.Warn("flow: cannot read Go GC cycles after forced collection", log.Error(gcErr))
				} else {
					naturalRefresh = true
				}
			}
			if naturalRefresh {
				profile, profileErr := t.probe.ReadHeapProfile()
				if profileErr != nil {
					t.logger.Warn("flow: cannot read companion heap profile", log.Error(profileErr))
				} else {
					state.profile = profile
					state.hasProfile = true
					state.profiledGCCycles = gcCycles
					state.lastProfileTime = now
					state.lastProfileRSS = rss
				}
			}
		}
	}

	if now.Sub(state.windowStart) >= t.config.reportWindow {
		t.assign(state.profile, state.peakRSS)
		state.windowStart = now
		state.peakRSS = 0
		if err == nil {
			state.peakRSS = rss
		}
	}
}

func materialMemoryGrowth(peakRSS, baselineRSS, minimum int64) bool {
	growth := minimum
	if relative := baselineRSS / 10; relative > growth {
		growth = relative
	}
	return peakRSS > baselineRSS+growth
}

type memoryJobShare struct {
	id      guid.GUID
	slot    uint32
	hasSlot bool
}

func (t *memoryTracker) assign(profile memoryProfile, rss int64) {
	activeIDs := t.jobs.ActiveIDs()
	active := make(map[guid.GUID]struct{}, len(activeIDs))
	shares := make([]memoryJobShare, 0, len(activeIDs))

	t.mu.Lock()
	for _, jobID := range activeIDs {
		active[jobID] = struct{}{}
		job := t.ensureJobLocked(jobID)
		shares = append(shares, memoryJobShare{id: jobID, slot: job.slot, hasSlot: job.hasSlot})
	}
	for jobID, job := range t.trackedJobs {
		if _, ok := active[jobID]; !ok && job.inFlight == 0 {
			delete(t.trackedJobs, jobID)
		}
	}
	usage := distributeMemory(rss, profile, shares)
	for jobID, value := range usage {
		t.trackedJobs[jobID].usage = value
	}
	t.mu.Unlock()
}

func distributeMemory(rss int64, profile memoryProfile, jobs []memoryJobShare) map[guid.GUID]int64 {
	result := make(map[guid.GUID]int64, len(jobs))
	if len(jobs) == 0 {
		return result
	}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].id.String() < jobs[j].id.String()
	})

	var knownInUse int64
	weights := make([]int64, len(jobs))
	for index, job := range jobs {
		if job.hasSlot {
			weights[index] = profile.inUseBySlot[job.slot]
			knownInUse += weights[index]
		}
	}

	coverage := 0.0
	if profile.totalInUse > 0 && knownInUse > 0 {
		coverage = min(1, float64(knownInUse)/float64(profile.totalInUse))
	}
	attributedRSS := float64(rss) * coverage
	residualPerJob := (float64(rss) - attributedRSS) / float64(len(jobs))
	assigned := int64(0)
	for index, job := range jobs {
		value := residualPerJob
		if knownInUse > 0 {
			value += attributedRSS * float64(weights[index]) / float64(knownInUse)
		}
		result[job.id] = int64(math.Round(value))
		assigned += result[job.id]
	}
	result[jobs[0].id] += rss - assigned
	return result
}
