package flow

import (
	"bytes"
	"context"
	"io"
	"runtime/pprof"
	"sync"
	"time"

	pprofprofile "github.com/google/pprof/profile"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/guid"
)

const (
	cpuJobLabel      = "yt_flow_job_id"
	cpuProfileWindow = time.Second
)

type cpuProfiler interface {
	Start(io.Writer) error
	Stop()
}

type runtimeCPUProfiler struct{}

func (runtimeCPUProfiler) Start(w io.Writer) error {
	return pprof.StartCPUProfile(w)
}

func (runtimeCPUProfiler) Stop() {
	pprof.StopCPUProfile()
}

type cpuTracker struct {
	jobs     *jobCache
	logger   log.Structured
	profiler cpuProfiler
	window   time.Duration

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func newCPUTracker(jobs *jobCache, logger log.Structured, profiler cpuProfiler, window time.Duration) *cpuTracker {
	return &cpuTracker{
		jobs:     jobs,
		logger:   logger,
		profiler: profiler,
		window:   window,
	}
}

func (t *cpuTracker) Start() {
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

func (t *cpuTracker) Stop() {
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

func (t *cpuTracker) run(ctx context.Context, done chan<- struct{}) {
	profiles := make(chan []byte, 1)
	parsed := make(chan struct{})
	go func() {
		defer close(parsed)
		for data := range profiles {
			if err := t.consume(data); err != nil {
				t.logger.Warn("flow: cannot attribute companion CPU profile", log.Error(err))
			}
		}
	}()

	defer func() {
		close(profiles)
		<-parsed
		close(done)
	}()

	reportedStartFailure := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var profile bytes.Buffer
		if err := t.profiler.Start(&profile); err != nil {
			if !reportedStartFailure {
				t.logger.Warn("flow: companion CPU accounting is unavailable", log.Error(err))
				reportedStartFailure = true
			}
			if !waitCPUProfileWindow(ctx, t.window) {
				return
			}
			continue
		}
		reportedStartFailure = false

		complete := waitCPUProfileWindow(ctx, t.window)
		t.profiler.Stop()
		profiles <- profile.Bytes()
		if !complete {
			return
		}
	}
}

func waitCPUProfileWindow(ctx context.Context, window time.Duration) bool {
	timer := time.NewTimer(window)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (t *cpuTracker) consume(data []byte) error {
	profile, err := pprofprofile.ParseData(data)
	if err != nil {
		return xerrors.Errorf("parse CPU profile: %w", err)
	}
	cpuByJob, err := cpuTimeByJob(profile)
	if err != nil {
		return err
	}
	for jobID, cpuTime := range cpuByJob {
		t.jobs.AddCPUTime(jobID, cpuTime)
	}
	return nil
}

func cpuTimeByJob(profile *pprofprofile.Profile) (map[guid.GUID]int64, error) {
	cpuIndex := -1
	for index, sampleType := range profile.SampleType {
		if sampleType.Type == "cpu" && sampleType.Unit == "nanoseconds" {
			cpuIndex = index
			break
		}
	}
	if cpuIndex == -1 {
		return nil, xerrors.Errorf("flow: CPU profile has no cpu/nanoseconds sample type")
	}

	result := make(map[guid.GUID]int64)
	for _, sample := range profile.Sample {
		if cpuIndex >= len(sample.Value) || sample.Value[cpuIndex] <= 0 {
			continue
		}
		labels := sample.Label[cpuJobLabel]
		if len(labels) != 1 {
			continue
		}
		jobID, err := guid.ParseString(labels[0])
		if err != nil {
			continue
		}
		result[jobID] += sample.Value[cpuIndex]
	}
	return result, nil
}

func withJobCPU(ctx context.Context, jobID guid.GUID, f func(context.Context)) {
	pprof.Do(ctx, pprof.Labels(cpuJobLabel, jobID.String()), f)
}
