package agent

import (
	"context"
	"errors"
	"slices"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
)

type OperationStatus struct {
	yt.OperationStatus
	CrashedJobs []yt.JobID
	// MaxUnavailableJobs is the maximum within the collector's window;
	// nil means no successful job checker result has been received yet.
	MaxUnavailableJobs *int
}

type OperationsOrError struct {
	Operations []OperationStatus
	Error      error
}

func runCollectOperationsRoutine(
	ctx context.Context,
	ytc yt.Client,
	period time.Duration,
	opNamespace string,
	l log.Logger) <-chan OperationsOrError {
	l.Debug("operation collecting started", log.Duration("period", period))

	eventCh := make(chan OperationsOrError)

	go func() {
		ticker := time.NewTicker(period)

		for {
			select {
			case <-ctx.Done():
				l.Debug("operation collecting finished")
				return
			case <-ticker.C:
				{
					l.Info("collecting running operations")

					startedAt := time.Now()

					optFilter := `"strawberry_operation_namespace"="` + opNamespace + `"`
					optState := yt.StateRunning
					optType := yt.OperationVanilla

					runningOps, err := yt.ListAllOperations(
						ctx,
						ytc,
						&yt.ListOperationsOptions{
							Filter: &optFilter,
							State:  &optState,
							Type:   &optType,
							MasterReadOptions: &yt.MasterReadOptions{
								ReadFrom: yt.ReadFromFollower,
							},
						})

					if err != nil {
						l.Error("error collecting running operations", log.Error(err))
						eventCh <- OperationsOrError{Error: err}
						continue
					}

					opStatuses := make([]OperationStatus, len(runningOps))
					opIDs := make([]string, len(runningOps))
					for i, op := range runningOps {
						opIDs[i] = op.ID.String()
						opStatuses[i].OperationStatus = op
					}

					l.Info("collected running operations",
						log.Strings("operation_ids", opIDs),
						log.Duration("elapsed_time", time.Since(startedAt)),
						log.Int("total_operations_count", len(runningOps)))

					eventCh <- OperationsOrError{Operations: opStatuses}
				}
			}
		}
	}()

	return eventCh
}

type unavailabilityInterval struct {
	start  time.Time
	finish time.Time
}

type unavailabilityWindow struct {
	intervals    []unavailabilityInterval
	maximum      int
	needsRecount bool
}

func (w *unavailabilityWindow) add(intervals []unavailabilityInterval) {
	w.intervals = append(w.intervals, intervals...)
	w.needsRecount = true
}

func (w *unavailabilityWindow) maxUnavailableJobs(windowStart time.Time) int {
	previousCount := len(w.intervals)
	w.intervals = slices.DeleteFunc(w.intervals, func(iv unavailabilityInterval) bool {
		return !iv.finish.After(windowStart)
	})
	if w.needsRecount || len(w.intervals) != previousCount {
		w.maximum = maxIntervalsIntersection(w.intervals)
		w.needsRecount = false
	}
	if len(w.intervals) == 0 {
		w.intervals = nil
	}
	return w.maximum
}

func maxIntervalsIntersection(intervals []unavailabilityInterval) (result int) {
	type event struct {
		t     time.Time
		delta int
	}

	events := make([]event, 0, len(intervals)*2)
	for _, iv := range intervals {
		events = append(events, event{iv.start, 1}, event{iv.finish, -1})
	}

	slices.SortFunc(events, func(a, b event) int {
		if c := a.t.Compare(b.t); c != 0 {
			return c
		}
		return a.delta - b.delta
	})

	var current int
	for _, e := range events {
		current += e.delta
		if current > result {
			result = current
		}
	}

	return
}

func CollectOperations(
	ctx context.Context,
	ytc yt.Client,
	l log.Logger,
	period time.Duration,
	opNamespace string,
	cfg *JobCheckerConfig) <-chan OperationsOrError {

	collectOpsCh := runCollectOperationsRoutine(ctx, ytc, period, opNamespace, l.WithName("track_ops"))

	if cfg == nil {
		return collectOpsCh
	}

	jobCheckerInputCh, jobCheckerOutputCh := CheckFinishedJobs(ctx, ytc, l.WithName("check_jobs"), cfg)
	return collectOperationsWithJobChecker(ctx, collectOpsCh, jobCheckerInputCh, jobCheckerOutputCh,
		time.Duration(cfg.UnavailabilityWindowWidthOrDefault()))
}

func contextGuardedChanSender[T any](ctx context.Context, ch chan<- T) func(T) bool {
	return func(v T) bool {
		select {
		case ch <- v:
			return true
		case <-ctx.Done():
			return false
		}
	}
}

func collectOperationsWithJobChecker(
	ctx context.Context,
	collectOpsCh <-chan OperationsOrError,
	jobCheckerInputCh chan<- []OperationStatus,
	jobCheckerOutputCh <-chan JobCheckerResultBatch,
	unavailabilityWindowWidth time.Duration,
) <-chan OperationsOrError {
	eventCh := make(chan OperationsOrError)

	type aggrJobCheckerResult struct {
		crashedJobs             map[yt.OperationID][]yt.JobID
		unavailabilityIntervals map[yt.OperationID][]unavailabilityInterval
		errs                    []error
	}
	jobCheckerResultsCh := make(chan aggrJobCheckerResult, 1)

	go func() {
		aggrRes := aggrJobCheckerResult{
			crashedJobs:             make(map[yt.OperationID][]yt.JobID),
			unavailabilityIntervals: make(map[yt.OperationID][]unavailabilityInterval),
			errs:                    make([]error, 0),
		}
		var outputCh chan<- aggrJobCheckerResult
		for {
			select {
			case <-ctx.Done():
				return

			case event := <-jobCheckerOutputCh:
				if event.AggrErr != nil {
					aggrRes.errs = append(aggrRes.errs, event.AggrErr)
				}
				for _, result := range event.Results {
					aggrRes.crashedJobs[result.ID] = append(aggrRes.crashedJobs[result.ID], result.CrashedJobs...)
					aggrRes.unavailabilityIntervals[result.ID] = append(aggrRes.unavailabilityIntervals[result.ID], result.UnavailabilityIntervals...)
				}
				outputCh = jobCheckerResultsCh

			case outputCh <- aggrRes:
				aggrRes.crashedJobs = make(map[yt.OperationID][]yt.JobID)
				aggrRes.unavailabilityIntervals = make(map[yt.OperationID][]unavailabilityInterval)
				aggrRes.errs = make([]error, 0)
				outputCh = nil
			}
		}
	}()

	go func() {
		sendToJobChecker := contextGuardedChanSender(ctx, jobCheckerInputCh)
		sendToEventCh := contextGuardedChanSender(ctx, eventCh)

		crashedJobs := make(map[yt.OperationID][]yt.JobID)
		windows := make(map[yt.OperationID]*unavailabilityWindow)
		var runningOpIDs map[yt.OperationID]struct{}
		var errs []error

		for {
			select {
			case <-ctx.Done():
				return

			case incomingMsg := <-jobCheckerResultsCh:
				errs = append(errs, incomingMsg.errs...)
				for opID, intervals := range incomingMsg.unavailabilityIntervals {
					if _, ok := runningOpIDs[opID]; !ok {
						continue
					}
					crashedJobs[opID] = append(crashedJobs[opID], incomingMsg.crashedJobs[opID]...)
					window, ok := windows[opID]
					if !ok {
						window = &unavailabilityWindow{}
						windows[opID] = window
					}
					window.add(intervals)
				}

			case event := <-collectOpsCh:
				if len(errs) > 0 {
					checkError := errors.Join(errs...)
					errs = errs[:0]

					event.Error = errors.Join(event.Error, checkError)
				}

				if event.Error != nil {
					if !sendToEventCh(event) {
						return
					}
					continue
				}

				if !sendToJobChecker(event.Operations) {
					return
				}

				runningOpIDs = make(map[yt.OperationID]struct{}, len(event.Operations))
				windowStart := time.Now().Add(-unavailabilityWindowWidth)
				for i := range event.Operations {
					opID := event.Operations[i].ID
					runningOpIDs[opID] = struct{}{}
					if jobs, ok := crashedJobs[opID]; ok {
						event.Operations[i].CrashedJobs = append(event.Operations[i].CrashedJobs, jobs...)
						delete(crashedJobs, opID)
					}
					if window, ok := windows[opID]; ok {
						maxUnavailableJobs := window.maxUnavailableJobs(windowStart)
						event.Operations[i].MaxUnavailableJobs = &maxUnavailableJobs
					}
				}
				for opID := range windows {
					if _, ok := runningOpIDs[opID]; !ok {
						delete(windows, opID)
						delete(crashedJobs, opID)
					}
				}

				if !sendToEventCh(event) {
					return
				}
			}
		}
	}()

	return eventCh
}
