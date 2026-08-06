package agent

import (
	"context"
	"errors"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
)

type OperationStatus struct {
	yt.OperationStatus
	CrashedJobs []yt.JobID
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
	return collectOperationsWithJobChecker(ctx, collectOpsCh, jobCheckerInputCh, jobCheckerOutputCh)
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
) <-chan OperationsOrError {
	eventCh := make(chan OperationsOrError)

	type aggrJobCheckerResult struct {
		crashedJobs map[yt.OperationID][]yt.JobID
		errs        []error
	}
	crashedJobsCh := make(chan aggrJobCheckerResult, 1)

	go func() {
		aggrRes := aggrJobCheckerResult{
			crashedJobs: make(map[yt.OperationID][]yt.JobID),
			errs:        make([]error, 0),
		}
		var outputCh chan<- aggrJobCheckerResult
		for {
			select {
			case <-ctx.Done():
				return

			case event := <-jobCheckerOutputCh:
				if event.AggrErr != nil {
					aggrRes.errs = append(aggrRes.errs, event.AggrErr)
				} else {
					for _, result := range event.Results {
						aggrRes.crashedJobs[result.ID] = append(aggrRes.crashedJobs[result.ID], result.CrashedJobs...)
					}
				}
				outputCh = crashedJobsCh

			case outputCh <- aggrRes:
				aggrRes.crashedJobs = make(map[yt.OperationID][]yt.JobID)
				aggrRes.errs = make([]error, 0)
				outputCh = nil
			}
		}
	}()

	go func() {
		sendToJobChecker := contextGuardedChanSender(ctx, jobCheckerInputCh)
		sendToEventCh := contextGuardedChanSender(ctx, eventCh)

		aggrRes := aggrJobCheckerResult{
			crashedJobs: make(map[yt.OperationID][]yt.JobID),
			errs:        make([]error, 0),
		}

		for {
			select {
			case <-ctx.Done():
				return

			case incomingMsg := <-crashedJobsCh:
				aggrRes.errs = append(aggrRes.errs, incomingMsg.errs...)
				for opID, jobs := range incomingMsg.crashedJobs {
					aggrRes.crashedJobs[opID] = append(aggrRes.crashedJobs[opID], jobs...)
				}

			case event := <-collectOpsCh:
				if len(aggrRes.errs) > 0 {
					checkError := errors.Join(aggrRes.errs...)
					aggrRes.errs = aggrRes.errs[:0]

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

				for i := range event.Operations {
					opID := event.Operations[i].ID
					if jobs, ok := aggrRes.crashedJobs[opID]; ok {
						event.Operations[i].CrashedJobs = append(event.Operations[i].CrashedJobs, jobs...)
						delete(aggrRes.crashedJobs, opID)
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
