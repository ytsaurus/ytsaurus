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

	eventCh := make(chan OperationsOrError)
	jobCheckerInputCh, jobCheckerOutputCh := CheckFinishedJobs(ctx, ytc, l.WithName("check_jobs"), cfg)
	go func() {
		crashedJobs := make(map[yt.OperationID][]yt.JobID)
		jobCheckerErrs := make([]error, 0)
		for {
			select {
			case <-ctx.Done():
				return

			case event := <-jobCheckerOutputCh:
				if event.AggrErr != nil {
					jobCheckerErrs = append(jobCheckerErrs, event.AggrErr)
					continue
				}

				for _, result := range event.Results {
					crashedJobs[result.ID] = append(crashedJobs[result.ID], result.CrashedJobs...)
				}

			case event := <-collectOpsCh:
				if len(jobCheckerErrs) > 0 {
					checkError := errors.Join(jobCheckerErrs...)
					jobCheckerErrs = jobCheckerErrs[:0]

					event.Error = errors.Join(event.Error, checkError)
				}

				if event.Error != nil {
					eventCh <- event
					continue
				}

				jobCheckerInputCh <- event.Operations

				for i := range event.Operations {
					opID := event.Operations[i].ID
					if jobs, ok := crashedJobs[opID]; ok {
						event.Operations[i].CrashedJobs = append(event.Operations[i].CrashedJobs, jobs...)
						delete(crashedJobs, opID)
					}
				}

				eventCh <- event
			}
		}
	}()

	return eventCh
}
