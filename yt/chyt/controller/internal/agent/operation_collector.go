package agent

import (
	"context"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/yt"
)

func CollectOperations(
	ctx context.Context,
	ytc yt.Client,
	period time.Duration,
	opNamespace string,
	l log.Logger) <-chan []yt.OperationStatus {
	l.Debug("operation collecting started", log.Duration("period", period))

	eventCh := make(chan []yt.OperationStatus)

	go func() {
		ticker := time.NewTicker(period)

		for {
			select {
			case <-ctx.Done():
				l.Debug("operation collecting finished")
				close(eventCh)
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
					}

					opIDs := make([]string, len(runningOps))
					for i, op := range runningOps {
						opIDs[i] = op.ID.String()
					}

					l.Info("collected running operations",
						log.Strings("operation_ids", opIDs),
						log.Duration("elapsed_time", time.Since(startedAt)),
						log.Int("total_operations_count", len(runningOps)))

					eventCh <- runningOps
				}
			}
		}
	}()

	return eventCh
}
