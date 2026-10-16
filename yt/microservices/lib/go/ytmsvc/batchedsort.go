package ytmsvc

import (
	"context"
	"fmt"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func BatchedSort(ctx context.Context, mr mapreduce.Client, tx yt.Tx, sortSpec *spec.Spec, workDir ypath.Path) error {
	const maxInputTables = 256
	queue := make([]ypath.YPath, len(sortSpec.InputTablePaths))
	copy(queue, sortSpec.InputTablePaths)

	totalSteps := 1
	if len(queue) > maxInputTables {
		totalSteps += (len(queue) - 2) / (maxInputTables - 1)
	}

	nestedTx, err := tx.BeginTx(ctx, nil)
	if err != nil {
		return xerrors.Errorf("failed to begin nested transaction: %w", err)
	}
	defer func() { _ = nestedTx.Abort() }()

	var outputSchema any
	if err := tx.GetNode(ctx, sortSpec.OutputTablePath.YPath().Attr("schema"), &outputSchema, nil); err != nil {
		return xerrors.Errorf("failed to get output table schema: %w", err)
	}

	nestedMR := mr.WithTx(nestedTx)
	currentStep := 1
	var tmpTables []ypath.YPath

	for {
		batchSpec := *sortSpec
		isFinalStep := len(queue) <= maxInputTables

		if isFinalStep {
			batchSpec.InputTablePaths = queue
		} else {
			batchSpec.InputTablePaths = queue[:maxInputTables]

			tmpTable := workDir.Child(fmt.Sprintf("tmp_sort_step_%d", currentStep))
			batchSpec.OutputTablePath = tmpTable
			tmpTables = append(tmpTables, tmpTable)

			if _, err := nestedTx.CreateNode(ctx, tmpTable, yt.NodeTable, &yt.CreateNodeOptions{
				Attributes: map[string]any{
					"schema": outputSchema,
				},
			}); err != nil {
				return xerrors.Errorf("failed to create temp table %q: %w", tmpTable, err)
			}
		}

		if totalSteps > 1 {
			batchSpec.Title = fmt.Sprintf("%s [%d/%d]", sortSpec.Title, currentStep, totalSteps)
		}
		sortOp, err := nestedMR.Sort(&batchSpec)
		if err != nil {
			return xerrors.Errorf("failed to create sort operation (step %d/%d): %w", currentStep, totalSteps, err)
		}
		if err := sortOp.Wait(); err != nil {
			return xerrors.Errorf("failed to wait for sort operation (step %d/%d): %w", currentStep, totalSteps, err)
		}
		if isFinalStep {
			break
		}

		queue = append(queue[maxInputTables:], batchSpec.OutputTablePath)
		currentStep++
	}

	for _, tmpPath := range tmpTables {
		if err := nestedTx.RemoveNode(ctx, tmpPath, &yt.RemoveNodeOptions{Force: true}); err != nil {
			return xerrors.Errorf("failed to remove temp table %q: %w", tmpPath, err)
		}
	}

	return nestedTx.Commit()
}
