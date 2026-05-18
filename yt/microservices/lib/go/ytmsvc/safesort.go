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

func SafeSort(ctx context.Context, mr mapreduce.Client, tx yt.Tx, sortSpec *spec.Spec, workDir ypath.Path) error {
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

	for len(queue) > maxInputTables {
		tmpTable := workDir.Child(fmt.Sprintf("tmp_sort_step_%d", currentStep))
		tmpTables = append(tmpTables, tmpTable)

		if _, err := nestedTx.CreateNode(ctx, tmpTable, yt.NodeTable, &yt.CreateNodeOptions{
			Attributes: map[string]any{
				"schema": outputSchema,
			},
		}); err != nil {
			return xerrors.Errorf("failed to create temp table %q: %w", tmpTable, err)
		}

		batchSpec := *sortSpec
		batchSpec.InputTablePaths = queue[:maxInputTables]
		batchSpec.OutputTablePath = tmpTable
		batchSpec.Title = fmt.Sprintf("%s [%d/%d]", sortSpec.Title, currentStep, totalSteps)

		sortOp, err := nestedMR.Sort(&batchSpec)
		if err != nil {
			return xerrors.Errorf("failed to create intermediate sort operation (step %d/%d): %w", currentStep, totalSteps, err)
		}
		if err := sortOp.Wait(); err != nil {
			return xerrors.Errorf("failed to wait for intermediate sort operation (step %d/%d): %w", currentStep, totalSteps, err)
		}

		queue = append(queue[maxInputTables:], tmpTable)
		currentStep++
	}

	finalSpec := *sortSpec
	finalSpec.InputTablePaths = queue
	if totalSteps > 1 {
		finalSpec.Title = fmt.Sprintf("%s [%d/%d]", sortSpec.Title, currentStep, totalSteps)
	}

	sortOp, err := nestedMR.Sort(&finalSpec)
	if err != nil {
		return xerrors.Errorf("failed to create sort operation: %w", err)
	}

	if err := sortOp.Wait(); err != nil {
		return xerrors.Errorf("failed to wait for sort operation: %w", err)
	}

	for _, tmpPath := range tmpTables {
		if err := nestedTx.RemoveNode(ctx, tmpPath, &yt.RemoveNodeOptions{Force: true}); err != nil {
			return xerrors.Errorf("failed to remove temp table %q: %w", tmpPath, err)
		}
	}

	return nestedTx.Commit()
}
