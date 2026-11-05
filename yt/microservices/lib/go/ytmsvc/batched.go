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
	return batchedOperation(ctx, mr, tx, sortSpec, workDir, yt.OperationSort)
}

func BatchedMerge(ctx context.Context, mr mapreduce.Client, tx yt.Tx, mergeSpec *spec.Spec, workDir ypath.Path) error {
	return batchedOperation(ctx, mr, tx, mergeSpec, workDir, yt.OperationMerge)
}

func batchedOperation(ctx context.Context, mr mapreduce.Client, tx yt.Tx, operationSpec *spec.Spec, workDir ypath.Path, operationType yt.OperationType) error {
	const maxInputTables = 256
	queue := make([]ypath.YPath, len(operationSpec.InputTablePaths))
	copy(queue, operationSpec.InputTablePaths)

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
	if err := tx.GetNode(ctx, operationSpec.OutputTablePath.YPath().Attr("schema"), &outputSchema, nil); err != nil {
		return xerrors.Errorf("failed to get output table schema: %w", err)
	}
	tableAttributes := map[string]any{"schema": outputSchema}
	if operationType == yt.OperationMerge && len(queue) > maxInputTables {
		for _, name := range []string{"compression_codec", "optimize_for", "erasure_codec"} {
			var value any
			if err := tx.GetNode(ctx, operationSpec.OutputTablePath.YPath().Attr(name), &value, nil); err != nil {
				return xerrors.Errorf("failed to get output table %s: %w", name, err)
			}
			tableAttributes[name] = value
		}
	}

	nestedMR := mr.WithTx(nestedTx)
	currentStep := 1
	var tmpTables []ypath.YPath

	for {
		batchSpec := *operationSpec
		isFinalStep := len(queue) <= maxInputTables

		if isFinalStep {
			batchSpec.InputTablePaths = queue
		} else {
			batchSpec.InputTablePaths = queue[:maxInputTables]

			tmpTable := workDir.Child(fmt.Sprintf("tmp_%s_step_%d", operationType, currentStep))
			batchSpec.OutputTablePath = tmpTable
			tmpTables = append(tmpTables, tmpTable)

			if _, err := nestedTx.CreateNode(ctx, tmpTable, yt.NodeTable, &yt.CreateNodeOptions{
				Attributes: tableAttributes,
			}); err != nil {
				return xerrors.Errorf("failed to create temp table %q: %w", tmpTable, err)
			}
		}

		if totalSteps > 1 {
			batchSpec.Title = fmt.Sprintf("%s [%d/%d]", operationSpec.Title, currentStep, totalSteps)
		}
		var op mapreduce.Operation
		if operationType == yt.OperationMerge {
			op, err = nestedMR.Merge(&batchSpec)
		} else {
			op, err = nestedMR.Sort(&batchSpec)
		}
		if err != nil {
			return xerrors.Errorf("failed to create %s operation (step %d/%d): %w", operationType, currentStep, totalSteps, err)
		}
		if err := op.Wait(); err != nil {
			return xerrors.Errorf("failed to wait for %s operation (step %d/%d): %w", operationType, currentStep, totalSteps, err)
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
