package spec

import "go.ytsaurus.tech/yt/go/yt"

// ConfigureJobIOForDynamicTable sets appropriate IO settings for operation
// that outputs static table that would be later converted to dynamic table.
func ConfigureJobIOForDynamicTable(spec *Spec) {
	setJobIO := func(io **JobIO) {
		if *io == nil {
			*io = new(JobIO)
		}

		(*io).TableWriter = map[string]interface{}{
			"desired_chunk_size": 100 * 1024 * 1024,
			"block_size":         256 * 1024,
		}
	}

	switch spec.Type {
	case yt.OperationMap:
		setJobIO(&spec.MapJobIO)

	case yt.OperationReduce:
		setJobIO(&spec.ReduceJobIO)

	case yt.OperationMerge:
		setJobIO(&spec.MergeJobIO)

	case yt.OperationVanilla:
		for _, us := range spec.Tasks {
			setJobIO(&us.JobIO)
		}

	case yt.OperationSort:
		setJobIO(&spec.SortJobIO)
	}
}
