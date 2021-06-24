package rpcclient

import (
	"testing"

	"a.yandex-team.ru/yt/go/proto/client/api/rpc_proxy"
	"a.yandex-team.ru/yt/go/yt"
	"github.com/stretchr/testify/require"
)

func TestConvertOperationType(t *testing.T) {
	ptr := func(typ rpc_proxy.EOperationType) *rpc_proxy.EOperationType {
		return &typ
	}

	unexpectedOpType := yt.OperationType("jump")

	for _, tc := range []struct {
		name     string
		typ      *yt.OperationType
		expected *rpc_proxy.EOperationType
		err      bool
	}{
		{name: "nil", typ: nil, expected: nil},
		{name: "Map", typ: &yt.OperationMap, expected: ptr(rpc_proxy.EOperationType_OT_MAP)},
		{name: "Reduce", typ: &yt.OperationReduce, expected: ptr(rpc_proxy.EOperationType_OT_REDUCE)},
		{name: "MapReduce", typ: &yt.OperationMapReduce, expected: ptr(rpc_proxy.EOperationType_OT_MAP_REDUCE)},
		{name: "Sort", typ: &yt.OperationSort, expected: ptr(rpc_proxy.EOperationType_OT_SORT)},
		{name: "Merge", typ: &yt.OperationMerge, expected: ptr(rpc_proxy.EOperationType_OT_MERGE)},
		{name: "Erase", typ: &yt.OperationErase, expected: ptr(rpc_proxy.EOperationType_OT_ERASE)},
		{name: "RemoteCopy", typ: &yt.OperationRemoteCopy, expected: ptr(rpc_proxy.EOperationType_OT_REMOTE_COPY)},
		{name: "Vanilla", typ: &yt.OperationVanilla, expected: ptr(rpc_proxy.EOperationType_OT_VANILLA)},
		{name: "unexpected", typ: &unexpectedOpType, err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			converted, err := convertOperationType(tc.typ)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tc.expected == nil {
					require.Nil(t, converted)
				} else {
					require.Equal(t, *tc.expected, *converted)
				}
			}
		})
	}
}

func TestMakeOperationType(t *testing.T) {
	ptr := func(typ rpc_proxy.EOperationType) *rpc_proxy.EOperationType {
		return &typ
	}

	for _, tc := range []struct {
		name     string
		typ      *rpc_proxy.EOperationType
		expected yt.OperationType
		err      bool
	}{
		{name: "nil", typ: nil, err: true},
		{name: "Map", typ: ptr(rpc_proxy.EOperationType_OT_MAP), expected: yt.OperationMap},
		{name: "Reduce", typ: ptr(rpc_proxy.EOperationType_OT_REDUCE), expected: yt.OperationReduce},
		{name: "MapReduce", typ: ptr(rpc_proxy.EOperationType_OT_MAP_REDUCE), expected: yt.OperationMapReduce},
		{name: "Sort", typ: ptr(rpc_proxy.EOperationType_OT_SORT), expected: yt.OperationSort},
		{name: "Merge", typ: ptr(rpc_proxy.EOperationType_OT_MERGE), expected: yt.OperationMerge},
		{name: "Erase", typ: ptr(rpc_proxy.EOperationType_OT_ERASE), expected: yt.OperationErase},
		{name: "RemoteCopy", typ: ptr(rpc_proxy.EOperationType_OT_REMOTE_COPY), expected: yt.OperationRemoteCopy},
		{name: "Vanilla", typ: ptr(rpc_proxy.EOperationType_OT_VANILLA), expected: yt.OperationVanilla},
		{name: "JoinReduce", typ: ptr(rpc_proxy.EOperationType_OT_JOIN_REDUCE), err: true},
		{name: "unexpected", typ: ptr(rpc_proxy.EOperationType(31337)), err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			converted, err := makeOperationType(tc.typ)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, converted)
			}
		})
	}
}

func TestConvertOperationState(t *testing.T) {
	ptr := func(state rpc_proxy.EOperationState) *rpc_proxy.EOperationState {
		return &state
	}

	unexpectedOpState := yt.OperationState("jumping")

	for _, tc := range []struct {
		name     string
		state    *yt.OperationState
		expected *rpc_proxy.EOperationState
		err      bool
	}{
		{name: "nil", state: nil, expected: nil},
		{name: "Running", state: &yt.StateRunning, expected: ptr(rpc_proxy.EOperationState_OS_RUNNING)},
		{name: "Pending", state: &yt.StatePending, expected: ptr(rpc_proxy.EOperationState_OS_PENDING)},
		{name: "Completed", state: &yt.StateCompleted, expected: ptr(rpc_proxy.EOperationState_OS_COMPLETED)},
		{name: "Failed", state: &yt.StateFailed, expected: ptr(rpc_proxy.EOperationState_OS_FAILED)},
		{name: "Aborted", state: &yt.StateAborted, expected: ptr(rpc_proxy.EOperationState_OS_ABORTED)},
		{name: "Reviving", state: &yt.StateReviving, expected: ptr(rpc_proxy.EOperationState_OS_REVIVING)},
		{name: "Initializing", state: &yt.StateInitializing, expected: ptr(rpc_proxy.EOperationState_OS_INITIALIZING)},
		{name: "Preparing", state: &yt.StatePreparing, expected: ptr(rpc_proxy.EOperationState_OS_PREPARING)},
		{name: "Materializing", state: &yt.StateMaterializing, expected: ptr(rpc_proxy.EOperationState_OS_MATERIALIZING)},
		{name: "Completing", state: &yt.StateCompleting, expected: ptr(rpc_proxy.EOperationState_OS_COMPLETING)},
		{name: "Aborting", state: &yt.StateAborting, expected: ptr(rpc_proxy.EOperationState_OS_ABORTING)},
		{name: "Failing", state: &yt.StateFailing, expected: ptr(rpc_proxy.EOperationState_OS_FAILING)},
		{name: "unexpected", state: &unexpectedOpState, err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			converted, err := convertOperationState(tc.state)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tc.expected == nil {
					require.Nil(t, converted)
				} else {
					require.Equal(t, *tc.expected, *converted)
				}
			}
		})
	}
}

func TestMakeOperationState(t *testing.T) {
	ptr := func(state rpc_proxy.EOperationState) *rpc_proxy.EOperationState {
		return &state
	}

	for _, tc := range []struct {
		name     string
		state    *rpc_proxy.EOperationState
		expected yt.OperationState
		err      bool
	}{
		{name: "nil", state: nil, err: true},
		{name: "Running", state: ptr(rpc_proxy.EOperationState_OS_RUNNING), expected: yt.StateRunning},
		{name: "Pending", state: ptr(rpc_proxy.EOperationState_OS_PENDING), expected: yt.StatePending},
		{name: "Completed", state: ptr(rpc_proxy.EOperationState_OS_COMPLETED), expected: yt.StateCompleted},
		{name: "Failed", state: ptr(rpc_proxy.EOperationState_OS_FAILED), expected: yt.StateFailed},
		{name: "Aborted", state: ptr(rpc_proxy.EOperationState_OS_ABORTED), expected: yt.StateAborted},
		{name: "Reviving", state: ptr(rpc_proxy.EOperationState_OS_REVIVING), expected: yt.StateReviving},
		{name: "Initializing", state: ptr(rpc_proxy.EOperationState_OS_INITIALIZING), expected: yt.StateInitializing},
		{name: "Preparing", state: ptr(rpc_proxy.EOperationState_OS_PREPARING), expected: yt.StatePreparing},
		{name: "Materializing", state: ptr(rpc_proxy.EOperationState_OS_MATERIALIZING), expected: yt.StateMaterializing},
		{name: "Completing", state: ptr(rpc_proxy.EOperationState_OS_COMPLETING), expected: yt.StateCompleting},
		{name: "Aborting", state: ptr(rpc_proxy.EOperationState_OS_ABORTING), expected: yt.StateAborting},
		{name: "Failing", state: ptr(rpc_proxy.EOperationState_OS_FAILING), expected: yt.StateFailing},
		{name: "None", state: ptr(rpc_proxy.EOperationState_OS_NONE), err: true},
		{name: "Starting", state: ptr(rpc_proxy.EOperationState_OS_STARTING), err: true},
		{name: "Orphaned", state: ptr(rpc_proxy.EOperationState_OS_ORPHANED), err: true},
		{name: "WaitingForAgent", state: ptr(rpc_proxy.EOperationState_OS_WAITING_FOR_AGENT), err: true},
		{name: "RevivingJobs", state: ptr(rpc_proxy.EOperationState_OS_REVIVING_JOBS), err: true},
		{name: "unexpected", state: ptr(rpc_proxy.EOperationState(31337)), err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			converted, err := makeOperationState(tc.state)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, converted)
			}
		})
	}
}

func TestConvertJobType(t *testing.T) {
	ptr := func(typ rpc_proxy.EJobType) *rpc_proxy.EJobType {
		return &typ
	}

	unexpectedJobType := yt.JobType("jump")

	for _, tc := range []struct {
		name     string
		typ      *yt.JobType
		expected *rpc_proxy.EJobType
		err      bool
	}{
		{name: "nil", typ: nil, expected: nil},
		{name: "Map", typ: &yt.JobTypeMap, expected: ptr(rpc_proxy.EJobType_JT_MAP)},
		{name: "PartitionMap", typ: &yt.JobTypePartitionMap, expected: ptr(rpc_proxy.EJobType_JT_PARTITION_MAP)},
		{name: "SortedMerge", typ: &yt.JobTypeSortedMerge, expected: ptr(rpc_proxy.EJobType_JT_SORTED_MERGE)},
		{name: "OrderedMerge", typ: &yt.JobTypeOrderedMerge, expected: ptr(rpc_proxy.EJobType_JT_ORDERED_MERGE)},
		{name: "UnorderedMerge", typ: &yt.JobTypeUnorderedMerge, expected: ptr(rpc_proxy.EJobType_JT_UNORDERED_MERGE)},
		{name: "Partition", typ: &yt.JobTypePartition, expected: ptr(rpc_proxy.EJobType_JT_PARTITION)},
		{name: "SimpleSort", typ: &yt.JobTypeSimpleSort, expected: ptr(rpc_proxy.EJobType_JT_SIMPLE_SORT)},
		{name: "FinalSort", typ: &yt.JobTypeFinalSort, expected: ptr(rpc_proxy.EJobType_JT_FINAL_SORT)},
		{name: "SortedReduce", typ: &yt.JobTypeSortedReduce, expected: ptr(rpc_proxy.EJobType_JT_SORTED_REDUCE)},
		{name: "PartitionReduce", typ: &yt.JobTypePartitionReduce, expected: ptr(rpc_proxy.EJobType_JT_PARTITION_REDUCE)},
		{name: "ReduceCombiner", typ: &yt.JobTypeReduceCombiner, expected: ptr(rpc_proxy.EJobType_JT_REDUCE_COMBINER)},
		{name: "RemoteCopy", typ: &yt.JobTypeRemoteCopy, expected: ptr(rpc_proxy.EJobType_JT_REMOTE_COPY)},
		{name: "IntermediateSort", typ: &yt.JobTypeIntermediateSort, expected: ptr(rpc_proxy.EJobType_JT_INTERMEDIATE_SORT)},
		{name: "OrderedMap", typ: &yt.JobTypeOrderedMap, expected: ptr(rpc_proxy.EJobType_JT_ORDERED_MAP)},
		{name: "JoinReduce", typ: &yt.JobTypeJoinReduce, expected: ptr(rpc_proxy.EJobType_JT_JOIN_REDUCE)},
		{name: "Vanilla", typ: &yt.JobTypeVanilla, expected: ptr(rpc_proxy.EJobType_JT_VANILLA)},
		{name: "SchedulerUnknown", typ: &yt.JobTypeSchedulerUnknown, expected: ptr(rpc_proxy.EJobType_JT_SCHEDULER_UNKNOWN)},
		{name: "unexpected", typ: &unexpectedJobType, err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			converted, err := convertJobType(tc.typ)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tc.expected == nil {
					require.Nil(t, converted)
				} else {
					require.Equal(t, *tc.expected, *converted)
				}
			}
		})
	}
}

func TestMakeJobType(t *testing.T) {
	ptr := func(typ rpc_proxy.EJobType) *rpc_proxy.EJobType {
		return &typ
	}

	for _, tc := range []struct {
		name     string
		typ      *rpc_proxy.EJobType
		expected yt.JobType
		err      bool
	}{
		{name: "nil", typ: nil, err: true},
		{name: "Map", typ: ptr(rpc_proxy.EJobType_JT_MAP), expected: yt.JobTypeMap},
		{name: "PartitionMap", typ: ptr(rpc_proxy.EJobType_JT_PARTITION_MAP), expected: yt.JobTypePartitionMap},
		{name: "SortedMerge", typ: ptr(rpc_proxy.EJobType_JT_SORTED_MERGE), expected: yt.JobTypeSortedMerge},
		{name: "OrderedMerge", typ: ptr(rpc_proxy.EJobType_JT_ORDERED_MERGE), expected: yt.JobTypeOrderedMerge},
		{name: "UnorderedMerge", typ: ptr(rpc_proxy.EJobType_JT_UNORDERED_MERGE), expected: yt.JobTypeUnorderedMerge},
		{name: "Partition", typ: ptr(rpc_proxy.EJobType_JT_PARTITION), expected: yt.JobTypePartition},
		{name: "SimpleSort", typ: ptr(rpc_proxy.EJobType_JT_SIMPLE_SORT), expected: yt.JobTypeSimpleSort},
		{name: "FinalSort", typ: ptr(rpc_proxy.EJobType_JT_FINAL_SORT), expected: yt.JobTypeFinalSort},
		{name: "SortedReduce", typ: ptr(rpc_proxy.EJobType_JT_SORTED_REDUCE), expected: yt.JobTypeSortedReduce},
		{name: "PartitionReduce", typ: ptr(rpc_proxy.EJobType_JT_PARTITION_REDUCE), expected: yt.JobTypePartitionReduce},
		{name: "ReduceCombiner", typ: ptr(rpc_proxy.EJobType_JT_REDUCE_COMBINER), expected: yt.JobTypeReduceCombiner},
		{name: "RemoteCopy", typ: ptr(rpc_proxy.EJobType_JT_REMOTE_COPY), expected: yt.JobTypeRemoteCopy},
		{name: "IntermediateSort", typ: ptr(rpc_proxy.EJobType_JT_INTERMEDIATE_SORT), expected: yt.JobTypeIntermediateSort},
		{name: "OrderedMap", typ: ptr(rpc_proxy.EJobType_JT_ORDERED_MAP), expected: yt.JobTypeOrderedMap},
		{name: "JoinReduce", typ: ptr(rpc_proxy.EJobType_JT_JOIN_REDUCE), expected: yt.JobTypeJoinReduce},
		{name: "Vanilla", typ: ptr(rpc_proxy.EJobType_JT_VANILLA), expected: yt.JobTypeVanilla},
		{name: "SchedulerUnknown", typ: ptr(rpc_proxy.EJobType_JT_SCHEDULER_UNKNOWN), expected: yt.JobTypeSchedulerUnknown},
		{name: "unexpected", typ: ptr(rpc_proxy.EJobType(31337)), err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			converted, err := makeJobType(tc.typ)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, converted)
			}
		})
	}
}

func TestConvertJobState(t *testing.T) {
	ptr := func(state rpc_proxy.EJobState) *rpc_proxy.EJobState {
		return &state
	}

	unexpectedJobState := yt.JobState("jumping")

	for _, tc := range []struct {
		name     string
		state    *yt.JobState
		expected *rpc_proxy.EJobState
		err      bool
	}{
		{name: "nil", state: nil, expected: nil},
		{name: "Running", state: &yt.JobRunning, expected: ptr(rpc_proxy.EJobState_JS_RUNNING)},
		{name: "Waiting", state: &yt.JobWaiting, expected: ptr(rpc_proxy.EJobState_JS_WAITING)},
		{name: "Completed", state: &yt.JobCompleted, expected: ptr(rpc_proxy.EJobState_JS_COMPLETED)},
		{name: "Failed", state: &yt.JobFailed, expected: ptr(rpc_proxy.EJobState_JS_FAILED)},
		{name: "Aborted", state: &yt.JobAborted, expected: ptr(rpc_proxy.EJobState_JS_ABORTED)},
		{name: "unexpected", state: &unexpectedJobState, err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			converted, err := convertJobState(tc.state)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tc.expected == nil {
					require.Nil(t, converted)
				} else {
					require.Equal(t, *tc.expected, *converted)
				}
			}
		})
	}
}

func TestMakeJobState(t *testing.T) {
	ptr := func(state rpc_proxy.EJobState) *rpc_proxy.EJobState {
		return &state
	}

	for _, tc := range []struct {
		name     string
		state    *rpc_proxy.EJobState
		expected yt.JobState
		err      bool
	}{
		{name: "nil", state: nil, err: true},
		{name: "Running", state: ptr(rpc_proxy.EJobState_JS_RUNNING), expected: yt.JobRunning},
		{name: "Pending", state: ptr(rpc_proxy.EJobState_JS_WAITING), expected: yt.JobWaiting},
		{name: "Completed", state: ptr(rpc_proxy.EJobState_JS_COMPLETED), expected: yt.JobCompleted},
		{name: "Failed", state: ptr(rpc_proxy.EJobState_JS_FAILED), expected: yt.JobFailed},
		{name: "Aborted", state: ptr(rpc_proxy.EJobState_JS_ABORTED), expected: yt.JobAborted},
		{name: "unexpected", state: ptr(rpc_proxy.EJobState(31337)), err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			converted, err := makeJobState(tc.state)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, converted)
			}
		})
	}
}

func TestConvertJobSortOrder(t *testing.T) {
	ptr := func(d rpc_proxy.EJobSortDirection) *rpc_proxy.EJobSortDirection {
		return &d
	}

	unexpectedJobSortOrder := yt.JobSortOrder("as-you-want")

	for _, tc := range []struct {
		name      string
		sortOrder *yt.JobSortOrder
		expected  *rpc_proxy.EJobSortDirection
		err       bool
	}{
		{name: "nil", sortOrder: nil, expected: nil},
		{name: "Ascending", sortOrder: &yt.Ascending, expected: ptr(rpc_proxy.EJobSortDirection_JSD_ASCENDING)},
		{name: "Descending", sortOrder: &yt.Descending, expected: ptr(rpc_proxy.EJobSortDirection_JSD_DESCENDING)},
		{name: "unexpected", sortOrder: &unexpectedJobSortOrder, err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			converted, err := convertJobSortOrder(tc.sortOrder)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tc.expected == nil {
					require.Nil(t, converted)
				} else {
					require.Equal(t, *tc.expected, *converted)
				}
			}
		})
	}
}

func TestConvertDataSource(t *testing.T) {
	ptr := func(s rpc_proxy.EDataSource) *rpc_proxy.EDataSource {
		return &s
	}

	unexpectedDataSource := yt.JobDataSource("nowhere")

	for _, tc := range []struct {
		name       string
		dataSource *yt.JobDataSource
		expected   *rpc_proxy.EDataSource
		err        bool
	}{
		{name: "nil", dataSource: nil, expected: nil},
		{name: "Archive", dataSource: &yt.JobDataSourceArchive, expected: ptr(rpc_proxy.EDataSource_DS_ARCHIVE)},
		{name: "Runtime", dataSource: &yt.JobDataSourceRuntime, expected: ptr(rpc_proxy.EDataSource_DS_RUNTIME)},
		{name: "Auto", dataSource: &yt.JobDataSourceAuto, expected: ptr(rpc_proxy.EDataSource_DS_AUTO)},
		{name: "Manual", dataSource: &yt.JobDataSourceManual, expected: ptr(rpc_proxy.EDataSource_DS_MANUAL)},
		{name: "unexpected", dataSource: &unexpectedDataSource, err: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			converted, err := convertDataSource(tc.dataSource)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tc.expected == nil {
					require.Nil(t, converted)
				} else {
					require.Equal(t, *tc.expected, *converted)
				}
			}
		})
	}
}
