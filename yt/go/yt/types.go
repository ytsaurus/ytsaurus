package yt

import (
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
)

type NodeType string

const (
	// NodeMap is cypress analog for directory.
	NodeMap NodeType = "map_node"
	// NodeLink is symbolic link.
	NodeLink NodeType = "link"
	// NodeFile is regular file. Used for artifacts and opaque blobs.
	NodeFile NodeType = "file"
	// NodeTable is table.
	NodeTable NodeType = "table"

	NodeString  NodeType = "string_node"
	NodeBoolean NodeType = "boolean_node"

	NodeDocument          NodeType = "document"
	NodeTableReplica      NodeType = "table_replica"
	NodeReplicatedTable   NodeType = "replicated_table"
	NodeUser              NodeType = "user"
	NodeGroup             NodeType = "group"
	NodeAccount           NodeType = "account"
	NodeDomesticMedium    NodeType = "domestic_medium"
	NodeTabletCellBundle  NodeType = "tablet_cell_bundle"
	NodeTabletCell        NodeType = "tablet_cell"
	NodeSys               NodeType = "sys_node"
	NodePortalEntrance    NodeType = "portal_entrance"
	NodePortalExit        NodeType = "portal_exit"
	NodeSchedulerPool     NodeType = "scheduler_pool"
	NodeSchedulerPoolTree NodeType = "scheduler_pool_tree"

	NodeAccessControlObject          NodeType = "access_control_object"
	NodeAccessControlObjectNamespace NodeType = "access_control_object_namespace"

	NodeNetworkProject    NodeType = "network_project"
	NodeNetworkProjectMap NodeType = "network_project_map"

	NodeRack       NodeType = "rack"
	NodeDataCenter NodeType = "data_center"
)

func (n NodeType) String() string {
	return string(n)
}

type OperationType string

var (
	OperationMap        OperationType = "map"
	OperationReduce     OperationType = "reduce"
	OperationMapReduce  OperationType = "map_reduce"
	OperationSort       OperationType = "sort"
	OperationMerge      OperationType = "merge"
	OperationErase      OperationType = "erase"
	OperationRemoteCopy OperationType = "remote_copy"
	OperationVanilla    OperationType = "vanilla"
)

type OperationState string

var (
	StateRunning       OperationState = "running"
	StatePending       OperationState = "pending"
	StateCompleted     OperationState = "completed"
	StateFailed        OperationState = "failed"
	StateAborted       OperationState = "aborted"
	StateReviving      OperationState = "reviving"
	StateInitializing  OperationState = "initializing"
	StatePreparing     OperationState = "preparing"
	StateMaterializing OperationState = "materializing"
	StateCompleting    OperationState = "completing"
	StateAborting      OperationState = "aborting"
	StateFailing       OperationState = "failing"
)

func (o OperationState) IsFinished() bool {
	switch o {
	case StateCompleted, StateFailed, StateAborted:
		return true
	}

	return false
}

type JobType string

var (
	JobTypeMap              JobType = "map"
	JobTypePartitionMap     JobType = "partition_map"
	JobTypeSortedMerge      JobType = "sorted_merge"
	JobTypeOrderedMerge     JobType = "ordered_merge"
	JobTypeUnorderedMerge   JobType = "unordered_merge"
	JobTypePartition        JobType = "partition"
	JobTypeSimpleSort       JobType = "simple_sort"
	JobTypeFinalSort        JobType = "final_sort"
	JobTypeSortedReduce     JobType = "sorted_reduce"
	JobTypePartitionReduce  JobType = "partition_reduce"
	JobTypeReduceCombiner   JobType = "reduce_combiner"
	JobTypeRemoteCopy       JobType = "remote_copy"
	JobTypeIntermediateSort JobType = "intermediate_sort"
	JobTypeOrderedMap       JobType = "ordered_map"
	JobTypeJoinReduce       JobType = "join_reduce"
	JobTypeVanilla          JobType = "vanilla"
	JobTypeSchedulerUnknown JobType = "scheduler_unknown"
)

type JobState string

var (
	JobRunning   JobState = "running"
	JobWaiting   JobState = "waiting"
	JobCompleted JobState = "completed"
	JobFailed    JobState = "failed"
	JobAborted   JobState = "aborted"
)

type JobDataSource string

var (
	JobDataSourceArchive JobDataSource = "archive"
	JobDataSourceRuntime JobDataSource = "runtime"
	JobDataSourceAuto    JobDataSource = "auto"
	JobDataSourceManual  JobDataSource = "manual"
)

type NodeID guid.GUID

func (id NodeID) String() string {
	return guid.GUID(id).String()
}

func (id NodeID) MarshalYSON(w *yson.Writer) error {
	return guid.GUID(id).MarshalYSON(w)
}

func (id *NodeID) UnmarshalYSON(data []byte) (err error) {
	var g guid.GUID
	err = g.UnmarshalYSON(data)
	*id = NodeID(g)
	return
}

func (id NodeID) MarshalText() ([]byte, error) {
	return guid.GUID(id).MarshalText()
}

func (id *NodeID) UnmarshalText(data []byte) (err error) {
	return (*guid.GUID)(id).UnmarshalText(data)
}

func (id NodeID) YPath() ypath.Path {
	return ypath.Path("#" + id.String())
}

type Revision uint64

type OperationID guid.GUID

var NullOperationID = OperationID(guid.FromHalves(0, 0))

func (id OperationID) String() string {
	return guid.GUID(id).String()
}

func (id OperationID) MarshalYSON(w *yson.Writer) error {
	return guid.GUID(id).MarshalYSON(w)
}

func (id *OperationID) UnmarshalYSON(data []byte) (err error) {
	var g guid.GUID
	err = g.UnmarshalYSON(data)
	*id = OperationID(g)
	return
}

func (id OperationID) MarshalText() ([]byte, error) {
	return guid.GUID(id).MarshalText()
}

func (id *OperationID) UnmarshalText(data []byte) error {
	return (*guid.GUID)(id).UnmarshalText(data)
}

type TxID guid.GUID

func (id TxID) String() string {
	return guid.GUID(id).String()
}

func (id TxID) MarshalYSON(w *yson.Writer) error {
	return guid.GUID(id).MarshalYSON(w)
}

func (id *TxID) UnmarshalYSON(data []byte) (err error) {
	var g guid.GUID
	err = g.UnmarshalYSON(data)
	*id = TxID(g)
	return
}

type MutationID guid.GUID

func (id MutationID) String() string {
	return guid.GUID(id).String()
}

func (id MutationID) MarshalYSON(w *yson.Writer) error {
	return guid.GUID(id).MarshalYSON(w)
}

func (id *MutationID) UnmarshalYSON(data []byte) (err error) {
	var g guid.GUID
	err = g.UnmarshalYSON(data)
	*id = MutationID(g)
	return
}

type JobID guid.GUID

func (id JobID) String() string {
	return guid.GUID(id).String()
}

func (id JobID) MarshalYSON(w *yson.Writer) error {
	return guid.GUID(id).MarshalYSON(w)
}

func (id *JobID) UnmarshalYSON(data []byte) (err error) {
	var g guid.GUID
	err = g.UnmarshalYSON(data)
	*id = JobID(g)
	return
}

type LockMode string

const (
	LockSnapshot  LockMode = "snapshot"
	LockShared    LockMode = "shared"
	LockExclusive LockMode = "exclusive"
)

// LockState type holds available lock states.
type LockState string

const (
	// LockPending is a state of a queued waitable lock.
	LockPending LockState = "pending"
	// LockAcquired is a state of an acquired lock.
	LockAcquired LockState = "acquired"
)

type JobSortField string

var (
	SortFieldNone       JobSortField = "none"
	SortFieldType       JobSortField = "type"
	SortFieldState      JobSortField = "state"
	SortFieldStartTime  JobSortField = "start_time"
	SortFieldFinishTime JobSortField = "finish_time"
	SortFieldAddress    JobSortField = "address"
	SortFieldDuration   JobSortField = "duration"
	SortFieldProgress   JobSortField = "progress"
	SortFieldID         JobSortField = "id"
)

type JobSortOrder string

var (
	Ascending  JobSortOrder = "ascending"
	Descending JobSortOrder = "descending"
)

type TableReplicaMode string

var (
	SyncMode  TableReplicaMode = "sync"
	AsyncMode TableReplicaMode = "async"
)

type LockType string

const (
	LockTypeNone         LockType = "none"
	LockTypeSharedWeak   LockType = "shared_weak"
	LockTypeSharedStrong LockType = "shared_strong"
	LockTypeExclusive    LockType = "exclusive"
)

// Timestamp is a cluster-wide unique monotonically increasing number used to implement the MVCC.
type Timestamp uint64

type TxType string

var (
	TxTypeMaster TxType = "master"
	TxTypeTablet TxType = "tablet"
)

type Atomicity string

var (
	AtomicityNone Atomicity = "none"
	AtomicityFull Atomicity = "full"
)

type MaintenanceType string

const (
	MaintenanceTypeBan                  MaintenanceType = "ban"
	MaintenanceTypeDecommission         MaintenanceType = "decommission"
	MaintenanceTypeDisableSchedulerJobs MaintenanceType = "disable_scheduler_jobs"
	MaintenanceTypeDisableWriteSessions MaintenanceType = "disable_write_sessions"
	MaintenanceTypeDisableTabletCells   MaintenanceType = "disable_tablet_cells"
	MaintenanceTypePendingRestart       MaintenanceType = "pending_restart"
)

type MaintenanceComponent string

const (
	MaintenanceComponentClusterNode MaintenanceComponent = "cluster_node"
	MaintenanceComponentHTTPProxy   MaintenanceComponent = "http_proxy"
	MaintenanceComponentRPCProxy    MaintenanceComponent = "rpc_proxy"
	MaintenanceComponentHost        MaintenanceComponent = "host"
)

type MaintenanceID guid.GUID

func (id MaintenanceID) String() string {
	return guid.GUID(id).String()
}

func (id MaintenanceID) MarshalYSON(w *yson.Writer) error {
	return guid.GUID(id).MarshalYSON(w)
}

func (id *MaintenanceID) UnmarshalYSON(data []byte) (err error) {
	var g guid.GUID
	err = g.UnmarshalYSON(data)
	*id = MaintenanceID(g)
	return
}

type OrderedTableBackupMode string

const (
	OrderedTableBackupModeExact   OrderedTableBackupMode = "exact"
	OrderedTableBackupModeAtLeast OrderedTableBackupMode = "at_least"
	OrderedTableBackupModeAtMost  OrderedTableBackupMode = "at_most"
)

type TableBackupManifest struct {
	SourcePath      ypath.Path             `yson:"source_path"`
	DestinationPath ypath.Path             `yson:"destination_path"`
	OrderedMode     OrderedTableBackupMode `yson:"ordered_mode,omitempty"`
}

type BackupManifest struct {
	Clusters map[string][]TableBackupManifest `yson:"clusters"`
}

type QueryEngine string

const (
	QueryEngineQL   QueryEngine = "ql"
	QueryEngineYQL  QueryEngine = "yql"
	QueryEngineCHYT QueryEngine = "chyt"
	QueryEngineMock QueryEngine = "mock"
)

type QueryID guid.GUID

func (id QueryID) String() string {
	return guid.GUID(id).String()
}

func (id QueryID) MarshalYSON(w *yson.Writer) error {
	return guid.GUID(id).MarshalYSON(w)
}

func (id *QueryID) UnmarshalYSON(data []byte) (err error) {
	var g guid.GUID
	err = g.UnmarshalYSON(data)
	*id = QueryID(g)
	return
}

type QueryState string

const (
	QueryStateDraft      QueryState = "draft"
	QueryStatePending    QueryState = "pending"
	QueryStateRunning    QueryState = "running"
	QueryStateAborting   QueryState = "aborting"
	QueryStateAborted    QueryState = "aborted"
	QueryStateCompleting QueryState = "completing"
	QueryStateCompleted  QueryState = "completed"
	QueryStateFailing    QueryState = "failing"
	QueryStateFailed     QueryState = "failed"
)

type OperationSortDirection string

const (
	SortDirectionNone   OperationSortDirection = "none"
	SortDirectionPast   OperationSortDirection = "past"
	SortDirectionFuture OperationSortDirection = "future"
)
