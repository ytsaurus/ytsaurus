package rpcclient

import (
	"encoding/json"
	"net/http"
	"time"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
	"go.ytsaurus.tech/yt/go/proto/core/ytree"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

// unexpectedStatusCode is last effort attempt to get useful error message from a failed request.
func unexpectedStatusCode(rsp *http.Response) error {
	d := json.NewDecoder(rsp.Body)
	d.UseNumber()

	var ytErr yterrors.Error
	if err := d.Decode(&ytErr); err == nil {
		return &ytErr
	}

	return xerrors.Errorf("unexpected status code %d", rsp.StatusCode)
}

func convertTxID(txID yt.TxID) *misc.TGuid {
	return convertGUID(guid.GUID(txID))
}

func convertParentTxID(txID yt.TxID) *misc.TGuid {
	zero := yt.TxID{}
	if txID == zero {
		return nil
	}
	return convertTxID(txID)
}

func getTxID(opts *yt.TransactionOptions) *misc.TGuid {
	if opts == nil {
		return nil
	}
	return convertTxID(opts.TransactionID)
}

func convertGUID(g guid.GUID) *misc.TGuid {
	first, second := g.Halves()
	return &misc.TGuid{
		First:  &first,
		Second: &second,
	}
}

func convertGUIDPtr(g *guid.GUID) *misc.TGuid {
	if g == nil {
		return nil
	}
	return convertGUID(*g)
}

func convertGUIDs(guids []guid.GUID) []*misc.TGuid {
	ret := make([]*misc.TGuid, 0, len(guids))
	for _, g := range guids {
		ret = append(ret, convertGUID(g))
	}
	return ret
}

func convertMaintenanceID(id yt.MaintenanceID) *misc.TGuid {
	guid := guid.GUID(id)
	return convertGUID(guid)
}

func convertMaintenanceIDs(ids []yt.MaintenanceID) []*misc.TGuid {
	ret := make([]*misc.TGuid, 0, len(ids))
	for _, id := range ids {
		ret = append(ret, convertMaintenanceID(id))
	}
	return ret
}

func makeGUID(g *misc.TGuid) guid.GUID {
	return guid.FromHalves(g.GetFirst(), g.GetSecond())
}

func makeGUIDs(guids []*misc.TGuid) []guid.GUID {
	ret := make([]guid.GUID, 0, len(guids))
	for _, g := range guids {
		ret = append(ret, makeGUID(g))
	}
	return ret
}

func makeNodeID(g *misc.TGuid) yt.NodeID {
	return yt.NodeID(makeGUID(g))
}

func convertAttributes(attrs map[string]any) (*ytree.TAttributeDictionary, error) {
	if attrs == nil {
		return nil, nil
	}

	ret := &ytree.TAttributeDictionary{
		Attributes: make([]*ytree.TAttribute, 0, len(attrs)),
	}

	for key, value := range attrs {
		valueBytes, err := yson.Marshal(value)
		if err != nil {
			return nil, err
		}

		ret.Attributes = append(ret.Attributes, &ytree.TAttribute{
			Key:   ptr.String(key),
			Value: valueBytes,
		})
	}

	return ret, nil
}

func convertLegacyAttributeKeys(keys []string) *rpc_proxy.TLegacyAttributeKeys {
	if keys == nil {
		return &rpc_proxy.TLegacyAttributeKeys{All: ptr.Bool(true)}
	}

	return &rpc_proxy.TLegacyAttributeKeys{Keys: keys}
}

func convertAttributeFilter(keys []string) *ytree.TAttributeFilter {
	if keys == nil {
		return nil
	}

	return &ytree.TAttributeFilter{Keys: keys}
}

func convertTransactionOptions(opts *yt.TransactionOptions) *rpc_proxy.TTransactionalOptions {
	if opts == nil {
		return nil
	}

	return &rpc_proxy.TTransactionalOptions{
		TransactionId:                      convertTxID(opts.TransactionID),
		Ping:                               &opts.Ping,
		PingAncestors:                      &opts.PingAncestors,
		SuppressTransactionCoordinatorSync: &opts.SuppressTransactionCoordinatorSync,
		SuppressUpstreamSync:               &opts.SuppressUpstreamSync,
	}
}

func convertPrerequisiteOptions(opts *yt.PrerequisiteOptions) *rpc_proxy.TPrerequisiteOptions {
	if opts == nil {
		return nil
	}

	txIDs := make([]*rpc_proxy.TPrerequisiteOptions_TTransactionPrerequisite, 0, len(opts.TransactionIDs))
	for _, id := range opts.TransactionIDs {
		txIDs = append(txIDs, &rpc_proxy.TPrerequisiteOptions_TTransactionPrerequisite{
			TransactionId: convertTxID(id),
		})
	}

	revisions := make([]*rpc_proxy.TPrerequisiteOptions_TRevisionPrerequisite, 0, len(opts.Revisions))
	for _, rev := range opts.Revisions {
		revisions = append(revisions, &rpc_proxy.TPrerequisiteOptions_TRevisionPrerequisite{
			Path:     ptr.String(rev.Path.String()),
			Revision: ptr.Uint64(uint64(rev.Revision)),
		})
	}

	return &rpc_proxy.TPrerequisiteOptions{
		Transactions: txIDs,
		Revisions:    revisions,
	}
}

func convertPrerequisiteTxIDs(ids []yt.TxID) []*misc.TGuid {
	if ids == nil {
		return nil
	}

	txIDs := make([]*misc.TGuid, 0, len(ids))
	for _, id := range ids {
		txIDs = append(txIDs, convertTxID(id))
	}

	return txIDs
}

func convertMasterReadOptions(opts *yt.MasterReadOptions) *rpc_proxy.TMasterReadOptions {
	if opts == nil {
		return nil
	}

	return &rpc_proxy.TMasterReadOptions{
		ReadFrom:                        convertReadKind(opts.ReadFrom),
		DisablePerUserCache:             opts.DisablePerUserCache,
		ExpireAfterSuccessfulUpdateTime: convertDuration(opts.ExpireAfterSuccessfulUpdateTime),
		ExpireAfterFailedUpdateTime:     convertDuration(opts.ExpireAfterFailedUpdateTime),
		CacheStickyGroupSize:            opts.CacheStickyGroupSize,
		SuccessStalenessBound:           convertDuration(opts.SuccessStalenessBound),
	}
}

func convertReadKind(k yt.ReadKind) *rpc_proxy.EMasterReadKind {
	var ret rpc_proxy.EMasterReadKind

	switch k {
	case yt.ReadFromLeader:
		ret = rpc_proxy.EMasterReadKind_MRK_LEADER
	case yt.ReadFromFollower:
		ret = rpc_proxy.EMasterReadKind_MRK_FOLLOWER
	case yt.ReadFromCache:
		ret = rpc_proxy.EMasterReadKind_MRK_CACHE
	case yt.ReadFromMasterCache:
		ret = rpc_proxy.EMasterReadKind_MRK_MASTER_CACHE
	default:
		return nil
	}

	return &ret
}

func convertAccessTrackingOptions(opts *yt.AccessTrackingOptions) *rpc_proxy.TSuppressableAccessTrackingOptions {
	if opts == nil {
		return nil
	}

	return &rpc_proxy.TSuppressableAccessTrackingOptions{
		SuppressAccessTracking:       &opts.SuppressAccessTracking,
		SuppressModificationTracking: &opts.SuppressModificationTracking,
	}
}

func convertMutatingOptions(opts *yt.MutatingOptions) *rpc_proxy.TMutatingOptions {
	if opts == nil {
		return nil
	}

	retry := opts.Retry
	return &rpc_proxy.TMutatingOptions{
		MutationId: convertGUID(guid.GUID(opts.MutationID)),
		Retry:      &retry,
	}
}

func convertDuration(d *yson.Duration) *int64 {
	if d == nil {
		return nil
	}

	return ptr.Int64(int64(time.Duration(*d) / time.Microsecond))
}

func convertTime(t *yson.Time) *uint64 {
	if t == nil {
		return nil
	}

	return ptr.Uint64(uint64(time.Time(*t).UTC().UnixNano() / time.Hour.Microseconds()))
}

func makeTime(us *uint64) yson.Time {
	if us == nil {
		return yson.Time{}
	}

	return yson.Time(time.Unix(0, time.Microsecond.Nanoseconds()*int64(*us)).UTC())
}

func convertAtomicity(a *yt.Atomicity) (*rpc_proxy.EAtomicity, error) {
	if a == nil {
		return nil, nil
	}

	var ret rpc_proxy.EAtomicity

	switch *a {
	case yt.AtomicityNone:
		ret = rpc_proxy.EAtomicity_A_NONE
	case yt.AtomicityFull:
		ret = rpc_proxy.EAtomicity_A_FULL
	default:
		return nil, xerrors.Errorf("unexpected atomicity %q", *a)
	}

	return &ret, nil
}

func convertCheckPermissionColumns(columns []string) *rpc_proxy.TReqCheckPermission_TColumns {
	if columns == nil {
		return nil
	}
	return &rpc_proxy.TReqCheckPermission_TColumns{
		Items: columns,
	}
}

func convertMaintenanceComponent(c yt.MaintenanceComponent) (*rpc_proxy.EMaintenanceComponent, error) {
	var ret rpc_proxy.EMaintenanceComponent

	switch c {
	case yt.MaintenanceComponentClusterNode:
		ret = rpc_proxy.EMaintenanceComponent_MC_CLUSTER_NODE
	case yt.MaintenanceComponentHost:
		ret = rpc_proxy.EMaintenanceComponent_MC_HOST
	case yt.MaintenanceComponentHTTPProxy:
		ret = rpc_proxy.EMaintenanceComponent_MC_HTTP_PROXY
	case yt.MaintenanceComponentRPCProxy:
		ret = rpc_proxy.EMaintenanceComponent_MC_RPC_PROXY
	default:
		return nil, xerrors.Errorf("unexpected maintenance component %q", c)
	}

	return &ret, nil
}

func convertMaintenanceType(t yt.MaintenanceType) (*rpc_proxy.EMaintenanceType, error) {
	var ret rpc_proxy.EMaintenanceType

	switch t {
	case yt.MaintenanceTypeBan:
		ret = rpc_proxy.EMaintenanceType_MT_BAN
	case yt.MaintenanceTypeDecommission:
		ret = rpc_proxy.EMaintenanceType_MT_DECOMMISSION
	case yt.MaintenanceTypeDisableSchedulerJobs:
		ret = rpc_proxy.EMaintenanceType_MT_DISABLE_SCHEDULER_JOBS
	case yt.MaintenanceTypeDisableTabletCells:
		ret = rpc_proxy.EMaintenanceType_MT_DISABLE_TABLET_CELLS
	case yt.MaintenanceTypeDisableWriteSessions:
		ret = rpc_proxy.EMaintenanceType_MT_DISABLE_WRITE_SESSIONS
	case yt.MaintenanceTypePendingRestart:
		ret = rpc_proxy.EMaintenanceType_MT_PENDING_RESTART
	default:
		return nil, xerrors.Errorf("unexpected maintenance type %q", t)
	}

	return &ret, nil
}

func makeRemoveMaintenanceResponse(r *rpc_proxy.TRspRemoveMaintenance) (*yt.RemoveMaintenanceResponse, error) {
	if r == nil {
		return nil, xerrors.Errorf("unable to convert nil remove maintenance result")
	}

	ret := &yt.RemoveMaintenanceResponse{
		BanCounts:                  int(r.GetBan()),
		DecommissionCounts:         int(r.GetDecommission()),
		DisableSchedulerJobsCounts: int(r.GetDisableSchedulerJobs()),
		DisableWriteSessionsCounts: int(r.GetDisableWriteSessions()),
		DisableTabletCellsCounts:   int(r.GetDisableTabletCells()),
		PendingRestartCounts:       int(r.GetPendingRestart()),
	}

	return ret, nil
}

func makeSecurityActionType(typ rpc_proxy.ESecurityAction) (yt.SecurityAction, error) {
	var ret yt.SecurityAction

	switch typ {
	case rpc_proxy.ESecurityAction_SA_ALLOW:
		ret = yt.ActionAllow
	case rpc_proxy.ESecurityAction_SA_DENY:
		ret = yt.ActionDeny
	case rpc_proxy.ESecurityAction_SA_UNDEFINED:
		return "", xerrors.Errorf("unsupported security action type %q", typ)
	default:
		return "", xerrors.Errorf("unexpected security action type %q", typ)
	}

	return ret, nil
}

func makeCheckPermissionResult(result *rpc_proxy.TCheckPermissionResult) (yt.CheckPermissionResult, error) {
	if result == nil {
		return yt.CheckPermissionResult{}, xerrors.Errorf("unable to convert nil check permission result")
	}

	action, err := makeSecurityActionType(result.GetAction())
	if err != nil {
		return yt.CheckPermissionResult{}, err
	}

	ret := yt.CheckPermissionResult{
		Action:      action,
		ObjectID:    makeNodeID(result.ObjectId),
		ObjectName:  result.ObjectName,
		SubjectID:   makeNodeID(result.SubjectId),
		SubjectName: result.SubjectName,
	}

	return ret, nil
}

func makeCheckPermissionResponse(response *rpc_proxy.TRspCheckPermission) (*yt.CheckPermissionResponse, error) {
	if response == nil {
		return nil, nil
	}

	result, err := makeCheckPermissionResult(response.Result)
	if err != nil {
		return nil, err
	}

	var columns []yt.CheckPermissionResult

	if response.Columns != nil {
		columns = make([]yt.CheckPermissionResult, len(response.Columns.GetItems()))
		for index, item := range response.Columns.GetItems() {
			columns[index], err = makeCheckPermissionResult(item)
			if err != nil {
				return nil, err
			}
		}
	}

	ret := &yt.CheckPermissionResponse{
		CheckPermissionResult: result,
		Columns:               columns,
	}

	return ret, nil
}

func makeDisableChunkLocationsResponse(response *rpc_proxy.TRspDisableChunkLocations) (*yt.DisableChunkLocationsResponse, error) {
	if response == nil {
		return nil, nil
	}

	ret := &yt.DisableChunkLocationsResponse{
		LocationUUIDs: makeGUIDs(response.LocationUuids),
	}

	return ret, nil
}

func makeDestroyChunkLocationsResponse(response *rpc_proxy.TRspDestroyChunkLocations) (*yt.DestroyChunkLocationsResponse, error) {
	if response == nil {
		return nil, nil
	}

	ret := &yt.DestroyChunkLocationsResponse{
		LocationUUIDs: makeGUIDs(response.LocationUuids),
	}

	return ret, nil
}

func makeResurrectChunkLocationsResponse(response *rpc_proxy.TRspResurrectChunkLocations) (*yt.ResurrectChunkLocationsResponse, error) {
	if response == nil {
		return nil, nil
	}

	ret := &yt.ResurrectChunkLocationsResponse{
		LocationUUIDs: makeGUIDs(response.LocationUuids),
	}

	return ret, nil
}

func convertOperationType(typ *yt.OperationType) (*rpc_proxy.EOperationType, error) {
	if typ == nil {
		return nil, nil
	}

	var ret rpc_proxy.EOperationType

	switch *typ {
	case yt.OperationMap:
		ret = rpc_proxy.EOperationType_OT_MAP
	case yt.OperationReduce:
		ret = rpc_proxy.EOperationType_OT_REDUCE
	case yt.OperationMapReduce:
		ret = rpc_proxy.EOperationType_OT_MAP_REDUCE
	case yt.OperationSort:
		ret = rpc_proxy.EOperationType_OT_SORT
	case yt.OperationMerge:
		ret = rpc_proxy.EOperationType_OT_MERGE
	case yt.OperationErase:
		ret = rpc_proxy.EOperationType_OT_ERASE
	case yt.OperationRemoteCopy:
		ret = rpc_proxy.EOperationType_OT_REMOTE_COPY
	case yt.OperationVanilla:
		ret = rpc_proxy.EOperationType_OT_VANILLA
	default:
		return nil, xerrors.Errorf("unexpected operation type %q", *typ)
	}

	return &ret, nil
}

func makeOperationType(typ *rpc_proxy.EOperationType) (yt.OperationType, error) {
	if typ == nil {
		return "", xerrors.Errorf("unable to convert nil operation type")
	}

	var ret yt.OperationType

	switch *typ {
	case rpc_proxy.EOperationType_OT_MAP:
		ret = yt.OperationMap
	case rpc_proxy.EOperationType_OT_MERGE:
		ret = yt.OperationMerge
	case rpc_proxy.EOperationType_OT_ERASE:
		ret = yt.OperationErase
	case rpc_proxy.EOperationType_OT_SORT:
		ret = yt.OperationSort
	case rpc_proxy.EOperationType_OT_REDUCE:
		ret = yt.OperationReduce
	case rpc_proxy.EOperationType_OT_MAP_REDUCE:
		ret = yt.OperationMapReduce
	case rpc_proxy.EOperationType_OT_REMOTE_COPY:
		ret = yt.OperationRemoteCopy
	case rpc_proxy.EOperationType_OT_VANILLA:
		ret = yt.OperationVanilla
	case rpc_proxy.EOperationType_OT_JOIN_REDUCE:
		return "", xerrors.Errorf("unsupported operation type %q", *typ)
	default:
		return "", xerrors.Errorf("unexpected operation type %q", *typ)
	}

	return ret, nil
}

func convertOperationState(state *yt.OperationState) (*rpc_proxy.EOperationState, error) {
	if state == nil {
		return nil, nil
	}

	var ret rpc_proxy.EOperationState

	switch *state {
	case yt.StateRunning:
		ret = rpc_proxy.EOperationState_OS_RUNNING
	case yt.StatePending:
		ret = rpc_proxy.EOperationState_OS_PENDING
	case yt.StateCompleted:
		ret = rpc_proxy.EOperationState_OS_COMPLETED
	case yt.StateFailed:
		ret = rpc_proxy.EOperationState_OS_FAILED
	case yt.StateAborted:
		ret = rpc_proxy.EOperationState_OS_ABORTED
	case yt.StateReviving:
		ret = rpc_proxy.EOperationState_OS_REVIVING
	case yt.StateInitializing:
		ret = rpc_proxy.EOperationState_OS_INITIALIZING
	case yt.StatePreparing:
		ret = rpc_proxy.EOperationState_OS_PREPARING
	case yt.StateMaterializing:
		ret = rpc_proxy.EOperationState_OS_MATERIALIZING
	case yt.StateCompleting:
		ret = rpc_proxy.EOperationState_OS_COMPLETING
	case yt.StateAborting:
		ret = rpc_proxy.EOperationState_OS_ABORTING
	case yt.StateFailing:
		ret = rpc_proxy.EOperationState_OS_FAILING
	default:
		return nil, xerrors.Errorf("unexpected operation state %q", *state)
	}

	return &ret, nil
}

func makeOperationState(state *rpc_proxy.EOperationState) (yt.OperationState, error) {
	if state == nil {
		return "", xerrors.Errorf("unable to convert nil operation state")
	}

	var ret yt.OperationState

	switch *state {
	case rpc_proxy.EOperationState_OS_INITIALIZING:
		ret = yt.StateInitializing
	case rpc_proxy.EOperationState_OS_PREPARING:
		ret = yt.StatePreparing
	case rpc_proxy.EOperationState_OS_MATERIALIZING:
		ret = yt.StateMaterializing
	case rpc_proxy.EOperationState_OS_REVIVING:
		ret = yt.StateReviving
	case rpc_proxy.EOperationState_OS_PENDING:
		ret = yt.StatePending
	case rpc_proxy.EOperationState_OS_RUNNING:
		ret = yt.StateRunning
	case rpc_proxy.EOperationState_OS_COMPLETING:
		ret = yt.StateCompleting
	case rpc_proxy.EOperationState_OS_COMPLETED:
		ret = yt.StateCompleted
	case rpc_proxy.EOperationState_OS_ABORTING:
		ret = yt.StateAborting
	case rpc_proxy.EOperationState_OS_ABORTED:
		ret = yt.StateAborted
	case rpc_proxy.EOperationState_OS_FAILING:
		ret = yt.StateFailing
	case rpc_proxy.EOperationState_OS_FAILED:
		ret = yt.StateFailed
	case rpc_proxy.EOperationState_OS_NONE,
		rpc_proxy.EOperationState_OS_STARTING,
		rpc_proxy.EOperationState_OS_ORPHANED,
		rpc_proxy.EOperationState_OS_WAITING_FOR_AGENT,
		rpc_proxy.EOperationState_OS_REVIVING_JOBS:
		return "", xerrors.Errorf("unsupported operation state %q", *state)
	default:
		return "", xerrors.Errorf("unexpected operation state %q", *state)
	}

	return ret, nil
}

func makeOperationID(g *misc.TGuid) yt.OperationID {
	return yt.OperationID(guid.FromHalves(g.GetFirst(), g.GetSecond()))
}

func makeListOperationsResult(result *rpc_proxy.TListOperationsResult) (*yt.ListOperationsResult, error) {
	if result == nil {
		return nil, nil
	}

	operations := make([]yt.OperationStatus, 0, len(result.GetOperations()))
	for _, op := range result.GetOperations() {
		opID := makeOperationID(op.Id)

		opState, err := makeOperationState(op.State)
		if err != nil {
			return nil, xerrors.Errorf("unable to convert operation state of operation %q: %w", opID, err)
		}

		opType, err := makeOperationType(op.Type)
		if err != nil {
			return nil, xerrors.Errorf("unable to convert operation type of operation %q: %w", opID, err)
		}

		var briefSpec map[string]any
		if err := yson.Unmarshal(op.BriefSpec, &briefSpec); err != nil {
			return nil, xerrors.Errorf("unable to deserialize brief spec of operation %q: %w", opID, err)
		}

		var runtimeParameters yt.OperationRuntimeParameters
		if err := yson.Unmarshal(op.RuntimeParameters, &runtimeParameters); err != nil {
			return nil, xerrors.Errorf("unable to deserialize runtime parameters of operation %q: %w", opID, err)
		}

		var opResult yt.OperationResult

		if op.Result != nil {
			if err := yson.Unmarshal(op.Result, &opResult); err != nil {
				return nil, xerrors.Errorf("unable to deserialize result of operation %q: %w", opID, err)
			}
		}

		operations = append(operations, yt.OperationStatus{
			ID:                opID,
			State:             opState,
			Result:            &opResult,
			Type:              opType,
			BriefSpec:         briefSpec,
			FullSpec:          op.GetFullSpec(),
			StartTime:         makeTime(op.StartTime),
			Suspend:           op.GetSuspended(),
			AuthenticatedUser: op.GetAuthenticatedUser(),
			RuntimeParameters: runtimeParameters,
		})
	}

	poolCounts, err := makePoolCounts(result.GetPoolCounts())
	if err != nil {
		return nil, xerrors.Errorf("unable to convert pool counts: %w", err)
	}

	userCounts, err := makeUserCounts(result.GetUserCounts())
	if err != nil {
		return nil, xerrors.Errorf("unable to convert user counts: %w", err)
	}

	stateCounts, err := makeStateCounts(result.GetStateCounts())
	if err != nil {
		return nil, xerrors.Errorf("unable to convert state counts: %w", err)
	}

	typeCounts, err := makeTypeCounts(result.GetTypeCounts())
	if err != nil {
		return nil, xerrors.Errorf("unable to convert type counts: %w", err)
	}

	ret := &yt.ListOperationsResult{
		Operations:      operations,
		Incomplete:      result.GetIncomplete(),
		PoolCounts:      poolCounts,
		UserCounts:      userCounts,
		StateCounts:     stateCounts,
		TypeCounts:      typeCounts,
		FailedJobsCount: int(result.GetFailedJobsCount()),
	}

	return ret, nil
}

func makePoolCounts(counts *rpc_proxy.TListOperationsResult_TPoolCounts) (map[string]int, error) {
	if counts == nil {
		return nil, nil
	}

	ret := make(map[string]int)
	for _, entry := range counts.GetEntries() {
		if entry.Pool == nil {
			return nil, xerrors.Errorf("missing pool in pool counts entry")
		}
		ret[entry.GetPool()] = int(entry.GetCount())
	}

	return ret, nil
}

func makeUserCounts(counts *rpc_proxy.TListOperationsResult_TUserCounts) (map[string]int, error) {
	if counts == nil {
		return nil, nil
	}

	ret := make(map[string]int)
	for _, entry := range counts.GetEntries() {
		if entry.User == nil {
			return nil, xerrors.Errorf("missing user in user counts entry")
		}
		ret[entry.GetUser()] = int(entry.GetCount())
	}

	return ret, nil
}

func makeStateCounts(counts *rpc_proxy.TListOperationsResult_TOperationStateCounts) (map[string]int, error) {
	if counts == nil {
		return nil, nil
	}

	ret := make(map[string]int)
	for _, entry := range counts.GetEntries() {
		opState, err := makeOperationState(entry.State)
		if err != nil {
			return nil, xerrors.Errorf("unable to convert operation state: %w", err)
		}
		ret[string(opState)] = int(entry.GetCount())
	}

	return ret, nil
}

func makeTypeCounts(counts *rpc_proxy.TListOperationsResult_TOperationTypeCounts) (map[string]int, error) {
	if counts == nil {
		return nil, nil
	}

	ret := make(map[string]int)
	for _, entry := range counts.GetEntries() {
		opType, err := makeOperationType(entry.Type)
		if err != nil {
			return nil, xerrors.Errorf("unable to convert operation type: %w", err)
		}
		ret[string(opType)] = int(entry.GetCount())
	}

	return ret, nil
}

func convertJobType(typ *yt.JobType) (*rpc_proxy.EJobType, error) {
	if typ == nil {
		return nil, nil
	}

	var ret rpc_proxy.EJobType

	switch *typ {
	case yt.JobTypeMap:
		ret = rpc_proxy.EJobType_JT_MAP
	case yt.JobTypePartitionMap:
		ret = rpc_proxy.EJobType_JT_PARTITION_MAP
	case yt.JobTypeSortedMerge:
		ret = rpc_proxy.EJobType_JT_SORTED_MERGE
	case yt.JobTypeOrderedMerge:
		ret = rpc_proxy.EJobType_JT_ORDERED_MERGE
	case yt.JobTypeUnorderedMerge:
		ret = rpc_proxy.EJobType_JT_UNORDERED_MERGE
	case yt.JobTypePartition:
		ret = rpc_proxy.EJobType_JT_PARTITION
	case yt.JobTypeSimpleSort:
		ret = rpc_proxy.EJobType_JT_SIMPLE_SORT
	case yt.JobTypeFinalSort:
		ret = rpc_proxy.EJobType_JT_FINAL_SORT
	case yt.JobTypeSortedReduce:
		ret = rpc_proxy.EJobType_JT_SORTED_REDUCE
	case yt.JobTypePartitionReduce:
		ret = rpc_proxy.EJobType_JT_PARTITION_REDUCE
	case yt.JobTypeReduceCombiner:
		ret = rpc_proxy.EJobType_JT_REDUCE_COMBINER
	case yt.JobTypeRemoteCopy:
		ret = rpc_proxy.EJobType_JT_REMOTE_COPY
	case yt.JobTypeIntermediateSort:
		ret = rpc_proxy.EJobType_JT_INTERMEDIATE_SORT
	case yt.JobTypeOrderedMap:
		ret = rpc_proxy.EJobType_JT_ORDERED_MAP
	case yt.JobTypeJoinReduce:
		ret = rpc_proxy.EJobType_JT_JOIN_REDUCE
	case yt.JobTypeVanilla:
		ret = rpc_proxy.EJobType_JT_VANILLA
	case yt.JobTypeSchedulerUnknown:
		ret = rpc_proxy.EJobType_JT_SCHEDULER_UNKNOWN
	default:
		return nil, xerrors.Errorf("unexpected job type %q", *typ)
	}

	return &ret, nil
}

func makeJobType(typ *rpc_proxy.EJobType) (yt.JobType, error) {
	if typ == nil {
		return "", xerrors.Errorf("unable to convert nil job type")
	}

	var ret yt.JobType

	switch *typ {
	case rpc_proxy.EJobType_JT_MAP:
		ret = yt.JobTypeMap
	case rpc_proxy.EJobType_JT_PARTITION_MAP:
		ret = yt.JobTypePartitionMap
	case rpc_proxy.EJobType_JT_SORTED_MERGE:
		ret = yt.JobTypeSortedMerge
	case rpc_proxy.EJobType_JT_ORDERED_MERGE:
		ret = yt.JobTypeOrderedMerge
	case rpc_proxy.EJobType_JT_UNORDERED_MERGE:
		ret = yt.JobTypeUnorderedMerge
	case rpc_proxy.EJobType_JT_PARTITION:
		ret = yt.JobTypePartition
	case rpc_proxy.EJobType_JT_SIMPLE_SORT:
		ret = yt.JobTypeSimpleSort
	case rpc_proxy.EJobType_JT_FINAL_SORT:
		ret = yt.JobTypeFinalSort
	case rpc_proxy.EJobType_JT_SORTED_REDUCE:
		ret = yt.JobTypeSortedReduce
	case rpc_proxy.EJobType_JT_PARTITION_REDUCE:
		ret = yt.JobTypePartitionReduce
	case rpc_proxy.EJobType_JT_REDUCE_COMBINER:
		ret = yt.JobTypeReduceCombiner
	case rpc_proxy.EJobType_JT_REMOTE_COPY:
		ret = yt.JobTypeRemoteCopy
	case rpc_proxy.EJobType_JT_INTERMEDIATE_SORT:
		ret = yt.JobTypeIntermediateSort
	case rpc_proxy.EJobType_JT_ORDERED_MAP:
		ret = yt.JobTypeOrderedMap
	case rpc_proxy.EJobType_JT_JOIN_REDUCE:
		ret = yt.JobTypeJoinReduce
	case rpc_proxy.EJobType_JT_VANILLA:
		ret = yt.JobTypeVanilla
	case rpc_proxy.EJobType_JT_SCHEDULER_UNKNOWN:
		ret = yt.JobTypeSchedulerUnknown
	default:
		return "", xerrors.Errorf("unexpected job type %q", *typ)
	}

	return ret, nil
}

func convertJobState(state *yt.JobState) (*rpc_proxy.EJobState, error) {
	if state == nil {
		return nil, nil
	}

	var ret rpc_proxy.EJobState

	switch *state {
	case yt.JobRunning:
		ret = rpc_proxy.EJobState_JS_RUNNING
	case yt.JobWaiting:
		ret = rpc_proxy.EJobState_JS_WAITING
	case yt.JobCompleted:
		ret = rpc_proxy.EJobState_JS_COMPLETED
	case yt.JobFailed:
		ret = rpc_proxy.EJobState_JS_FAILED
	case yt.JobAborted:
		ret = rpc_proxy.EJobState_JS_ABORTED
	default:
		return nil, xerrors.Errorf("unexpected job state %q", *state)
	}

	return &ret, nil
}

func makeJobState(state *rpc_proxy.EJobState) (yt.JobState, error) {
	if state == nil {
		return "", xerrors.Errorf("unable to convert nil job state")
	}

	var ret yt.JobState

	switch *state {
	case rpc_proxy.EJobState_JS_RUNNING:
		ret = yt.JobRunning
	case rpc_proxy.EJobState_JS_WAITING:
		ret = yt.JobWaiting
	case rpc_proxy.EJobState_JS_COMPLETED:
		ret = yt.JobCompleted
	case rpc_proxy.EJobState_JS_FAILED:
		ret = yt.JobFailed
	case rpc_proxy.EJobState_JS_ABORTED:
		ret = yt.JobAborted
	default:
		return "", xerrors.Errorf("unexpected job state %q", *state)
	}

	return ret, nil
}

func convertJobSortOrder(o *yt.JobSortOrder) (*rpc_proxy.EJobSortDirection, error) {
	if o == nil {
		return nil, nil
	}

	var ret rpc_proxy.EJobSortDirection

	switch *o {
	case yt.Ascending:
		ret = rpc_proxy.EJobSortDirection_JSD_ASCENDING
	case yt.Descending:
		ret = rpc_proxy.EJobSortDirection_JSD_DESCENDING
	default:
		return nil, xerrors.Errorf("unexpected job sort order %q", *o)
	}

	return &ret, nil
}

func convertDataSource(dataSource *yt.JobDataSource) (*rpc_proxy.EDataSource, error) {
	if dataSource == nil {
		return nil, nil
	}

	var ret rpc_proxy.EDataSource

	switch *dataSource {
	case yt.JobDataSourceArchive:
		ret = rpc_proxy.EDataSource_DS_ARCHIVE
	case yt.JobDataSourceRuntime:
		ret = rpc_proxy.EDataSource_DS_RUNTIME
	case yt.JobDataSourceAuto:
		ret = rpc_proxy.EDataSource_DS_AUTO
	case yt.JobDataSourceManual:
		ret = rpc_proxy.EDataSource_DS_MANUAL
	default:
		return nil, xerrors.Errorf("unexpected data source %q", *dataSource)
	}

	return &ret, nil
}

func makeJobID(g *misc.TGuid) yt.JobID {
	return yt.JobID(guid.FromHalves(g.GetFirst(), g.GetSecond()))
}

func makeListJobsResult(result *rpc_proxy.TListJobsResult) (*yt.ListJobsResult, error) {
	if result == nil {
		return nil, nil
	}

	jobs := make([]yt.JobStatus, 0, len(result.GetJobs()))
	for _, job := range result.GetJobs() {
		jobID := makeJobID(job.Id)

		j := yt.JobStatus{
			ID:         jobID,
			StartTime:  makeTime(job.StartTime),
			FinishTime: makeTime(job.FinishTime),
		}

		jobType, err := makeJobType(job.Type)
		if err != nil {
			return nil, xerrors.Errorf("unable to deserialize job type of job %q: %w", jobID, err)
		}
		j.Type = string(jobType)

		jobState, err := makeJobState(job.State)
		if err != nil {
			return nil, xerrors.Errorf("unable to deserialize job state of job %q: %w", jobID, err)
		}
		j.State = string(jobState)

		if job.Address != nil {
			j.Address = *job.Address
		}

		if job.FailContextSize != nil {
			j.FailContextSize = int(*job.FailContextSize)
		}

		var jobError yterrors.Error
		if job.Error != nil {
			if err := json.Unmarshal(job.Error, &jobError); err != nil {
				return nil, xerrors.Errorf("unable to deserialize job error of job %q: %w", jobID, err)
			}
		}
		j.Error = jobError

		if job.Progress != nil {
			j.Progress = *job.Progress
		}

		jobs = append(jobs, j)
	}

	ret := &yt.ListJobsResult{
		Jobs: jobs,
	}

	return ret, nil
}

func convertTabletRangeOptions(opts *yt.TabletRangeOptions) *rpc_proxy.TTabletRangeOptions {
	if opts == nil {
		return nil
	}

	return &rpc_proxy.TTabletRangeOptions{
		FirstTabletIndex: ptr.Int32(int32(opts.FirstTabletIndex)),
		LastTabletIndex:  ptr.Int32(int32(opts.LastTabletIndex)),
	}
}

func convertTableReplicaMode(mode *yt.TableReplicaMode) (*rpc_proxy.ETableReplicaMode, error) {
	if mode == nil {
		return nil, nil
	}

	var ret rpc_proxy.ETableReplicaMode

	switch *mode {
	case yt.AsyncMode:
		ret = rpc_proxy.ETableReplicaMode_TRM_ASYNC
	case yt.SyncMode:
		ret = rpc_proxy.ETableReplicaMode_TRM_SYNC
	default:
		return nil, xerrors.Errorf("unexpected table replica mode %q", *mode)
	}

	return &ret, nil
}

func convertTimestamp(ts *yt.Timestamp) *uint64 {
	if ts == nil {
		return nil
	}
	return ptr.Uint64(uint64(*ts))
}

func intPtrToInt32Ptr(i *int) *int32 {
	if i == nil {
		return nil
	}
	return ptr.Int32(int32(*i))
}

func intPtrToUint64Ptr(i *int) *uint64 {
	if i == nil {
		return nil
	}
	return ptr.Uint64(uint64(*i))
}

type LockMode int32

const (
	LockModeNone      LockMode = 0
	LockModeSnapshot  LockMode = 1
	LockModeShared    LockMode = 2
	LockModeExclusive LockMode = 3
)

func convertLockMode(m yt.LockMode) (LockMode, error) {
	var ret LockMode

	switch m {
	case yt.LockSnapshot:
		ret = LockModeSnapshot
	case yt.LockShared:
		ret = LockModeShared
	case yt.LockExclusive:
		ret = LockModeExclusive
	default:
		return 0, xerrors.Errorf("unexpected lock mode %q", m)
	}

	return ret, nil
}

func makeLockNodeResult(r *rpc_proxy.TRspLockNode) (yt.LockResult, error) {
	if r == nil {
		return yt.LockResult{}, xerrors.Errorf("unable to convert nil lock node result")
	}

	ret := yt.LockResult{
		NodeID: makeNodeID(r.NodeId),
		LockID: makeGUID(r.LockId),
	}

	return ret, nil
}

func convertPlaceHolderValues(placeholderValues any) ([]byte, error) {
	if placeholderValues == nil {
		return nil, nil
	}

	return yson.Marshal(placeholderValues)
}
