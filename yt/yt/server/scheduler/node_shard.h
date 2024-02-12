#pragma once

#include "private.h"
#include "scheduler.h"
#include "scheduler_strategy.h"

#include <yt/yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/misc/sync_expiring_cache.h>

#include <util/generic/hash_multi_map.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct INodeShardHost
{
    virtual ~INodeShardHost() = default;

    virtual int GetNodeShardId(NNodeTrackerClient::TNodeId nodeId) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TNodeShardMasterHandshakeResult final
{
    THashSet<TOperationId> OperationIds;
};

using TNodeShardMasterHandshakeResultPtr = TIntrusivePtr<TNodeShardMasterHandshakeResult>;

////////////////////////////////////////////////////////////////////////////////

class TNodeShard
    : public TRefCounted
{
public:
    TNodeShard(
        int id,
        TSchedulerConfigPtr config,
        INodeShardHost* host,
        INodeManagerHost* managerHost,
        TBootstrap* bootstrap);

    int GetId() const;
    const IInvokerPtr& GetInvoker() const;

    void UpdateConfig(const TSchedulerConfigPtr& config);

    IInvokerPtr OnMasterConnected(const TNodeShardMasterHandshakeResultPtr& result);
    void OnMasterDisconnected();

    void RegisterOperation(
        TOperationId operationId,
        TControllerEpoch controllerEpoch,
        const IOperationControllerPtr& controller,
        bool waitingForRevival);
    void StartOperationRevival(TOperationId operationId, TControllerEpoch newControllerEpoch);
    void FinishOperationRevival(
        TOperationId operationId,
        const std::vector<TAllocationPtr>& allocations);
    void ResetOperationRevival(TOperationId operationId);
    void UnregisterOperation(TOperationId operationId);

    void ProcessHeartbeat(const TScheduler::TCtxNodeHeartbeatPtr& context);

    void UnregisterAndRemoveNodeById(NNodeTrackerClient::TNodeId nodeId);
    void AbortAllocationsAtNode(NNodeTrackerClient::TNodeId nodeId, EAbortReason reason);


    TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors();
    void UpdateExecNodeDescriptors();

    void RemoveMissingNodes(const std::vector<TString>& nodeAddresses);
    std::vector<TError> HandleNodesAttributes(const std::vector<std::pair<TString, NYTree::INodePtr>>& nodeMaps);

    void AbortOperationAllocations(
        TOperationId operationId,
        const TError& abortError,
        EAbortReason abortReason,
        bool controllerTerminated);
    void ResumeOperationAllocations(TOperationId operationId);

    NNodeTrackerClient::TNodeDescriptor GetAllocationNode(TAllocationId allocationId);

    TAllocationDescription GetAllocationDescription(TAllocationId allocationId);

    void AbortAllocation(TAllocationId allocationId, const TError& error, EAbortReason abortReason);
    void AbortAllocations(
        const std::vector<TAllocationId>& allocationIds,
        const TError& error,
        EAbortReason abortReason);

    TNodeYsonList BuildNodeYsonList() const;

    TOperationId FindOperationIdByAllocationId(TAllocationId allocationId);

    TJobResources GetResourceLimits(const TSchedulingTagFilter& filter) const;
    TJobResources GetResourceUsage(const TSchedulingTagFilter& filter) const;

    int GetActiveAllocationCount() const;
    int GetExecNodeCount() const;
    int GetTotalNodeCount() const;
    int GetSubmitToStrategyAllocationCount() const;

    int GetTotalConcurrentHeartbeatComplexity() const;

    TFuture<TControllerScheduleAllocationResultPtr> BeginScheduleAllocation(
        TIncarnationId incarnationId,
        TOperationId operationId,
        TAllocationId allocationId);
    void EndScheduleAllocation(
        const NProto::TScheduleAllocationResponse& response);
    void RemoveOutdatedScheduleAllocationEntries();

    int ExtractJobReporterWriteFailuresCount();
    int GetJobReporterQueueIsTooLargeNodeCount();

    TControllerEpoch GetOperationControllerEpoch(TOperationId operationId);
    TControllerEpoch GetAllocationControllerEpoch(TAllocationId allocationId);

    bool IsOperationControllerTerminated(TOperationId operationId) const noexcept;
    bool IsOperationRegistered(TOperationId operationId) const noexcept;
    bool AreNewAllocationsForbiddenForOperation(TOperationId operationId) const noexcept;

    int GetOnGoingHeartbeatCount() const noexcept;

    void RegisterAgent(
        TAgentId id,
        NNodeTrackerClient::TAddressMap addresses,
        TIncarnationId incarnationId);
    void UnregisterAgent(TAgentId id);

    struct TRunningAllocationTimeStatistics
    {
        TInstant PreemptibleProgressStartTime;
    };

    struct TRunningAllocationStatisticsUpdate
    {
        TAllocationId AllocationId;
        TRunningAllocationTimeStatistics TimeStatistics;
    };
    void UpdateRunningAllocationsStatistics(const std::vector<TRunningAllocationStatisticsUpdate>& updates);

private:
    const int Id_;
    TSchedulerConfigPtr Config_;
    INodeShardHost* const Host_;
    INodeManagerHost* const ManagerHost_;
    TBootstrap* const Bootstrap_;

    const NConcurrency::TActionQueuePtr ActionQueue_;
    const NConcurrency::TPeriodicExecutorPtr CachedExecNodeDescriptorsRefresher_;

    struct TResourceStatistics
    {
        TJobResources Usage;
        TJobResources Limits;
    };
    const TIntrusivePtr<TSyncExpiringCache<TSchedulingTagFilter, TResourceStatistics>> CachedResourceStatisticsByTags_;

    const NLogging::TLogger Logger;

    bool Connected_ = false;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    int ConcurrentHeartbeatCount_ = 0;
    std::atomic<int> ConcurrentHeartbeatComplexity_ = 0;

    bool HasOngoingNodesAttributesUpdate_ = false;

    std::atomic<int> ActiveAllocationCount_ = 0;

    TAtomicIntrusivePtr<TRefCountedExecNodeDescriptorMap> CachedExecNodeDescriptors_{New<TRefCountedExecNodeDescriptorMap>()};

    THashMap<NNodeTrackerClient::TNodeId, TExecNodePtr> IdToNode_;

    struct TControllerAgentInfo
    {
        NNodeTrackerClient::TAddressMap Addresses;
        TIncarnationId IncarnationId;
    };
    THashMap<TAgentId, TControllerAgentInfo> RegisteredAgents_;
    THashSet<TIncarnationId> RegisteredAgentIncarnationIds_;

    // Exec node is the node that is online and has user slots.
    std::atomic<int> ExecNodeCount_ = 0;
    std::atomic<int> TotalNodeCount_ = 0;

    std::atomic<int> JobReporterWriteFailuresCount_ = 0;
    std::atomic<int> JobReporterQueueIsTooLargeNodeCount_ = 0;

    using TAllocationCounterKey = std::tuple<EAllocationState, TString>;
    using TAllocationCounter = THashMap<TAllocationCounterKey, std::pair<i64, NProfiling::TGauge>>;
    TAllocationCounter AllocationCounter_;

    NProfiling::TCounter HardConcurrentHeartbeatLimitReachedCounter_;
    NProfiling::TCounter SoftConcurrentHeartbeatLimitReachedCounter_;
    NProfiling::TCounter ConcurrentHeartbeatComplexityLimitReachedCounter_;
    NProfiling::TCounter HeartbeatWithScheduleAllocationsCounter_;
    NProfiling::TCounter HeartbeatAllocationCount_;
    NProfiling::TCounter HeartbeatCount_;
    NProfiling::TCounter HeartbeatRequestProtoMessageBytes_;
    NProfiling::TCounter HeartbeatResponseProtoMessageBytes_;
    NProfiling::TCounter HeartbeatRegisteredControllerAgentsBytes_;

    THashMap<TAllocationId, TAllocationUpdate> AllocationsToSubmitToStrategy_;
    std::atomic<int> SubmitToStrategyAllocationCount_;

    struct TScheduleAllocationEntry
    {
        TOperationId OperationId;
        TIncarnationId IncarnationId;
        TPromise<TControllerScheduleAllocationResultPtr> Promise;
        THashMultiMap<TOperationId, THashMap<TAllocationId, TScheduleAllocationEntry>::iterator>::iterator OperationIdToAllocationIdsIterator;
        NProfiling::TCpuInstant StartTime;
    };
    // NB: It is important to use THash* instead of std::unordered_* since we rely on
    // iterators not to be invalidated.
    THashMap<TAllocationId, TScheduleAllocationEntry> AllocationIdToScheduleEntry_;
    THashMultiMap<TOperationId, THashMap<TAllocationId, TScheduleAllocationEntry>::iterator> OperationIdToAllocationIterators_;

    NConcurrency::TPeriodicExecutorPtr RemoveOutdatedScheduleAllocationEntryExecutor_;

    NConcurrency::TPeriodicExecutorPtr SubmitAllocationsToStrategyExecutor_;

    using TShardEpoch = ui64;

    struct TOperationState
    {
        TOperationState(
            IOperationControllerPtr controller,
            bool waitingForRevival,
            TShardEpoch shardEpoch,
            TControllerEpoch controllerEpoch)
            : Controller(std::move(controller))
            , WaitingForRevival(waitingForRevival)
            , ShardEpoch(shardEpoch)
            , ControllerEpoch(controllerEpoch)
        { }

        THashMap<TAllocationId, TAllocationPtr> Allocations;
        THashSet<TAllocationId> AllocationsToSubmitToStrategy;
        //! Used only to avoid multiple log messages per allocation about 'operation is not ready'.
        THashSet<TAllocationId> OperationUnreadyLoggedAllocationIds;
        IOperationControllerPtr Controller;
        bool ControllerTerminated = false;
        //! Raised to prevent races between suspension and scheduler strategy scheduling new allocations.
        bool ForbidNewAllocations = false;
        bool WaitingForRevival = true;
        TShardEpoch ShardEpoch;
        TControllerEpoch ControllerEpoch;
    };

    THashMap<TOperationId, TOperationState> IdToOperationState_;
    THashSet<TOperationId> WaitingForRegisterOperationIds_;
    TShardEpoch CurrentEpoch_ = 0;

    void ValidateConnected();

    void DoCleanup();

    void DoProcessHeartbeat(const TScheduler::TCtxNodeHeartbeatPtr& context);

    TResourceStatistics CalculateResourceStatistics(const TSchedulingTagFilter& filter);

    TExecNodePtr GetOrRegisterNode(
        NNodeTrackerClient::TNodeId nodeId,
        const NNodeTrackerClient::TNodeDescriptor& descriptor,
        ENodeState state);
    TExecNodePtr RegisterNode(
        NNodeTrackerClient::TNodeId nodeId,
        const NNodeTrackerClient::TNodeDescriptor& descriptor,
        ENodeState state);
    void UnregisterNode(const TExecNodePtr& node);
    void DoUnregisterNode(const TExecNodePtr& node);
    void OnNodeHeartbeatLeaseExpired(NNodeTrackerClient::TNodeId nodeId);
    void OnNodeRegistrationLeaseExpired(NNodeTrackerClient::TNodeId nodeId);
    // NB: 'node' passed by value since we want to own it after remove.
    void RemoveNode(TExecNodePtr node);

    void AbortAllAllocationsAtNode(const TExecNodePtr& node, EAbortReason reason);
    void DoAbortAllAllocationsAtNode(const TExecNodePtr& node, EAbortReason reason);

    void ProcessHeartbeatAllocations(
        TScheduler::TCtxNodeHeartbeat::TTypedRequest* request,
        TScheduler::TCtxNodeHeartbeat::TTypedResponse* response,
        const TExecNodePtr& node,
        const INodeHeartbeatStrategyProxyPtr& strategyProxy,
        std::vector<TAllocationPtr>* runningAllocations,
        bool* hasWaitingAllocations);

    TAllocationPtr ProcessAllocationHeartbeat(
        const TExecNodePtr& node,
        NProto::NNode::TRspHeartbeat* response,
        NProto::TAllocationStatus* allocationStatus);

    bool IsHeartbeatThrottlingWithComplexity(
        const TExecNodePtr& node,
        const INodeHeartbeatStrategyProxyPtr& strategyProxy);
    bool IsHeartbeatThrottlingWithCount(const TExecNodePtr& node);

    using TStateToAllocationList = TEnumIndexedArray<EAllocationState, std::vector<TAllocationPtr>>;
    void LogOngoingAllocationsOnHeartbeat(
        const INodeHeartbeatStrategyProxyPtr& strategyProxy,
        TInstant now,
        const TStateToAllocationList& ongoingAllocationsByState) const;

    void SubtractNodeResources(const TExecNodePtr& node);
    void AddNodeResources(const TExecNodePtr& node);
    void UpdateNodeResources(
        const TExecNodePtr& node,
        const TJobResources& limits,
        const TJobResources& usage,
        TDiskResources diskResources);

    void BeginNodeHeartbeatProcessing(const TExecNodePtr& node);
    void EndNodeHeartbeatProcessing(const TExecNodePtr& node);

    void SubmitAllocationsToStrategy();

    void ProcessScheduledAndPreemptedAllocations(
        const ISchedulingContextPtr& schedulingContext,
        NProto::NNode::TRspHeartbeat* response);

    void OnAllocationFinished(const TAllocationPtr& allocation);
    void OnAllocationAborted(
        const TAllocationPtr& allocation,
        const TError& error,
        EAbortReason abortReason);
    void OnAllocationRunning(const TAllocationPtr& allocation, NProto::TAllocationStatus* status);

    void UpdateProfilingCounter(const TAllocationPtr& allocation, int value);

    void SetAllocationState(const TAllocationPtr& allocation, EAllocationState state);

    void RegisterAllocation(const TAllocationPtr& allocation);
    void UnregisterAllocation(const TAllocationPtr& allocation, bool causedByRevival = false);

    void ProcessPreemptedAllocation(
        NProto::NNode::TRspHeartbeat* response,
        const TAllocationPtr& allocation,
        TDuration preemptionTimeout);
    void PreemptAllocation(const TAllocationPtr& allocation, NProfiling::TCpuDuration preemptionTimeout);
    void SendPreemptedAllocationToNode(
        NProto::NNode::TRspHeartbeat* response,
        const TAllocationPtr& allocation,
        TDuration preemptionTimeout) const;
    void DoPreemptAllocation(
        const TAllocationPtr& allocation,
        NProfiling::TCpuDuration preemptionTimeout = 0);

    void ProcessAllocationsToAbort(NProto::NNode::TRspHeartbeat* response, const TExecNodePtr& node);

    TExecNodePtr FindNodeByAllocation(TAllocationId allocationId);

    bool IsAllocationAborted(TAllocationId avId, const TExecNodePtr& node);

    TAllocationPtr FindAllocation(TAllocationId allocationId, const TExecNodePtr& node);
    TAllocationPtr FindAllocation(TAllocationId allocationId);
    TAllocationPtr GetAllocationOrThrow(TAllocationId allocationId);

    TOperationState* FindOperationState(TOperationId operationId) noexcept;
    const TOperationState* FindOperationState(TOperationId operationId) const noexcept;
    TOperationState& GetOperationState(TOperationId operationId) noexcept;
    const TOperationState& GetOperationState(TOperationId operationId) const noexcept;

    NYson::TYsonString BuildNodeYson(const TExecNodePtr& node) const;

    void UpdateNodeState(
        const TExecNodePtr& execNode,
        NNodeTrackerClient::ENodeState newState,
        ENodeState newSchedulerState,
        const TError& error = TError());

    void RemoveOperationScheduleAllocationEntries(TOperationId operationId);

    void SetFinishedState(const TAllocationPtr& allocation);

    void ProcessOperationInfoHeartbeat(
        const TScheduler::TCtxNodeHeartbeat::TTypedRequest* request,
        TScheduler::TCtxNodeHeartbeat::TTypedResponse* response);

    bool ShouldSendRegisteredControllerAgents(TScheduler::TCtxNodeHeartbeat::TTypedRequest* request);
    void AddRegisteredControllerAgentsToResponse(auto* response);

    void SetMinSpareResources(TScheduler::TCtxNodeHeartbeat::TTypedResponse* response);

    void UpdateAllocationTimeStatisticsIfNeeded(const TAllocationPtr& allocation, TRunningAllocationTimeStatistics timeStatistics);
};

DEFINE_REFCOUNTED_TYPE(TNodeShard)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
