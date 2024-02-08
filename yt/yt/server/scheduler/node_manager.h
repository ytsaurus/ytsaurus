#pragma once

#include "private.h"
#include "node_shard.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct INodeManagerHost
{
    virtual ~INodeManagerHost() = default;

    virtual TString FormatHeartbeatResourceUsage(
        const TJobResources& usage,
        const TJobResources& limits,
        const NNodeTrackerClient::NProto::TDiskResources& diskResources) const = 0;

    virtual const ISchedulerStrategyPtr& GetStrategy() const = 0;

    virtual int GetOperationsArchiveVersion() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeManager
    : public TRefCounted
    , public INodeShardHost
{
public:
    TNodeManager(TSchedulerConfigPtr config, INodeManagerHost* host, TBootstrap* bootstrap);

    void ProcessNodeHeartbeat(const TScheduler::TCtxNodeHeartbeatPtr& context);

    void UpdateConfig(const TSchedulerConfigPtr& config);

    void OnMasterConnected(const TMasterHandshakeResult& result);
    void OnMasterDisconnected();

    void RegisterOperation(
        TOperationId operationId,
        TControllerEpoch controllerEpoch,
        const IOperationControllerPtr& controller,
        bool waitingForRevival);
    void StartOperationRevival(TOperationId operationId, TControllerEpoch newControllerEpoch);
    TFuture<void> FinishOperationRevival(
        TOperationId operationId,
        std::vector<TAllocationPtr> allocations);
    TFuture<void> ResetOperationRevival(const TOperationPtr& operation);
    void UnregisterOperation(TOperationId operationId);

    void AbortAllocationsAtNode(NNodeTrackerClient::TNodeId nodeId, EAbortReason reason);

    TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors();

    TError HandleNodesAttributes(const NYTree::IListNodePtr& nodeList);

    void AbortOperationAllocations(
        TOperationId operationId,
        const TError& error,
        EAbortReason abortReason,
        bool terminated);
    void ResumeOperationAllocations(TOperationId operationId);

    TFuture<NNodeTrackerClient::TNodeDescriptor> GetAllocationNode(TAllocationId allocationId);

    TFuture<TAllocationDescription> GetAllocationDescription(TAllocationId allocationId);

    void AbortAllocations(const std::vector<TAllocationId>& allocationIds, const TError& error, EAbortReason abortReason);

    TNodeYsonList BuildNodeYsonList() const;

    TFuture<TOperationId> FindOperationIdByAllocationId(TAllocationId allocationId);

    TJobResources GetResourceLimits(const TSchedulingTagFilter& filter) const;
    TJobResources GetResourceUsage(const TSchedulingTagFilter& filter) const;

    int GetActiveAllocationCount() const;
    int GetExecNodeCount() const;
    int GetTotalNodeCount() const;
    int GetSubmitToStrategyAllocationCount() const;

    int GetTotalConcurrentHeartbeatComplexity() const;

    int ExtractJobReporterWriteFailuresCount();
    int GetJobReporterQueueIsTooLargeNodeCount();

    // TODO(eshcherbin): Think how to hide node shards behind node manager completely.
    // Invoker affinity: any.
    int GetNodeShardCount() const;
    int GetNodeShardId(NNodeTrackerClient::TNodeId nodeId) const override;
    const std::vector<TNodeShardPtr>& GetNodeShards() const;
    const std::vector<IInvokerPtr>& GetNodeShardInvokers() const;

    int GetOngoingHeartbeatsCount() const;

    void RegisterAgentAtNodeShards(const TAgentId& id, const NNodeTrackerClient::TAddressMap& addresses, TIncarnationId incarnationId);
    void UnregisterAgentFromNodeShards(const TAgentId& id);

private:
    TSchedulerConfigPtr Config_;
    // TODO(eshcherbin): Remove bootstrap in favor of new host methods.
    TBootstrap* const Bootstrap_;

    std::vector<TNodeShardPtr> NodeShards_;
    std::vector<IInvokerPtr> CancelableNodeShardInvokers_;

    // Special map to support node consistency between node shards YT-11381.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, NodeAddressToNodeIdLock_);
    THashMap<TString, NNodeTrackerClient::TNodeId> NodeAddressToNodeId_;

    const TNodeShardPtr& GetNodeShard(NNodeTrackerClient::TNodeId nodeId) const;
    const TNodeShardPtr& GetNodeShardByAllocationId(TAllocationId allocationId) const;

    template <typename TCallback>
    auto ExecuteInNodeShards(TCallback callback) const -> std::vector<typename TFutureTraits<decltype(callback(std::declval<const TNodeShardPtr&>()))>::TWrapped>;
};

DEFINE_REFCOUNTED_TYPE(TNodeManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
