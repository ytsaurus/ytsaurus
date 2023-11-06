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

    virtual TFuture<void> ValidateOperationAccess(
        const TString& user,
        TOperationId operationId,
        NYTree::EPermission permission) = 0;

    virtual TFuture<void> AttachJobContext(
        const NYTree::TYPath& path,
        NChunkClient::TChunkId chunkId,
        TOperationId operationId,
        TJobId jobId,
        const TString& user) = 0;

    virtual NJobProberClient::TJobProberServiceProxy CreateJobProberProxy(const TString& address) = 0;

    virtual int GetOperationArchiveVersion() const = 0;
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
        bool jobsReady);
    void StartOperationRevival(TOperationId operationId, TControllerEpoch newControllerEpoch);
    TFuture<void> FinishOperationRevival(
        TOperationId operationId,
        std::vector<TJobPtr> jobs);
    TFuture<void> ResetOperationRevival(const TOperationPtr& operation);
    void UnregisterOperation(TOperationId operationId);

    void AbortJobsAtNode(NNodeTrackerClient::TNodeId nodeId, EAbortReason reason);

    TRefCountedExecNodeDescriptorMapPtr GetExecNodeDescriptors();

    TError HandleNodesAttributes(const NYTree::IListNodePtr& nodeList);

    void AbortOperationJobs(
        TOperationId operationId,
        const TError& error,
        EAbortReason abortReason,
        bool terminated);
    void ResumeOperationJobs(TOperationId operationId);

    TFuture<NNodeTrackerClient::TNodeDescriptor> GetJobNode(TJobId jobId);

    TFuture<void> DumpJobInputContext(TJobId jobId, const NYTree::TYPath& path, const TString& user);
    TFuture<void> AbandonJob(TJobId jobId);
    TFuture<void> AbortJobByUserRequest(TJobId jobId, std::optional<TDuration> interruptTimeout, const TString& user);

    void AbortJobs(const std::vector<TJobId>& jobIds, const TError& error, EAbortReason abortReason);

    TNodeYsonList BuildNodeYsonList() const;

    TFuture<TOperationId> FindOperationIdByJobId(TJobId job);

    TJobResources GetResourceLimits(const TSchedulingTagFilter& filter) const;
    TJobResources GetResourceUsage(const TSchedulingTagFilter& filter) const;

    int GetActiveJobCount() const;
    int GetExecNodeCount() const;
    int GetTotalNodeCount() const;
    int GetSubmitToStrategyJobCount() const;

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
    const TNodeShardPtr& GetNodeShardByJobId(TJobId jobId) const;

    template <typename TCallback>
    auto ExecuteInNodeShards(TCallback callback) const -> std::vector<typename TFutureTraits<decltype(callback(std::declval<const TNodeShardPtr&>()))>::TWrapped>;
};

DEFINE_REFCOUNTED_TYPE(TNodeManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
