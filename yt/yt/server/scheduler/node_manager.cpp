#include "node_manager.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/server/lib/scheduler/proto/allocation_tracker_service.pb.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NScheduler {

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;
using namespace NControllerAgent;

using NNodeTrackerClient::TNodeId;
using NNodeTrackerClient::TNodeDescriptor;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TNodeManager::TNodeManager(TSchedulerConfigPtr config, INodeManagerHost* host, TBootstrap* bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
{
    for (int index = 0; index < Config_->NodeShardCount; ++index) {
        NodeShards_.push_back(New<TNodeShard>(
            index,
            Config_,
            this,
            host,
            Bootstrap_));
        CancelableNodeShardInvokers_.push_back(GetNullInvoker());
    }
}

void TNodeManager::ProcessNodeHeartbeat(const TScheduler::TCtxNodeHeartbeatPtr& context)
{
    auto* request = &context->Request();
    auto nodeId = FromProto<NNodeTrackerClient::TNodeId>(request->node_id());

    if (nodeId == InvalidNodeId) {
        THROW_ERROR_EXCEPTION("Cannot process a heartbeat with invalid node id");
    }

    auto unregisterFuture = VoidFuture;

    {
        auto guard = Guard(NodeAddressToNodeIdLock_);

        auto descriptor = FromProto<TNodeDescriptor>(request->node_descriptor());
        const auto& address = descriptor.GetDefaultAddress();
        auto it = NodeAddressToNodeId_.find(address);
        if (it != NodeAddressToNodeId_.end()) {
            auto oldNodeId = it->second;
            if (nodeId != oldNodeId) {
                auto nodeShard = GetNodeShard(oldNodeId);
                unregisterFuture =
                    BIND(&TNodeShard::UnregisterAndRemoveNodeById, GetNodeShard(oldNodeId), oldNodeId)
                        .AsyncVia(nodeShard->GetInvoker())
                        .Run();
            }
        } else {
            NodeAddressToNodeId_[address] = nodeId;
        }
    }

    unregisterFuture.Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
        if (!error.IsOK()) {
            context->Reply(error);
            return;
        }

        const auto& nodeShard = GetNodeShard(nodeId);
        nodeShard->GetInvoker()->Invoke(BIND(&TNodeShard::ProcessHeartbeat, nodeShard, context));
    }));
}

void TNodeManager::UpdateConfig(const TSchedulerConfigPtr& config)
{
    Config_ = config;

    for (const auto& nodeShard : NodeShards_) {
        nodeShard->GetInvoker()->Invoke(
            BIND(&TNodeShard::UpdateConfig, nodeShard, config));
    }
}

void TNodeManager::OnMasterConnected(const TMasterHandshakeResult& result)
{
    auto nodeShardResult = New<TNodeShardMasterHandshakeResult>();
    nodeShardResult->OperationIds.reserve(std::size(result.Operations));
    for (const auto& operation : result.Operations) {
        nodeShardResult->OperationIds.insert(operation->GetId());
    }

    std::vector<TFuture<IInvokerPtr>> asyncInvokers;
    for (const auto& nodeShard : NodeShards_) {
        asyncInvokers.push_back(BIND(&TNodeShard::OnMasterConnected, nodeShard, nodeShardResult)
            .AsyncVia(nodeShard->GetInvoker())
            .Run());
    }

    auto invokerOrError = WaitFor(AllSucceeded(asyncInvokers));
    if (!invokerOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error connecting node shards")
                << invokerOrError;
    }

    const auto& invokers = invokerOrError.Value();
    for (size_t index = 0; index < NodeShards_.size(); ++index) {
        CancelableNodeShardInvokers_[index] = invokers[index];
    }
}

void TNodeManager::OnMasterDisconnected()
{
    std::vector<TFuture<void>> asyncResults;
    for (const auto& nodeShard : NodeShards_) {
        asyncResults.push_back(BIND(&TNodeShard::OnMasterDisconnected, nodeShard)
            .AsyncVia(nodeShard->GetInvoker())
            .Run());
    }

    // XXX(babenko): fiber switch is forbidden here; do we actually need to wait for these results?
    AllSucceeded(asyncResults)
        .Get();
}

void TNodeManager::RegisterOperation(
    TOperationId operationId,
    TControllerEpoch controllerEpoch,
    const IOperationControllerPtr& controller,
    bool jobsReady)
{
    for (const auto& nodeShard : NodeShards_) {
        nodeShard->GetInvoker()->Invoke(BIND(
            &TNodeShard::RegisterOperation,
            nodeShard,
            operationId,
            controllerEpoch,
            controller,
            jobsReady));
    }
}

void TNodeManager::StartOperationRevival(TOperationId operationId, TControllerEpoch newControllerEpoch)
{
    for (const auto& nodeShard : NodeShards_) {
        nodeShard->GetInvoker()->Invoke(BIND(
            &TNodeShard::StartOperationRevival,
            nodeShard,
            operationId,
            newControllerEpoch));
    }
}

TFuture<void> TNodeManager::FinishOperationRevival(
    TOperationId operationId,
    std::vector<TJobPtr> jobs)
{
    std::vector<std::vector<TJobPtr>> jobsPerShard(NodeShards_.size());
    for (auto& job : jobs) {
        auto shardId = GetNodeShardId(NodeIdFromJobId(job->GetId()));
        jobsPerShard[shardId].push_back(std::move(job));
    }

    std::vector<TFuture<void>> asyncResults;
    for (int shardId = 0; shardId < std::ssize(NodeShards_); ++shardId) {
        auto asyncResult = BIND(&TNodeShard::FinishOperationRevival, NodeShards_[shardId])
            .AsyncVia(NodeShards_[shardId]->GetInvoker())
            .Run(operationId, std::move(jobsPerShard[shardId]));
        asyncResults.emplace_back(std::move(asyncResult));
    }
    return AllSucceeded(asyncResults);
}

TFuture<void> TNodeManager::ResetOperationRevival(const TOperationPtr& operation)
{
    std::vector<TFuture<void>> asyncResults;
    for (int shardId = 0; shardId < std::ssize(NodeShards_); ++shardId) {
        auto asyncResult = BIND(&TNodeShard::ResetOperationRevival, NodeShards_[shardId])
            .AsyncVia(NodeShards_[shardId]->GetInvoker())
            .Run(operation->GetId());
        asyncResults.emplace_back(std::move(asyncResult));
    }
    return AllSucceeded(asyncResults);
}

void TNodeManager::UnregisterOperation(TOperationId operationId)
{
    for (const auto& nodeShard : NodeShards_) {
        nodeShard->GetInvoker()->Invoke(BIND(
            &TNodeShard::UnregisterOperation,
            nodeShard,
            operationId));
    }
}

void TNodeManager::AbortJobsAtNode(TNodeId nodeId, EAbortReason reason)
{
    auto nodeShard = GetNodeShard(nodeId);
    YT_UNUSED_FUTURE(BIND(&TNodeShard::AbortJobsAtNode, nodeShard, nodeId, reason)
        .AsyncVia(nodeShard->GetInvoker())
        .Run());
}

TRefCountedExecNodeDescriptorMapPtr TNodeManager::GetExecNodeDescriptors()
{
    std::vector<TFuture<TRefCountedExecNodeDescriptorMapPtr>> shardDescriptorsFutures;
    for (const auto& nodeShard : NodeShards_) {
        shardDescriptorsFutures.push_back(BIND(&TNodeShard::GetExecNodeDescriptors, nodeShard)
            .AsyncVia(nodeShard->GetInvoker())
            .Run());
    }

    auto shardDescriptors = WaitFor(AllSucceeded(shardDescriptorsFutures))
        .ValueOrThrow();

    auto result = New<TRefCountedExecNodeDescriptorMap>();
    for (const auto& descriptors : shardDescriptors) {
        for (const auto& pair : *descriptors) {
            InsertOrCrash(*result, pair);
        }
    }
    return result;
}

TError TNodeManager::HandleNodesAttributes(const NYTree::IListNodePtr& nodeList)
{
    std::vector<std::vector<std::pair<TString, INodePtr>>> nodesPerShard(NodeShards_.size());
    std::vector<std::vector<TString>> nodeAddressesPerShard(NodeShards_.size());

    for (const auto& child : nodeList->GetChildren()) {
        auto address = child->GetValue<TString>();
        auto objectId = child->Attributes().Get<TObjectId>("id");
        auto nodeId = NodeIdFromObjectId(objectId);
        auto shardId = GetNodeShardId(nodeId);
        nodeAddressesPerShard[shardId].push_back(address);
        nodesPerShard[shardId].emplace_back(address, child);
    }

    std::vector<TFuture<void>> removeFutures;
    for (int i = 0 ; i < std::ssize(NodeShards_); ++i) {
        auto& nodeShard = NodeShards_[i];
        removeFutures.push_back(
            BIND(&TNodeShard::RemoveMissingNodes, nodeShard)
                .AsyncVia(nodeShard->GetInvoker())
                .Run(std::move(nodeAddressesPerShard[i])));
    }
    WaitFor(AllSucceeded(removeFutures))
        .ThrowOnError();

    std::vector<TFuture<std::vector<TError>>> handleFutures;
    for (int i = 0 ; i < std::ssize(NodeShards_); ++i) {
        auto& nodeShard = NodeShards_[i];
        handleFutures.push_back(
            BIND(&TNodeShard::HandleNodesAttributes, nodeShard)
                .AsyncVia(nodeShard->GetInvoker())
                .Run(std::move(nodesPerShard[i])));
    }
    auto handleErrorsPerShard = WaitFor(AllSucceeded(handleFutures))
        .ValueOrThrow();

    std::vector<TError> handleErrors;
    for (auto& errors : handleErrorsPerShard) {
        for (auto& error : errors) {
            handleErrors.push_back(std::move(error));
        }
    }

    if (!handleErrors.empty()) {
        return TError("Failed to update some nodes")
            << handleErrors;
    }

    return {};
}

void TNodeManager::AbortOperationJobs(
    TOperationId operationId,
    const TError& error,
    EAbortReason abortReason,
    bool terminated)
{
    std::vector<TFuture<void>> abortFutures;
    for (const auto& nodeShard : NodeShards_) {
        abortFutures.push_back(BIND(&TNodeShard::AbortOperationJobs, nodeShard)
            .AsyncVia(nodeShard->GetInvoker())
            .Run(operationId, error, abortReason, terminated));
    }
    WaitFor(AllSucceeded(abortFutures))
        .ThrowOnError();
}

void TNodeManager::ResumeOperationJobs(TOperationId operationId)
{
    std::vector<TFuture<void>> futures;
    for (const auto& nodeShard : NodeShards_) {
        futures.push_back(BIND(&TNodeShard::ResumeOperationJobs, nodeShard)
            .AsyncVia(nodeShard->GetInvoker())
            .Run(operationId));
    }
    WaitFor(AllSucceeded(futures))
        .ThrowOnError();
}

TFuture<TNodeDescriptor> TNodeManager::GetJobNode(TJobId jobId)
{
    const auto& nodeShard = GetNodeShardByAllocationId(
        AllocationIdFromJobId(jobId));

    return BIND(&TNodeShard::GetJobNode, nodeShard, jobId)
        .AsyncVia(nodeShard->GetInvoker())
        .Run();
}

void TNodeManager::AbortJobs(const std::vector<TJobId>& jobIds, const TError& error, EAbortReason abortReason)
{
    std::vector<std::vector<TJobId>> jobIdsPerShard(NodeShards_.size());
    for (auto jobId : jobIds) {
        auto shardId = GetNodeShardId(NodeIdFromJobId(jobId));
        jobIdsPerShard[shardId].push_back(jobId);
    }

    for (int shardId = 0; shardId < std::ssize(NodeShards_); ++shardId) {
        if (jobIdsPerShard[shardId].empty()) {
            continue;
        }

        const auto& nodeShard = NodeShards_[shardId];
        nodeShard->GetInvoker()->Invoke(BIND(
            &TNodeShard::AbortJobs,
            nodeShard,
            Passed(std::move(jobIdsPerShard[shardId])),
            error,
            abortReason));
    }
}

TNodeYsonList TNodeManager::BuildNodeYsonList() const
{
    std::vector<TFuture<TNodeYsonList>> futures;
    futures.reserve(NodeShards_.size());
    for (const auto& nodeShard : NodeShards_) {
        futures.push_back(
            BIND(&TNodeShard::BuildNodeYsonList, nodeShard)
                .AsyncVia(nodeShard->GetInvoker())
                .Run());
    }

    auto nodeYsonsFromShards = WaitFor(AllSucceeded(std::move(futures)))
        .ValueOrThrow();

    TNodeYsonList nodeYsons;
    for (auto& fragment : nodeYsonsFromShards) {
        for (auto& nodeIdAndYson : fragment) {
            nodeYsons.push_back(std::move(nodeIdAndYson));
        }
    }

    return nodeYsons;
}

TFuture<TOperationId> TNodeManager::FindOperationIdByAllocationId(TAllocationId allocationId)
{
    const auto& nodeShard = GetNodeShardByAllocationId(allocationId);

    return BIND(&TNodeShard::FindOperationIdByAllocationId, nodeShard, allocationId)
        .AsyncVia(nodeShard->GetInvoker())
        .Run();
}

TJobResources TNodeManager::GetResourceLimits(const TSchedulingTagFilter& filter) const
{
    TJobResources result;
    for (const auto& nodeShard : NodeShards_) {
        result += nodeShard->GetResourceLimits(filter);
    }
    return result;
}

TJobResources TNodeManager::GetResourceUsage(const TSchedulingTagFilter& filter) const
{
    TJobResources result;
    for (const auto& nodeShard : NodeShards_) {
        result += nodeShard->GetResourceUsage(filter);
    }
    return result;
}

int TNodeManager::GetActiveJobCount() const
{
    int activeJobCount = 0;
    for (const auto& nodeShard : NodeShards_) {
        activeJobCount += nodeShard->GetActiveJobCount();
    }
    return activeJobCount;
}

int TNodeManager::GetExecNodeCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    int execNodeCount = 0;
    for (const auto& nodeShard : NodeShards_) {
        execNodeCount += nodeShard->GetExecNodeCount();
    }
    return execNodeCount;
}

int TNodeManager::GetTotalNodeCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    int totalNodeCount = 0;
    for (const auto& nodeShard : NodeShards_) {
        totalNodeCount += nodeShard->GetTotalNodeCount();
    }
    return totalNodeCount;
}

int TNodeManager::GetSubmitToStrategyJobCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    int submitToStrategyJobCount = 0;
    for (const auto& nodeShard : NodeShards_) {
        submitToStrategyJobCount += nodeShard->GetSubmitToStrategyJobCount();
    }
    return submitToStrategyJobCount;
}

int TNodeManager::GetTotalConcurrentHeartbeatComplexity() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    int totalConcurrentHeartbeatComplexity = 0;
    for (const auto& nodeShard : NodeShards_) {
        totalConcurrentHeartbeatComplexity += nodeShard->GetTotalConcurrentHeartbeatComplexity();
    }
    return totalConcurrentHeartbeatComplexity;
}

int TNodeManager::ExtractJobReporterWriteFailuresCount()
{
    int result = 0;
    for (const auto& shard : NodeShards_) {
        result += shard->ExtractJobReporterWriteFailuresCount();
    }
    return result;
}

int TNodeManager::GetJobReporterQueueIsTooLargeNodeCount()
{
    int result = 0;
    for (const auto& shard : NodeShards_) {
        result += shard->GetJobReporterQueueIsTooLargeNodeCount();
    }
    return result;
}

int TNodeManager::GetNodeShardCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return std::ssize(NodeShards_);
}

int TNodeManager::GetNodeShardId(TNodeId nodeId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return nodeId.Underlying() % std::ssize(NodeShards_);
}

const std::vector<TNodeShardPtr>& TNodeManager::GetNodeShards() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return NodeShards_;
}

const std::vector<IInvokerPtr>& TNodeManager::GetNodeShardInvokers() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CancelableNodeShardInvokers_;
}

int TNodeManager::GetOngoingHeartbeatsCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto futures = ExecuteInNodeShards([] (const TNodeShardPtr& nodeShard) {
        return nodeShard->GetOnGoingHeartbeatCount();
    });

    auto future = AllSucceeded(std::move(futures));

    auto heartbeatCounts = WaitFor(std::move(future)).ValueOrThrow();
    int result = 0;
    for (auto count : heartbeatCounts) {
        result += count;
    }

    return result;
}

void TNodeManager::RegisterAgentAtNodeShards(
    const TAgentId& id,
    const NNodeTrackerClient::TAddressMap& addresses,
    TIncarnationId incarnationId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto futures = ExecuteInNodeShards([id, addresses, incarnationId] (const TNodeShardPtr& nodeShard) mutable {
        nodeShard->RegisterAgent(std::move(id), std::move(addresses), incarnationId);
    });

    auto error = WaitFor(AllSucceeded(std::move(futures)));
    YT_LOG_FATAL_IF(
        !error.IsOK(),
        error,
        "Failed to register agent at node shards");
}

void TNodeManager::UnregisterAgentFromNodeShards(const TAgentId& id)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto futures = ExecuteInNodeShards([id] (const TNodeShardPtr& nodeShard) mutable {
        nodeShard->UnregisterAgent(std::move(id));
    });

    auto error = WaitFor(AllSucceeded(std::move(futures)));
    YT_LOG_FATAL_IF(
        !error.IsOK(),
        error,
        "Failed to unregister agents from node shards");
}

const TNodeShardPtr& TNodeManager::GetNodeShard(NNodeTrackerClient::TNodeId nodeId) const
{
    return NodeShards_[GetNodeShardId(nodeId)];
}

const TNodeShardPtr& TNodeManager::GetNodeShardByAllocationId(TAllocationId allocationId) const
{
    auto nodeId = NodeIdFromAllocationId(allocationId);
    return GetNodeShard(nodeId);
}

template <typename TCallback>
auto TNodeManager::ExecuteInNodeShards(TCallback callback) const -> std::vector<typename TFutureTraits<decltype(callback(std::declval<const TNodeShardPtr&>()))>::TWrapped>
{
    using TCallbackReturnType = decltype(callback(std::declval<const TNodeShardPtr&>()));
    std::vector<typename TFutureTraits<TCallbackReturnType>::TWrapped> result;
    result.reserve(GetNodeShardCount());

    for (const auto& nodeShard : NodeShards_) {
        result.push_back(BIND(callback, nodeShard)
            .AsyncVia(nodeShard->GetInvoker())
            .Run());
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
