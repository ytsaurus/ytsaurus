#include "node_manager.h"

#include "node_shard.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "operation_controller.h"
#include "controller_agent.h"
#include "bootstrap.h"
#include "helpers.h"
#include "persistent_scheduler_state.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/exec_node/public.h>

#include <yt/yt/server/lib/misc/job_report.h>

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/server/lib/scheduler/proto/allocation_tracker_service.pb.h>
#include <yt/yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/job_proxy/public.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/ytree/ypath_resolver.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;
using namespace NCypressClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

using NNodeTrackerClient::TNodeId;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

void SetControllerAgentDescriptor(
    const TAgentId& agentId,
    const TAddressMap& addresses,
    TIncarnationId incarnationId,
    NControllerAgent::NProto::TControllerAgentDescriptor* proto)
{
    ToProto(proto->mutable_addresses(), addresses);
    ToProto(proto->mutable_incarnation_id(), incarnationId);
    ToProto(proto->mutable_agent_id(), agentId);
}

void SetControllerAgentDescriptor(
    const TControllerAgentPtr& agent,
    NControllerAgent::NProto::TControllerAgentDescriptor* proto)
{
    SetControllerAgentDescriptor(
        agent->GetId(),
        agent->GetAgentAddresses(),
        agent->GetIncarnationId(),
        proto);
}

void SetControllerAgentIncarnationId(
    const TControllerAgentPtr& agent,
    NControllerAgent::NProto::TControllerAgentDescriptor* proto)
{
    ToProto(proto->mutable_incarnation_id(), agent->GetIncarnationId());
}

void AddAllocationToPreempt(
    NProto::NNode::TRspHeartbeat* response,
    TAllocationId allocationId,
    TDuration duration,
    const std::optional<TString>& preemptionReason,
    const std::optional<TPreemptedFor>& preemptedFor)
{
    auto allocationToPreempt = response->add_allocations_to_preempt();
    ToProto(allocationToPreempt->mutable_allocation_id(), allocationId);
    allocationToPreempt->set_timeout(ToProto<i64>(duration));

    // COMPAT(pogorelov): Remove after 23.3 will be everywhere.
    allocationToPreempt->set_interruption_reason(static_cast<int>(EInterruptReason::Preemption));

    if (preemptionReason) {
        allocationToPreempt->set_preemption_reason(*preemptionReason);
    }

    if (preemptedFor) {
        ToProto(allocationToPreempt->mutable_preempted_for(), *preemptedFor);
    }
}

std::optional<EAbortReason> ParseAbortReason(const TError& error, TAllocationId allocationId, const NLogging::TLogger& Logger)
{
    auto abortReasonString = error.Attributes().Find<TString>("abort_reason");
    if (!abortReasonString) {
        return {};
    }

    auto abortReason = TryParseEnum<EAbortReason>(*abortReasonString);
    if (!abortReason) {
        YT_LOG_DEBUG(
            "Failed to parse abort reason from result (AllocationId: %v, UnparsedAbortReason: %v)",
            allocationId,
            *abortReasonString);
    }

    return abortReason;
}

void AddAllocationToAbort(NProto::NNode::TRspHeartbeat* response, const TAllocationToAbort& allocationToAbort)
{
    NProto::ToProto(response->add_allocations_to_abort(), allocationToAbort);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TNodeShard::TNodeShard(
    int id,
    TSchedulerConfigPtr config,
    INodeShardHost* host,
    INodeManagerHost* managerHost,
    TBootstrap* bootstrap)
    : Id_(id)
    , Config_(std::move(config))
    , Host_(host)
    , ManagerHost_(managerHost)
    , Bootstrap_(bootstrap)
    // NB: we intentionally use ':' separator here, since we believe that all node shard
    // threads are equally loaded.
    , ActionQueue_(New<TActionQueue>(Format("NodeShard:%v", id)))
    , CachedExecNodeDescriptorsRefresher_(New<TPeriodicExecutor>(
        GetInvoker(),
        BIND(&TNodeShard::UpdateExecNodeDescriptors, MakeWeak(this)),
        Config_->NodeShardExecNodesCacheUpdatePeriod))
    , CachedResourceStatisticsByTags_(New<TSyncExpiringCache<TSchedulingTagFilter, TResourceStatistics>>(
        BIND(&TNodeShard::CalculateResourceStatistics, MakeStrong(this)),
        Config_->SchedulingTagFilterExpireTimeout,
        GetInvoker()))
    , Logger(NodeShardLogger.WithTag("NodeShardId: %v", Id_))
    , RemoveOutdatedScheduleAllocationEntryExecutor_(New<TPeriodicExecutor>(
        GetInvoker(),
        BIND(&TNodeShard::RemoveOutdatedScheduleAllocationEntries, MakeWeak(this)),
        Config_->ScheduleAllocationEntryCheckPeriod))
    , SubmitAllocationsToStrategyExecutor_(New<TPeriodicExecutor>(
        GetInvoker(),
        BIND(&TNodeShard::SubmitAllocationsToStrategy, MakeWeak(this)),
        Config_->NodeShardSubmitAllocationsToStrategyPeriod))
{
    SoftConcurrentHeartbeatLimitReachedCounter_ = SchedulerProfiler
        .WithTag("limit_type", "soft")
        .Counter("/node_heartbeat/concurrent_limit_reached_count");
    HardConcurrentHeartbeatLimitReachedCounter_ = SchedulerProfiler
        .WithTag("limit_type", "hard")
        .Counter("/node_heartbeat/concurrent_limit_reached_count");
    ConcurrentHeartbeatComplexityLimitReachedCounter_ = SchedulerProfiler
        .Counter("/node_heartbeat/concurrent_complexity_limit_reached_count");
    HeartbeatWithScheduleAllocationsCounter_ = SchedulerProfiler
        .Counter("/node_heartbeat/with_schedule_jobs_count");
    HeartbeatAllocationCount_ = SchedulerProfiler
        .Counter("/node_heartbeat/job_count");
    HeartbeatCount_ = SchedulerProfiler
        .Counter("/node_heartbeat/count");
    HeartbeatRequestProtoMessageBytes_ = SchedulerProfiler
        .Counter("/node_heartbeat/request/proto_message_bytes");
    HeartbeatResponseProtoMessageBytes_ = SchedulerProfiler
        .Counter("/node_heartbeat/response/proto_message_bytes");
    HeartbeatRegisteredControllerAgentsBytes_ = SchedulerProfiler
        .Counter("/node_heartbeat/response/proto_registered_controller_agents_bytes");
}

int TNodeShard::GetId() const
{
    return Id_;
}

const IInvokerPtr& TNodeShard::GetInvoker() const
{
    return ActionQueue_->GetInvoker();
}

void TNodeShard::UpdateConfig(const TSchedulerConfigPtr& config)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    Config_ = config;

    SubmitAllocationsToStrategyExecutor_->SetPeriod(config->NodeShardSubmitAllocationsToStrategyPeriod);
    CachedExecNodeDescriptorsRefresher_->SetPeriod(config->NodeShardExecNodesCacheUpdatePeriod);
    CachedResourceStatisticsByTags_->SetExpirationTimeout(Config_->SchedulingTagFilterExpireTimeout);
}

IInvokerPtr TNodeShard::OnMasterConnected(const TNodeShardMasterHandshakeResultPtr& result)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    DoCleanup();

    YT_VERIFY(!Connected_);
    Connected_ = true;

    WaitingForRegisterOperationIds_ = result->OperationIds;

    YT_VERIFY(!CancelableContext_);
    CancelableContext_ = New<TCancelableContext>();
    CancelableInvoker_ = CancelableContext_->CreateInvoker(GetInvoker());

    CachedExecNodeDescriptorsRefresher_->Start();
    SubmitAllocationsToStrategyExecutor_->Start();

    return CancelableInvoker_;
}

void TNodeShard::OnMasterDisconnected()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    DoCleanup();
}

void TNodeShard::ValidateConnected()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    if (!Connected_) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Node shard is not connected");
    }
}

void TNodeShard::DoCleanup()
{
    Connected_ = false;

    if (CancelableContext_) {
        CancelableContext_->Cancel(TError("Node shard disconnected"));
        CancelableContext_.Reset();
    }

    CancelableInvoker_.Reset();

    YT_UNUSED_FUTURE(CachedExecNodeDescriptorsRefresher_->Stop());

    for (const auto& [nodeId, node] : IdToNode_) {
        TLeaseManager::CloseLease(node->GetRegistrationLease());
        TLeaseManager::CloseLease(node->GetHeartbeatLease());
    }

    IdToOperationState_.clear();

    IdToNode_.clear();
    ExecNodeCount_ = 0;
    TotalNodeCount_ = 0;

    ActiveAllocationCount_ = 0;

    AllocationCounter_.clear();

    AllocationsToSubmitToStrategy_.clear();

    ConcurrentHeartbeatCount_ = 0;

    JobReporterQueueIsTooLargeNodeCount_ = 0;

    AllocationIdToScheduleEntry_.clear();
    OperationIdToAllocationIterators_.clear();

    RegisteredAgents_.clear();
    RegisteredAgentIncarnationIds_.clear();

    SubmitAllocationsToStrategy();
}

void TNodeShard::RegisterOperation(
    TOperationId operationId,
    TControllerEpoch controllerEpoch,
    const IOperationControllerPtr& controller,
    bool waitingForRevival)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    EmplaceOrCrash(
        IdToOperationState_,
        operationId,
        TOperationState(controller, waitingForRevival, ++CurrentEpoch_, controllerEpoch));

    WaitingForRegisterOperationIds_.erase(operationId);

    YT_LOG_DEBUG("Operation registered at node shard (OperationId: %v, WaitingForRevival: %v)",
        operationId,
        waitingForRevival);
}

void TNodeShard::StartOperationRevival(TOperationId operationId, TControllerEpoch newControllerEpoch)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto& operationState = GetOperationState(operationId);
    operationState.WaitingForRevival = true;
    operationState.ForbidNewAllocations = false;
    operationState.OperationUnreadyLoggedAllocationIds = THashSet<TAllocationId>();
    operationState.ControllerEpoch = newControllerEpoch;

    YT_LOG_DEBUG("Operation revival started at node shard (OperationId: %v, AllocationCount: %v, NewControllerEpoch: %v)",
        operationId,
        operationState.Allocations.size(),
        newControllerEpoch);

    auto allocations = operationState.Allocations;
    for (const auto& [allocationId, allocation] : allocations) {
        UnregisterAllocation(allocation, /*causedByRevival*/ true);
        AllocationsToSubmitToStrategy_.erase(allocationId);
    }

    for (const auto allocationId : operationState.AllocationsToSubmitToStrategy) {
        AllocationsToSubmitToStrategy_.erase(allocationId);
    }
    operationState.AllocationsToSubmitToStrategy.clear();

    RemoveOperationScheduleAllocationEntries(operationId);

    YT_VERIFY(operationState.Allocations.empty());
}

void TNodeShard::FinishOperationRevival(
    TOperationId operationId,
    const std::vector<TAllocationPtr>& allocations)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto& operationState = GetOperationState(operationId);

    YT_VERIFY(operationState.WaitingForRevival);
    operationState.WaitingForRevival = false;
    operationState.ForbidNewAllocations = false;
    operationState.ControllerTerminated = false;
    operationState.OperationUnreadyLoggedAllocationIds = THashSet<TAllocationId>();

    for (const auto& allocation : allocations) {
        auto node = GetOrRegisterNode(
            allocation->GetRevivalNodeId(),
            TNodeDescriptor(allocation->GetRevivalNodeAddress()),
            ENodeState::Online);
        allocation->SetNode(node);
        RegisterAllocation(allocation);
    }

    YT_LOG_DEBUG(
        "Operation revival finished at node shard (OperationId: %v, RevivedAllocationCount: %v)",
        operationId,
        allocations.size());
}

void TNodeShard::ResetOperationRevival(TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto& operationState = GetOperationState(operationId);

    operationState.WaitingForRevival = false;
    operationState.ForbidNewAllocations = false;
    operationState.ControllerTerminated = false;
    operationState.OperationUnreadyLoggedAllocationIds = THashSet<TAllocationId>();

    YT_LOG_DEBUG(
        "Operation revival state reset at node shard (OperationId: %v)",
        operationId);
}

void TNodeShard::UnregisterOperation(TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto it = IdToOperationState_.find(operationId);
    YT_VERIFY(it != IdToOperationState_.end());
    auto& operationState = it->second;

    for (const auto& allocation : operationState.Allocations) {
        YT_VERIFY(allocation.second->GetUnregistered());
    }

    for (const auto allocationId : operationState.AllocationsToSubmitToStrategy) {
        AllocationsToSubmitToStrategy_.erase(allocationId);
    }

    IdToOperationState_.erase(it);

    YT_LOG_DEBUG("Operation unregistered from node shard (OperationId: %v)",
        operationId);
}

void TNodeShard::UnregisterAndRemoveNodeById(TNodeId nodeId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto it = IdToNode_.find(nodeId);
    if (it != IdToNode_.end()) {
        const auto& node = it->second;
        UnregisterNode(node);
        RemoveNode(node);
    }
}

void TNodeShard::AbortAllocationsAtNode(TNodeId nodeId, EAbortReason reason)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto it = IdToNode_.find(nodeId);
    if (it != IdToNode_.end()) {
        const auto& node = it->second;
        AbortAllAllocationsAtNode(node, reason);
    }
}

void TNodeShard::ProcessHeartbeat(const TScheduler::TCtxNodeHeartbeatPtr& context)
{
    GetInvoker()->Invoke(
        BIND([=, this, this_ = MakeStrong(this)] {
            VERIFY_INVOKER_AFFINITY(GetInvoker());

            try {
                ValidateConnected();
                SwitchTo(CancelableInvoker_);

                DoProcessHeartbeat(context);
            } catch (const std::exception& ex) {
                context->Reply(TError(ex));
            }
        }));
}

void TNodeShard::DoProcessHeartbeat(const TScheduler::TCtxNodeHeartbeatPtr& context)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvoker_);

    const auto& strategy = ManagerHost_->GetStrategy();

    auto* request = &context->Request();
    auto* response = &context->Response();

    HeartbeatRequestProtoMessageBytes_.Increment(request->ByteSizeLong());

    int jobReporterWriteFailuresCount = 0;
    if (request->has_job_reporter_write_failures_count()) {
        jobReporterWriteFailuresCount = request->job_reporter_write_failures_count();
    }
    if (jobReporterWriteFailuresCount > 0) {
        JobReporterWriteFailuresCount_.fetch_add(jobReporterWriteFailuresCount, std::memory_order::relaxed);
    }

    auto nodeId = FromProto<TNodeId>(request->node_id());
    auto descriptor = FromProto<TNodeDescriptor>(request->node_descriptor());
    const auto& resourceLimits = request->resource_limits();
    const auto& resourceUsage = request->resource_usage();

    context->SetRequestInfo("NodeId: %v, NodeAddress: %v, ResourceUsage: %v, AllocationCount: %v",
        nodeId,
        descriptor.GetDefaultAddress(),
        ManagerHost_->FormatHeartbeatResourceUsage(
            ToJobResources(resourceUsage),
            ToJobResources(resourceLimits),
            request->disk_resources()
        ),
        request->allocations_size());

    YT_VERIFY(Host_->GetNodeShardId(nodeId) == Id_);

    auto node = GetOrRegisterNode(nodeId, descriptor, ENodeState::Online);

    if (request->has_job_reporter_queue_is_too_large()) {
        auto oldValue = node->GetJobReporterQueueIsTooLarge();
        auto newValue = request->job_reporter_queue_is_too_large();
        if (oldValue && !newValue) {
            --JobReporterQueueIsTooLargeNodeCount_;
        }
        if (!oldValue && newValue) {
            ++JobReporterQueueIsTooLargeNodeCount_;
        }
        YT_LOG_DEBUG_IF(newValue, "Job reporter queue is too large (NodeAddress: %v)", descriptor.GetDefaultAddress());
        node->SetJobReporterQueueIsTooLarge(newValue);
    }

    if (node->GetSchedulerState() == ENodeState::Online) {
        // NB: Resource limits and usage of node should be updated even if
        // node is offline at master to avoid getting incorrect total limits
        // when node becomes online.
        UpdateNodeResources(
            node,
            ToJobResources(request->resource_limits()),
            ToJobResources(request->resource_usage()),
            FromProto<TDiskResources>(request->disk_resources()));
    }

    TLeaseManager::RenewLease(node->GetHeartbeatLease(), Config_->NodeHeartbeatTimeout);
    TLeaseManager::RenewLease(node->GetRegistrationLease(), Config_->NodeRegistrationTimeout);

    auto strategyProxy = strategy->CreateNodeHeartbeatStrategyProxy(
        node->GetId(),
        node->GetDefaultAddress(),
        node->Tags(),
        node->GetMatchingTreeCookie());
    node->SetMatchingTreeCookie(strategyProxy->GetMatchingTreeCookie());

    bool isThrottlingActive = false;
    {
        // We need to prevent context switched between checking node state and BeginNodeHeartbeatProcessing.
        TForbidContextSwitchGuard guard;
        if (node->GetMasterState() != NNodeTrackerClient::ENodeState::Online || node->GetSchedulerState() != ENodeState::Online) {
            auto error = TError("Node is not online (MasterState: %v, SchedulerState: %v)",
                node->GetMasterState(),
                node->GetSchedulerState());
            if (!node->GetRegistrationError().IsOK()) {
                error = error << node->GetRegistrationError();
            }
            context->Reply(error);
            return;
        }

        // We should process only one heartbeat at a time from the same node.
        if (node->GetHasOngoingHeartbeat()) {
            context->Reply(TError("Node already has an ongoing heartbeat"));
            return;
        }

        if (Config_->UseHeartbeatSchedulingComplexityThrottling) {
            isThrottlingActive = IsHeartbeatThrottlingWithComplexity(node, strategyProxy);
        } else {
            isThrottlingActive = IsHeartbeatThrottlingWithCount(node);
            node->SetSchedulingHeartbeatComplexity(1);
        }

        response->set_operations_archive_version(ManagerHost_->GetOperationsArchiveVersion());

        BeginNodeHeartbeatProcessing(node);
    }
    auto finallyGuard = Finally([&, this, cancelableContext = CancelableContext_] {
        if (!cancelableContext->IsCanceled()) {
            EndNodeHeartbeatProcessing(node);
        }

        HeartbeatResponseProtoMessageBytes_.Increment(response->ByteSizeLong());
    });

    if (resourceLimits.user_slots()) {
        MaybeDelay(Config_->TestingOptions->NodeHeartbeatProcessingDelay);
    }

    std::vector<TAllocationPtr> runningAllocations;
    bool hasWaitingAllocations = false;
    YT_PROFILE_TIMING("/scheduler/analysis_time") {
        ProcessHeartbeatAllocations(
            request,
            response,
            node,
            strategyProxy,
            &runningAllocations,
            &hasWaitingAllocations);
    }

    bool skipScheduleAllocations = false;
    if (hasWaitingAllocations || isThrottlingActive) {
        if (hasWaitingAllocations) {
            YT_LOG_DEBUG(
                "Waiting allocations found, suppressing new allocations scheduling (NodeAddress: %v)",
                node->GetDefaultAddress());
        }
        if (isThrottlingActive) {
            YT_LOG_DEBUG(
                "Throttling is active, suppressing new allocations scheduling (NodeAddress: %v)",
                node->GetDefaultAddress());
        }
        skipScheduleAllocations = true;
    }

    response->set_scheduling_skipped(skipScheduleAllocations);

    if (Config_->EnableAllocationAbortOnZeroUserSlots && node->GetResourceLimits().GetUserSlots() == 0) {
        // Abort all allocations on node immediately, if it has no user slots.
        // Make a copy, the collection will be modified.
        auto allocations = node->Allocations();
        const auto& address = node->GetDefaultAddress();

        auto error = TError("Node without user slots")
            << TErrorAttribute("abort_reason", EAbortReason::NodeWithDisabledJobs);
        for (const auto& allocation : allocations) {
            YT_LOG_DEBUG(
                "Aborting allocation on node without user slots (Address: %v, AllocationId: %v, OperationId: %v)",
                address,
                allocation->GetId(),
                allocation->GetOperationId());

            OnAllocationAborted(allocation, error, EAbortReason::NodeWithDisabledJobs);
        }
    }

    SubmitAllocationsToStrategy();

    auto mediumDirectory = Bootstrap_
        ->GetClient()
        ->GetNativeConnection()
        ->GetMediumDirectory();
    auto schedulingContext = CreateSchedulingContext(
        Id_,
        Config_,
        node,
        runningAllocations,
        mediumDirectory);

    Y_UNUSED(WaitFor(strategyProxy->ProcessSchedulingHeartbeat(schedulingContext, skipScheduleAllocations)));

    ProcessScheduledAndPreemptedAllocations(
        schedulingContext,
        response);

    ProcessOperationInfoHeartbeat(request, response);
    SetMinSpareResources(response);

    auto now = TInstant::Now();
    auto shouldSendRegisteredControllerAgents = ShouldSendRegisteredControllerAgents(request);
    response->set_registered_controller_agents_sent(shouldSendRegisteredControllerAgents);
    if (shouldSendRegisteredControllerAgents) {
        AddRegisteredControllerAgentsToResponse(response);
        node->SetLastRegisteredControllerAgentsSentTime(now);
    }

    context->SetResponseInfo(
        "NodeId: %v, NodeAddress: %v, HeartbeatComplexity: %v, TotalComplexity: %v, IsThrottling: %v, SendRegisteredControllerAgents: %v",
        nodeId,
        descriptor.GetDefaultAddress(),
        node->GetSchedulingHeartbeatComplexity(),
        ConcurrentHeartbeatComplexity_.load(),
        isThrottlingActive,
        shouldSendRegisteredControllerAgents);

    TStringBuilder schedulingAttributesBuilder;
    TDelimitedStringBuilderWrapper delimitedSchedulingAttributesBuilder(&schedulingAttributesBuilder);
    strategyProxy->BuildSchedulingAttributesString(delimitedSchedulingAttributesBuilder);
    context->SetRawResponseInfo(schedulingAttributesBuilder.Flush(), /*incremental*/ true);

    if (!skipScheduleAllocations) {
        HeartbeatWithScheduleAllocationsCounter_.Increment();

        node->SetResourceUsage(schedulingContext->ResourceUsage());

        const auto& statistics = schedulingContext->GetSchedulingStatistics();
        if (statistics.ScheduleWithPreemption) {
            node->SetLastPreemptiveHeartbeatStatistics(statistics);
        } else {
            node->SetLastNonPreemptiveHeartbeatStatistics(statistics);
        }

        // NB: Some allocations maybe considered aborted after processing scheduled allocations.
        SubmitAllocationsToStrategy();

        // TODO(eshcherbin): It's possible to shorten this message by writing preemptible info
        // only when preemptive scheduling has been attempted.
        context->SetIncrementalResponseInfo(
            "StartedAllocations: {All: %v, ByPreemption: %v}, PreemptedAllocations: %v, "
            "PreemptibleInfo: %v, SsdPriorityPreemption: {Enabled: %v, Media: %v}, "
            "ScheduleAllocationAttempts: %v, OperationCountByPreemptionPriority: %v",
            schedulingContext->StartedAllocations().size(),
            statistics.ScheduledDuringPreemption,
            schedulingContext->PreemptedAllocations().size(),
            FormatPreemptibleInfoCompact(statistics),
            statistics.SsdPriorityPreemptionEnabled,
            statistics.SsdPriorityPreemptionMedia,
            FormatScheduleAllocationAttemptsCompact(statistics),
            FormatOperationCountByPreemptionPriorityCompact(statistics.OperationCountByPreemptionPriority));
    } else {
        context->SetIncrementalResponseInfo("PreemptedAllocations: %v", schedulingContext->PreemptedAllocations().size());
    }

    context->Reply();
}

TRefCountedExecNodeDescriptorMapPtr TNodeShard::GetExecNodeDescriptors()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    UpdateExecNodeDescriptors();

    return CachedExecNodeDescriptors_.Acquire();
}

void TNodeShard::UpdateExecNodeDescriptors()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto now = TInstant::Now();

    std::vector<TExecNodePtr> nodesToRemove;

    auto result = New<TRefCountedExecNodeDescriptorMap>();
    result->reserve(IdToNode_.size());
    for (const auto& [nodeId, node] : IdToNode_) {
        if (node->GetLastSeenTime() + Config_->MaxOfflineNodeAge > now) {
            YT_VERIFY(result->emplace(nodeId, node->BuildExecDescriptor()).second);
        } else {
            if (node->GetMasterState() != NNodeTrackerClient::ENodeState::Online && node->GetSchedulerState() == ENodeState::Offline) {
                nodesToRemove.push_back(node);
            }
        }
    }

    for (const auto& node : nodesToRemove) {
        YT_LOG_INFO("Node has not seen more than %v seconds, remove it (NodeId: %v, Address: %v)",
            Config_->MaxOfflineNodeAge,
            node->GetId(),
            node->GetDefaultAddress());
        UnregisterNode(node);
        RemoveNode(node);
    }

    CachedExecNodeDescriptors_.Store(std::move(result));
}

void TNodeShard::UpdateNodeState(
    const TExecNodePtr& node,
    NNodeTrackerClient::ENodeState newMasterState,
    ENodeState newSchedulerState,
    const TError& error)
{
    auto oldMasterState = node->GetMasterState();
    node->SetMasterState(newMasterState);

    auto oldSchedulerState = node->GetSchedulerState();
    node->SetSchedulerState(newSchedulerState);

    node->SetRegistrationError(error);

    if (oldMasterState != newMasterState || oldSchedulerState != newSchedulerState) {
        YT_LOG_INFO("Node state changed (NodeId: %v, NodeAddress: %v, MasterState: %v -> %v, SchedulerState: %v -> %v)",
            node->GetId(),
            node->NodeDescriptor().GetDefaultAddress(),
            oldMasterState,
            newMasterState,
            oldSchedulerState,
            newSchedulerState);
    }
}

void TNodeShard::RemoveOperationScheduleAllocationEntries(const TOperationId operationId)
{
    auto range = OperationIdToAllocationIterators_.equal_range(operationId);
    for (auto it = range.first; it != range.second; ++it) {
        AllocationIdToScheduleEntry_.erase(it->second);
    }
    OperationIdToAllocationIterators_.erase(operationId);
}

void TNodeShard::RemoveMissingNodes(const std::vector<TString>& nodeAddresses)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    if (!Connected_) {
        return;
    }

    auto nodeAddressesSet = THashSet<TString>(nodeAddresses.begin(), nodeAddresses.end());

    std::vector<TExecNodePtr> nodesToUnregister;
    for (const auto& [id, node] : IdToNode_) {
        auto it = nodeAddressesSet.find(node->GetDefaultAddress());
        if (it == nodeAddressesSet.end()) {
            nodesToUnregister.push_back(node);
        }
    }

    for (const auto& node : nodesToUnregister) {
        YT_LOG_DEBUG(
            "Node is not found at master, unregister and remove it "
            "(NodeId: %v, NodeShardId: %v, Address: %v)",
            node->GetId(),
            Id_,
            node->GetDefaultAddress());
        UnregisterNode(node);
        RemoveNode(node);
    }
}

std::vector<TError> TNodeShard::HandleNodesAttributes(const std::vector<std::pair<TString, INodePtr>>& nodeMaps)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    if (!Connected_) {
        return {};
    }

    auto now = TInstant::Now();

    if (HasOngoingNodesAttributesUpdate_) {
        auto error = TError("Node shard is handling nodes attributes update for too long, skipping new update");
        YT_LOG_WARNING(error);
        return {error};
    }

    HasOngoingNodesAttributesUpdate_ = true;
    auto finallyGuard = Finally([&] { HasOngoingNodesAttributesUpdate_ = false; });

    int nodeChangesCount = 0;
    std::vector<TError> errors;

    for (const auto& nodeMap : nodeMaps) {
        const auto& address = nodeMap.first;
        const auto& attributes = nodeMap.second->Attributes();
        auto objectId = attributes.Get<TObjectId>("id");
        auto nodeId = NodeIdFromObjectId(objectId);
        auto newState = attributes.Get<NNodeTrackerClient::ENodeState>("state");
        auto ioWeights = attributes.Get<THashMap<TString, double>>("io_weights", {});
        auto annotationsYson = attributes.FindYson("annotations");
        auto schedulingOptionsYson = attributes.FindYson("scheduling_options");

        YT_LOG_DEBUG("Handling node attributes (NodeId: %v, NodeAddress: %v, ObjectId: %v, NewState: %v)",
            nodeId,
            address,
            objectId,
            newState);

        YT_VERIFY(Host_->GetNodeShardId(nodeId) == Id_);

        if (!IdToNode_.contains(nodeId)) {
            if (newState != NNodeTrackerClient::ENodeState::Offline) {
                RegisterNode(nodeId, TNodeDescriptor(address), ENodeState::Offline);
            } else {
                // Skip nodes that offline both at master and at scheduler.
                YT_LOG_DEBUG("Skipping node since it is offline both at scheduler and at master (NodeId: %v, NodeAddress: %v)",
                    nodeId,
                    address);
                continue;
            }
        }

        auto execNode = IdToNode_[nodeId];

        if (execNode->GetSchedulerState() == ENodeState::Offline &&
            newState == NNodeTrackerClient::ENodeState::Online &&
            execNode->GetRegistrationError().IsOK())
        {
            YT_LOG_INFO("Node is not registered at scheduler but online at master (NodeId: %v, NodeAddress: %v)",
                nodeId,
                address);
        }

        if (newState == NNodeTrackerClient::ENodeState::Online) {
            TLeaseManager::RenewLease(execNode->GetRegistrationLease(), Config_->NodeRegistrationTimeout);
            if (execNode->GetSchedulerState() == ENodeState::Offline &&
                execNode->GetLastSeenTime() + Config_->MaxNodeUnseenPeriodToAbortAllocations < now)
            {
                AbortAllAllocationsAtNode(execNode, EAbortReason::NodeOffline);
            }
        }

        execNode->SetIOWeights(ioWeights);

        execNode->SetSchedulingOptions(schedulingOptionsYson ? ConvertToAttributes(schedulingOptionsYson) : nullptr);

        static const TString InfinibandClusterAnnotationsPath = "/" + InfinibandClusterNameKey;
        auto infinibandCluster = annotationsYson
            ? TryGetString(annotationsYson.AsStringBuf(), InfinibandClusterAnnotationsPath)
            : std::nullopt;
        if (auto nodeInfinibandCluster = execNode->GetInfinibandCluster()) {
            YT_LOG_WARNING_IF(nodeInfinibandCluster != infinibandCluster,
                "Node's infiniband cluster tag has changed "
                "(NodeAddress: %v, OldInfinibandCluster: %v, NewInfinibandCluster: %v)",
                address,
                nodeInfinibandCluster,
                infinibandCluster);
        }
        execNode->SetInfinibandCluster(std::move(infinibandCluster));

        auto oldState = execNode->GetMasterState();
        auto tags = TBooleanFormulaTags(attributes.Get<THashSet<TString>>("tags"));

        if (oldState == NNodeTrackerClient::ENodeState::Online && newState != NNodeTrackerClient::ENodeState::Online) {
            // NOTE: Tags will be validated when node become online, no need in additional check here.
            execNode->SetTags(std::move(tags));
            SubtractNodeResources(execNode);
            // State change must happen before aborting allocations.
            UpdateNodeState(execNode, newState, execNode->GetSchedulerState());

            AbortAllAllocationsAtNode(execNode, EAbortReason::NodeOffline);
            ++nodeChangesCount;
            continue;
        } else if (oldState != newState) {
            UpdateNodeState(execNode, newState, execNode->GetSchedulerState());
        }

        if ((oldState != NNodeTrackerClient::ENodeState::Online && newState == NNodeTrackerClient::ENodeState::Online) || execNode->Tags() != tags || !execNode->GetRegistrationError().IsOK()) {
            auto updateResult = WaitFor(ManagerHost_->GetStrategy()->RegisterOrUpdateNode(nodeId, address, tags));
            if (!updateResult.IsOK()) {
                auto error = TError("Node tags update failed")
                    << TErrorAttribute("node_id", nodeId)
                    << TErrorAttribute("address", address)
                    << TErrorAttribute("tags", tags)
                    << updateResult;
                YT_LOG_WARNING(error);
                errors.push_back(error);

                // State change must happen before aborting allocations.
                auto previousSchedulerState = execNode->GetSchedulerState();
                UpdateNodeState(execNode, /*newMasterState*/ newState, /*newSchedulerState*/ ENodeState::Offline, error);
                if (oldState == NNodeTrackerClient::ENodeState::Online && previousSchedulerState == ENodeState::Online) {
                    SubtractNodeResources(execNode);

                    AbortAllAllocationsAtNode(execNode, EAbortReason::NodeOffline);
                }
            } else {
                if (oldState != NNodeTrackerClient::ENodeState::Online && newState == NNodeTrackerClient::ENodeState::Online) {
                    AddNodeResources(execNode);
                }
                execNode->SetTags(std::move(tags));
                UpdateNodeState(execNode, /*newMasterState*/ newState, /*newSchedulerState*/ execNode->GetSchedulerState());
            }
            ++nodeChangesCount;
        }
    }

    if (nodeChangesCount > Config_->NodeChangesCountThresholdToUpdateCache) {
        UpdateExecNodeDescriptors();
        CachedResourceStatisticsByTags_->Clear();
    }

    return errors;
}

void TNodeShard::AbortOperationAllocations(
    TOperationId operationId,
    const TError& abortError,
    EAbortReason abortReason,
    bool controllerTerminated)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    if (controllerTerminated) {
        RemoveOperationScheduleAllocationEntries(operationId);
    }

    auto* operationState = FindOperationState(operationId);
    if (!operationState) {
        return;
    }

    operationState->ControllerTerminated = controllerTerminated;
    operationState->ForbidNewAllocations = true;
    auto allocations = operationState->Allocations;
    for (const auto& [allocationId, allocation] : allocations) {
        YT_LOG_DEBUG(abortError, "Aborting allocation (AllocationId: %v, OperationId: %v)", allocationId, operationId);
        OnAllocationAborted(allocation, abortError, abortReason);
    }

    for (const auto& allocation : operationState->Allocations) {
        YT_VERIFY(allocation.second->GetUnregistered());
    }
}

void TNodeShard::ResumeOperationAllocations(TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto* operationState = FindOperationState(operationId);
    if (!operationState || operationState->ControllerTerminated) {
        return;
    }

    operationState->ForbidNewAllocations = false;
}

TNodeDescriptor TNodeShard::GetAllocationNode(TAllocationId allocationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    const auto& allocation = FindAllocation(allocationId);

    if (allocation) {
        return allocation->GetNode()->NodeDescriptor();
    } else {
        auto node = FindNodeByAllocation(allocationId);
        if (!node) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::NoSuchAllocation,
                "Allocation %v not found",
                allocationId);
        }

        return node->NodeDescriptor();
    }
}

TAllocationDescription TNodeShard::GetAllocationDescription(TAllocationId allocationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    TAllocationDescription result;

    result.NodeId = NodeIdFromAllocationId(allocationId);

    if (const auto& node = FindNodeByAllocation(allocationId)) {
        result.NodeAddress = node->GetDefaultAddress();
    }

    if (const auto& allocation = FindAllocation(allocationId)) {
        result.Running = true;
        result.Properties = TAllocationDescription::TAllocationProperties{
            .OperationId = allocation->GetOperationId(),
            .StartTime = allocation->GetStartTime(),
            .State = allocation->GetState(),
            .TreeId = allocation->GetTreeId(),
            .Preempted = allocation->GetPreempted(),
            .PreemptionReason = allocation->GetPreemptionReason(),
            .PreemptionTimeout = CpuDurationToDuration(allocation->GetPreemptionTimeout()),
            .PreemptibleProgressTime = allocation->PreemptibleProgressTime(),
        };
    } else {
        result.Running = false;
    }

    return result;
}

void TNodeShard::AbortAllocation(TAllocationId allocationId, const TError& error, EAbortReason abortReason)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto allocation = FindAllocation(allocationId);
    if (!allocation) {
        YT_LOG_DEBUG(error, "Requested to abort an unknown allocation, ignored (AllocationId: %v)", allocationId);
        return;
    }

    YT_LOG_DEBUG(
        error,
        "Aborting allocation by internal request (AllocationId: %v, OperationId: %v, AbortReason: %v)",
        allocationId,
        allocation->GetOperationId(),
        abortReason);

    OnAllocationAborted(allocation, error, abortReason);
}

void TNodeShard::AbortAllocations(const std::vector<TAllocationId>& allocationIds, const TError& error, EAbortReason abortReason)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    for (auto allocationId : allocationIds) {
        AbortAllocation(allocationId, error, abortReason);
    }
}

TNodeYsonList TNodeShard::BuildNodeYsonList() const
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    TNodeYsonList nodeYsons;
    nodeYsons.reserve(std::ssize(IdToNode_));
    for (const auto& [nodeId, node] : IdToNode_) {
        nodeYsons.emplace_back(nodeId, BuildNodeYson(node));
    }

    return nodeYsons;
}

TOperationId TNodeShard::FindOperationIdByAllocationId(TAllocationId allocationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    const auto& allocation = FindAllocation(allocationId);
    if (allocation) {
        return allocation->GetOperationId();
    }

    return {};
}

TNodeShard::TResourceStatistics TNodeShard::CalculateResourceStatistics(const TSchedulingTagFilter& filter)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto descriptors = CachedExecNodeDescriptors_.Acquire();

    TResourceStatistics statistics;
    for (const auto& [nodeId, descriptor] : *descriptors) {
        if (descriptor->Online && descriptor->CanSchedule(filter)) {
            statistics.Usage += descriptor->ResourceUsage;
            statistics.Limits += descriptor->ResourceLimits;
        }
    }
    return statistics;
}

TJobResources TNodeShard::GetResourceLimits(const TSchedulingTagFilter& filter) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CachedResourceStatisticsByTags_->Get(filter).Limits;
}

TJobResources TNodeShard::GetResourceUsage(const TSchedulingTagFilter& filter) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CachedResourceStatisticsByTags_->Get(filter).Usage;
}

int TNodeShard::GetActiveAllocationCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ActiveAllocationCount_;
}

int TNodeShard::GetSubmitToStrategyAllocationCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return SubmitToStrategyAllocationCount_;
}

int TNodeShard::GetExecNodeCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ExecNodeCount_;
}

int TNodeShard::GetTotalNodeCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return TotalNodeCount_;
}

int TNodeShard::GetTotalConcurrentHeartbeatComplexity() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ConcurrentHeartbeatComplexity_;
}

TFuture<TControllerScheduleAllocationResultPtr> TNodeShard::BeginScheduleAllocation(
    TIncarnationId incarnationId,
    TOperationId operationId,
    TAllocationId allocationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto pair = AllocationIdToScheduleEntry_.emplace(allocationId, TScheduleAllocationEntry());
    YT_VERIFY(pair.second);

    auto& entry = pair.first->second;
    entry.Promise = NewPromise<TControllerScheduleAllocationResultPtr>();
    entry.IncarnationId = incarnationId;
    entry.OperationId = operationId;
    entry.OperationIdToAllocationIdsIterator = OperationIdToAllocationIterators_.emplace(operationId, pair.first);
    entry.StartTime = GetCpuInstant();

    return entry.Promise.ToFuture();
}

void TNodeShard::EndScheduleAllocation(const NProto::TScheduleAllocationResponse& response)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto allocationId = FromProto<TAllocationId>(response.allocation_id());
    auto operationId = FromProto<TOperationId>(response.operation_id());

    auto it = AllocationIdToScheduleEntry_.find(allocationId);
    if (it == std::end(AllocationIdToScheduleEntry_)) {
        YT_LOG_WARNING(
            "No schedule entry for allocation, probably allocation was scheduled by controller too late (OperationId: %v, AllocationId: %v)",
            operationId,
            allocationId);
        return;
    }

    auto& entry = it->second;
    YT_VERIFY(operationId == entry.OperationId);

    auto scheduleAllocationDuration = CpuDurationToDuration(GetCpuInstant() - entry.StartTime);
    if (scheduleAllocationDuration > Config_->ScheduleAllocationDurationLoggingThreshold) {
        YT_LOG_DEBUG(
            "Allocation schedule response received (OperationId: %v, AllocationId: %v, Success: %v, Duration: %v)",
            operationId,
            allocationId,
            response.success(),
            scheduleAllocationDuration.MilliSeconds());
    }

    auto result = New<TControllerScheduleAllocationResult>();
    if (response.success()) {
        result->StartDescriptor.emplace(
            allocationId,
            FromProto<TJobResourcesWithQuota>(response.resource_limits()));
    }
    for (const auto& protoCounter : response.failed()) {
        result->Failed[static_cast<EScheduleAllocationFailReason>(protoCounter.reason())] = protoCounter.value();
    }
    FromProto(&result->Duration, response.duration());
    if (response.has_next_duration_estimate()) {
        result->NextDurationEstimate = FromProto<TDuration>(response.next_duration_estimate());
    }
    result->IncarnationId = entry.IncarnationId;
    result->ControllerEpoch = TControllerEpoch(response.controller_epoch());

    entry.Promise.Set(std::move(result));

    OperationIdToAllocationIterators_.erase(entry.OperationIdToAllocationIdsIterator);
    AllocationIdToScheduleEntry_.erase(it);
}

void TNodeShard::RemoveOutdatedScheduleAllocationEntries()
{
    std::vector<TAllocationId> allocationIdsToRemove;
    auto now = TInstant::Now();
    for (const auto& [allocationId, entry] : AllocationIdToScheduleEntry_) {
        if (CpuInstantToInstant(entry.StartTime) + Config_->ScheduleAllocationEntryRemovalTimeout < now) {
            allocationIdsToRemove.push_back(allocationId);
        }
    }

    for (auto allocationId : allocationIdsToRemove) {
        auto it = AllocationIdToScheduleEntry_.find(allocationId);
        if (it == std::end(AllocationIdToScheduleEntry_)) {
            return;
        }

        auto& entry = it->second;

        OperationIdToAllocationIterators_.erase(entry.OperationIdToAllocationIdsIterator);
        AllocationIdToScheduleEntry_.erase(it);
    }
}

int TNodeShard::ExtractJobReporterWriteFailuresCount()
{
    return JobReporterWriteFailuresCount_.exchange(0);
}

int TNodeShard::GetJobReporterQueueIsTooLargeNodeCount()
{
    return JobReporterQueueIsTooLargeNodeCount_.load();
}

TControllerEpoch TNodeShard::GetOperationControllerEpoch(TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    if (auto* operationState = FindOperationState(operationId)) {
        return operationState->ControllerEpoch;
    } else {
        return InvalidControllerEpoch;
    }
}

TControllerEpoch TNodeShard::GetAllocationControllerEpoch(TAllocationId allocationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    if (auto allocation = FindAllocation(allocationId)) {
        return allocation->GetControllerEpoch();
    } else {
        return InvalidControllerEpoch;
    }
}

bool TNodeShard::IsOperationControllerTerminated(const TOperationId operationId) const noexcept
{
    const auto statePtr = FindOperationState(operationId);
    return !statePtr || statePtr->ControllerTerminated;
}

bool TNodeShard::IsOperationRegistered(const TOperationId operationId) const noexcept
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    return FindOperationState(operationId);
}

bool TNodeShard::AreNewAllocationsForbiddenForOperation(const TOperationId operationId) const noexcept
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    const auto& operationState = GetOperationState(operationId);
    return operationState.ForbidNewAllocations;
}

int TNodeShard::GetOnGoingHeartbeatCount() const noexcept
{
    return ConcurrentHeartbeatCount_;
}

void TNodeShard::RegisterAgent(
    TAgentId id,
    NNodeTrackerClient::TAddressMap addresses,
    TIncarnationId incarnationId)
{
    YT_LOG_DEBUG("Agent registered at node shard (AgentId: %v, IncarnationId: %v)", id, incarnationId);

    EmplaceOrCrash(
        RegisteredAgents_,
        std::move(id),
        TControllerAgentInfo{std::move(addresses), incarnationId});
    InsertOrCrash(
        RegisteredAgentIncarnationIds_,
        incarnationId);
}

void TNodeShard::UnregisterAgent(TAgentId id)
{
    YT_LOG_DEBUG("Agent unregistered from node shard (AgentId: %v)", id);

    auto it = GetIteratorOrCrash(RegisteredAgents_, id);
    const auto& info = it->second;

    EraseOrCrash(RegisteredAgentIncarnationIds_, info.IncarnationId);
    RegisteredAgents_.erase(it);
}

TExecNodePtr TNodeShard::GetOrRegisterNode(TNodeId nodeId, const TNodeDescriptor& descriptor, ENodeState state)
{
    TExecNodePtr node;

    auto it = IdToNode_.find(nodeId);
    if (it == IdToNode_.end()) {
        node = RegisterNode(nodeId, descriptor, state);
    } else {
        node = it->second;

        // Update the current descriptor and state, just in case.
        node->NodeDescriptor() = descriptor;

        // Update state to online only if node has no registration errors.
        if (state != ENodeState::Online || node->GetRegistrationError().IsOK()) {
            node->SetSchedulerState(state);
        }
    }

    return node;
}

void TNodeShard::UpdateAllocationTimeStatisticsIfNeeded(const TAllocationPtr& allocation, TRunningAllocationTimeStatistics timeStatistics)
{
    if (allocation->GetPreemptibleProgressStartTime() < timeStatistics.PreemptibleProgressStartTime) {
        allocation->SetPreemptibleProgressStartTime(timeStatistics.PreemptibleProgressStartTime);
    }
}

void TNodeShard::UpdateRunningAllocationsStatistics(const std::vector<TRunningAllocationStatisticsUpdate>& updates)
{
    YT_LOG_DEBUG("Update running allocation time statistics (UpdateCount: %v)", std::size(updates));

    for (auto [allocationId, timeStatistics] : updates) {
        if (const auto& allocation = FindAllocation(allocationId)) {
            UpdateAllocationTimeStatisticsIfNeeded(allocation, timeStatistics);
        }
    }
}

void TNodeShard::OnNodeRegistrationLeaseExpired(TNodeId nodeId)
{
    auto it = IdToNode_.find(nodeId);
    if (it == IdToNode_.end()) {
        return;
    }
    auto node = it->second;

    YT_LOG_INFO("Node lease expired, unregistering (Address: %v)",
        node->GetDefaultAddress());

    UnregisterNode(node);

    auto lease = TLeaseManager::CreateLease(
        Config_->NodeRegistrationTimeout,
        BIND(&TNodeShard::OnNodeRegistrationLeaseExpired, MakeWeak(this), node->GetId())
            .Via(GetInvoker()));
    node->SetRegistrationLease(lease);
}

void TNodeShard::OnNodeHeartbeatLeaseExpired(TNodeId nodeId)
{
    auto it = IdToNode_.find(nodeId);
    if (it == IdToNode_.end()) {
        return;
    }
    auto node = it->second;

    // We intentionally do not abort allocations here, it will happen when RegistrationLease expired or
    // at node attributes update by separate timeout.
    UpdateNodeState(node, /*newMasterState*/ node->GetMasterState(), /*newSchedulerState*/ ENodeState::Offline);

    auto lease = TLeaseManager::CreateLease(
        Config_->NodeHeartbeatTimeout,
        BIND(&TNodeShard::OnNodeHeartbeatLeaseExpired, MakeWeak(this), node->GetId())
            .Via(GetInvoker()));
    node->SetHeartbeatLease(lease);
}

TExecNodePtr TNodeShard::RegisterNode(TNodeId nodeId, const TNodeDescriptor& descriptor, ENodeState state)
{
    auto node = New<TExecNode>(nodeId, descriptor, state);
    const auto& address = node->GetDefaultAddress();

    {
        auto lease = TLeaseManager::CreateLease(
            Config_->NodeRegistrationTimeout,
            BIND(&TNodeShard::OnNodeRegistrationLeaseExpired, MakeWeak(this), node->GetId())
                .Via(GetInvoker()));
        node->SetRegistrationLease(lease);
    }

    {
        auto lease = TLeaseManager::CreateLease(
            Config_->NodeHeartbeatTimeout,
            BIND(&TNodeShard::OnNodeHeartbeatLeaseExpired, MakeWeak(this), node->GetId())
                .Via(GetInvoker()));
        node->SetHeartbeatLease(lease);
    }

    EmplaceOrCrash(IdToNode_, node->GetId(), node);

    node->SetLastSeenTime(TInstant::Now());

    YT_LOG_INFO("Node registered (Address: %v)", address);

    return node;
}

void TNodeShard::RemoveNode(TExecNodePtr node)
{
    TLeaseManager::CloseLease(node->GetRegistrationLease());
    TLeaseManager::CloseLease(node->GetHeartbeatLease());

    IdToNode_.erase(node->GetId());

    YT_LOG_INFO("Node removed (NodeId: %v, Address: %v, NodeShardId: %v)",
        node->GetId(),
        node->GetDefaultAddress(),
        Id_);
}

void TNodeShard::UnregisterNode(const TExecNodePtr& node)
{
    if (node->GetHasOngoingHeartbeat()) {
        YT_LOG_INFO("Node unregistration postponed until heartbeat is finished (Address: %v)",
            node->GetDefaultAddress());
        node->SetHasPendingUnregistration(true);
    } else {
        DoUnregisterNode(node);
    }
}

void TNodeShard::DoUnregisterNode(const TExecNodePtr& node)
{
    if (node->GetMasterState() == NNodeTrackerClient::ENodeState::Online && node->GetSchedulerState() == ENodeState::Online) {
        SubtractNodeResources(node);
    }

    AbortAllAllocationsAtNode(node, EAbortReason::NodeOffline);

    if (node->GetJobReporterQueueIsTooLarge()) {
        --JobReporterQueueIsTooLargeNodeCount_;
    }

    node->SetSchedulerState(ENodeState::Offline);

    const auto& address = node->GetDefaultAddress();

    ManagerHost_->GetStrategy()->UnregisterNode(node->GetId(), address);

    YT_LOG_INFO("Node unregistered (Address: %v)", address);
}

void TNodeShard::AbortAllAllocationsAtNode(const TExecNodePtr& node, EAbortReason reason)
{
    if (node->GetHasOngoingHeartbeat()) {
        YT_LOG_INFO("Allocations abortion postponed until heartbeat is finished (Address: %v)",
            node->GetDefaultAddress());
        node->SetPendingAllocationsAbortionReason(reason);
    } else {
        DoAbortAllAllocationsAtNode(node, reason);
    }
}

void TNodeShard::DoAbortAllAllocationsAtNode(const TExecNodePtr& node, EAbortReason reason)
{
    std::vector<TAllocationId> allocationIds;
    for (const auto& allocation : node->Allocations()) {
        allocationIds.push_back(allocation->GetId());
    }
    const auto& address = node->GetDefaultAddress();
    YT_LOG_DEBUG(
        "Aborting all allocations on a node (Address: %v, Reason: %v, AllocationIds: %v)",
        address,
        reason,
        allocationIds);

    // Make a copy, the collection will be modified.
    auto allocations = node->Allocations();
    auto error = TError("All allocations on the node were aborted by scheduler")
        << TErrorAttribute("abort_reason", reason);
    for (const auto& allocation : allocations) {
        OnAllocationAborted(allocation, error, reason);
    }
}

// TODO(eshcherbin): This method has become too big -- gotta split it.
void TNodeShard::ProcessHeartbeatAllocations(
    TScheduler::TCtxNodeHeartbeat::TTypedRequest* request,
    TScheduler::TCtxNodeHeartbeat::TTypedResponse* response,
    const TExecNodePtr& node,
    const INodeHeartbeatStrategyProxyPtr& strategyProxy,
    std::vector<TAllocationPtr>* runningAllocations,
    bool* hasWaitingAllocations)
{
    YT_VERIFY(runningAllocations->empty());

    auto now = GetCpuInstant();

    bool shouldLogOngoingAllocations = false;
    auto lastAllocationsLogTime = node->GetLastAllocationsLogTime();
    if (!lastAllocationsLogTime || now > *lastAllocationsLogTime + DurationToCpuDuration(Config_->AllocationsLoggingPeriod)) {
        shouldLogOngoingAllocations = true;
        node->SetLastAllocationsLogTime(now);
    }

    bool checkMissingAllocations = false;
    auto lastCheckMissingAllocationsTime = node->GetLastCheckMissingAllocationsTime();
    if ((!lastCheckMissingAllocationsTime ||
        now > *lastCheckMissingAllocationsTime + DurationToCpuDuration(Config_->MissingAllocationsCheckPeriod)))
    {
        checkMissingAllocations = true;
        node->SetLastCheckMissingAllocationsTime(now);
    }

    if (checkMissingAllocations) {
        for (const auto& allocation : node->Allocations()) {
            // Verify that all flags are in the initial state.
            YT_VERIFY(!allocation->GetFoundOnNode());
        }
    }

    // Used for debug logging.
    TStateToAllocationList ongoingAllocationsByState;
    for (auto& allocationStatus : *request->mutable_allocations()) {
        auto allocationId = FromProto<TAllocationId>(allocationStatus.allocation_id());

        TAllocationPtr allocation;
        try {
            allocation = ProcessAllocationHeartbeat(
                node,
                response,
                &allocationStatus);
        } catch (const std::exception& ex) {
            if (Config_->CrashOnAllocationHeartbeatProcessingException) {
                YT_LOG_FATAL(ex, "Failed to process allocation heartbeat (AllocationId: %v)", allocationId);
            } else {
                YT_LOG_WARNING(ex, "Failed to process allocation heartbeat (AllocationId: %v)", allocationId);
                throw;
            }
        }

        if (allocation) {
            if (checkMissingAllocations) {
                allocation->SetFoundOnNode(true);
            }
            switch (allocation->GetState()) {
                case EAllocationState::Running: {
                    runningAllocations->push_back(allocation);
                    ongoingAllocationsByState[allocation->GetState()].push_back(allocation);
                    break;
                }
                case EAllocationState::Waiting:
                    *hasWaitingAllocations = true;
                    ongoingAllocationsByState[allocation->GetState()].push_back(allocation);
                    break;
                default:
                    break;
            }
        }
    }
    HeartbeatAllocationCount_.Increment(request->allocations_size());
    HeartbeatCount_.Increment();

    if (shouldLogOngoingAllocations) {
        LogOngoingAllocationsOnHeartbeat(strategyProxy, CpuInstantToInstant(now), ongoingAllocationsByState);
    }

    if (checkMissingAllocations) {
        std::vector<TAllocationPtr> missingAllocations;
        for (const auto& allocation : node->Allocations()) {
            if (!allocation->GetFoundOnNode()) {
                // This situation is possible if heartbeat from node has timed out,
                // but we have scheduled some allocations.
                // TODO(ignat):  YT-15875: consider deadline from node.
                YT_LOG_INFO(
                    "Allocation is disappeared from node (Address: %v, AllocationId: %v, OperationId: %v)",
                    node->GetDefaultAddress(),
                    allocation->GetId(),
                    allocation->GetOperationId());
                missingAllocations.push_back(allocation);
            } else {
                allocation->SetFoundOnNode(false);
            }
        }

        auto error = TError("Allocation disappeared from node")
            << TErrorAttribute("abort_reason", EAbortReason::DisappearedFromNode);
        for (const auto& allocation : missingAllocations) {
            YT_LOG_DEBUG("Aborting vanished allocation (AllocationId: %v, OperationId: %v)", allocation->GetId(), allocation->GetOperationId());
            OnAllocationAborted(allocation, error, EAbortReason::DisappearedFromNode);
        }
    }

    ProcessAllocationsToAbort(response, node);
}

void TNodeShard::LogOngoingAllocationsOnHeartbeat(
    const INodeHeartbeatStrategyProxyPtr& strategyProxy,
    TInstant now,
    const TStateToAllocationList& ongoingAllocationsByState) const
{
    for (auto allocationState : TEnumTraits<EAllocationState>::GetDomainValues()) {
        const auto& allocations = ongoingAllocationsByState[allocationState];
        if (allocations.empty() || !strategyProxy->HasMatchingTree()) {
            continue;
        }

        TStringBuilder attributesBuilder;
        TDelimitedStringBuilderWrapper delimitedAttributesBuilder(&attributesBuilder);
        strategyProxy->BuildSchedulingAttributesStringForOngoingAllocations(
            allocations,
            now,
            delimitedAttributesBuilder);

        YT_LOG_DEBUG(
            "Allocations are %lv (%v)",
            allocationState,
            attributesBuilder.Flush());
    }
}

TAllocationPtr TNodeShard::ProcessAllocationHeartbeat(
    const TExecNodePtr& node,
    NProto::NNode::TRspHeartbeat* response,
    NProto::TAllocationStatus* allocationStatus)
{
    auto allocationId = FromProto<TAllocationId>(allocationStatus->allocation_id());
    auto operationId = FromProto<TOperationId>(allocationStatus->operation_id());

    auto allocationState = CheckedEnumCast<EAllocationState>(allocationStatus->state());

    const auto& address = node->GetDefaultAddress();

    if (IsAllocationAborted(allocationId, node)) {
        return nullptr;
    }

    auto allocation = FindAllocation(allocationId, node);
    auto operationState = FindOperationState(operationId);

    if (!allocation) {
        auto Logger = SchedulerLogger.WithTag(
            "Address: %v, AllocationId: %v, OperationId: %v, AllocationState: %v",
            address,
            allocationId,
            operationId,
            allocationState);

        // We can decide what to do with the allocation of an operation only when all allocations are revived.
        if ((operationState && operationState->WaitingForRevival) ||
            WaitingForRegisterOperationIds_.contains(operationId))
        {
            if (operationState && !operationState->OperationUnreadyLoggedAllocationIds.contains(allocationId)) {
                YT_LOG_DEBUG("Allocation is skipped since operation allocations are not ready yet");
                operationState->OperationUnreadyLoggedAllocationIds.insert(allocationId);
            }
            return nullptr;
        }

        switch (allocationState) {
            case EAllocationState::Finished:
                YT_LOG_DEBUG("Unknown allocation has finished");
                break;

            case EAllocationState::Running:
                AddAllocationToAbort(response, {allocationId, /*AbortReason*/ std::nullopt});
                YT_LOG_DEBUG("Unknown allocation is running, abort it");
                break;

            case EAllocationState::Waiting:
                AddAllocationToAbort(response, {allocationId, /*AbortReason*/ std::nullopt});
                YT_LOG_DEBUG("Unknown allocation is waiting, abort it");
                break;

            case EAllocationState::Finishing:
                YT_LOG_DEBUG("Unknown allocation is finishing");
                break;

            default:
                YT_ABORT();
        }
        return nullptr;
    }

    auto guard = TCodicilGuard{allocation->CodicilString()};

    const auto& Logger = allocation->Logger();

    YT_VERIFY(operationState);

    // Check if the allocation is running on a proper node.
    if (node->GetId() != allocation->GetNode()->GetId()) {
        // Allocation has moved from one node to another. No idea how this could happen.
        switch (allocationState) {
            case EAllocationState::Finishing:
                // Allocation is already finishing, do nothing.
                break;
            case EAllocationState::Finished:
                YT_LOG_WARNING(
                    "Allocation status report was expected from other node (ExpectedAddress: %v)",
                    node->GetDefaultAddress());
                break;
            case EAllocationState::Waiting:
            case EAllocationState::Running:
                AddAllocationToAbort(response, {allocationId, EAbortReason::JobOnUnexpectedNode});
                YT_LOG_WARNING(
                    "Allocation status report was expected from other node, abort scheduled (ExpectedAddress: %v)",
                    node->GetDefaultAddress());
                break;
            default:
                YT_ABORT();
        }
        return nullptr;
    }

    bool stateChanged = allocationState != allocation->GetState();

    switch (allocationState) {
        case EAllocationState::Finished: {
            if (auto error = FromProto<TError>(allocationStatus->result().error());
                ParseAbortReason(error, allocationId, Logger).value_or(EAbortReason::Scheduler) == EAbortReason::GetSpecFailed)
            {
                YT_LOG_DEBUG("Node has failed to get allocation spec, abort allocation");

                OnAllocationAborted(allocation, error, EAbortReason::GetSpecFailed);
            } else {
                YT_LOG_DEBUG("Allocation finished, storage scheduled");

                OnAllocationFinished(allocation);
            }

            break;
        }

        case EAllocationState::Running:
        case EAllocationState::Waiting:
            if (stateChanged) {
                SetAllocationState(allocation, allocationState);
            }
            switch (allocationState) {
                case EAllocationState::Running:
                    YT_LOG_DEBUG_IF(stateChanged, "Allocation is now running");
                    OnAllocationRunning(allocation, allocationStatus);
                    break;

                case EAllocationState::Waiting:
                    YT_LOG_DEBUG_IF(stateChanged, "Allocation is now waiting");
                    break;
                default:
                    YT_ABORT();
            }

            if (allocation->GetPreempted()) {
                SendPreemptedAllocationToNode(
                    response,
                    allocation,
                    CpuDurationToDuration(allocation->GetPreemptionTimeout()));
            }

            break;

        case EAllocationState::Finishing:
            YT_LOG_DEBUG("Allocation is finishing");
            break;

        default:
            YT_ABORT();
    }

    return allocation;
}

bool TNodeShard::IsHeartbeatThrottlingWithComplexity(
    const TExecNodePtr& node,
    const INodeHeartbeatStrategyProxyPtr& strategyProxy)
{
    int schedulingHeartbeatComplexity = strategyProxy->GetSchedulingHeartbeatComplexity();
    node->SetSchedulingHeartbeatComplexity(schedulingHeartbeatComplexity);

    if (ConcurrentHeartbeatComplexity_ >= Config_->SchedulingHeartbeatComplexityLimit) {
        YT_LOG_DEBUG("Heartbeat complexity limit reached (NodeAddress: %v, TotalHeartbeatComplexity: %v, CurrentComplexity: %v)",
            node->GetDefaultAddress(),
            ConcurrentHeartbeatComplexity_.load(),
            schedulingHeartbeatComplexity);
        ConcurrentHeartbeatComplexityLimitReachedCounter_.Increment();
        return true;
    }
    return false;
}

bool TNodeShard::IsHeartbeatThrottlingWithCount(const TExecNodePtr& node)
{
    if (ConcurrentHeartbeatCount_ >= Config_->HardConcurrentHeartbeatLimit) {
        YT_LOG_INFO("Hard heartbeat limit reached (NodeAddress: %v, Limit: %v, Count: %v)",
            node->GetDefaultAddress(),
            Config_->HardConcurrentHeartbeatLimit,
            ConcurrentHeartbeatCount_);
        HardConcurrentHeartbeatLimitReachedCounter_.Increment();
        return true;
    }

    if (ConcurrentHeartbeatCount_ >= Config_->SoftConcurrentHeartbeatLimit &&
        node->GetLastSeenTime() + Config_->HeartbeatProcessBackoff > TInstant::Now())
    {
        YT_LOG_DEBUG("Soft heartbeat limit reached (NodeAddress: %v, Limit: %v, Count: %v)",
            node->GetDefaultAddress(),
            Config_->SoftConcurrentHeartbeatLimit,
            ConcurrentHeartbeatCount_);
        SoftConcurrentHeartbeatLimitReachedCounter_.Increment();
        return true;
    }

    return false;
}

void TNodeShard::SubtractNodeResources(const TExecNodePtr& node)
{
    TotalNodeCount_ -= 1;
    if (node->GetResourceLimits().GetUserSlots() > 0) {
        ExecNodeCount_ -= 1;
    }
}

void TNodeShard::AddNodeResources(const TExecNodePtr& node)
{
    TotalNodeCount_ += 1;

    if (node->GetResourceLimits().GetUserSlots() > 0) {
        ExecNodeCount_ += 1;
    } else {
        // Check that we successfully reset all resource limits to zero for node with zero user slots.
        YT_VERIFY(node->GetResourceLimits() == TJobResources());
    }
}

void TNodeShard::UpdateNodeResources(
    const TExecNodePtr& node,
    const TJobResources& limits,
    const TJobResources& usage,
    TDiskResources diskResources)
{
    auto oldResourceLimits = node->GetResourceLimits();

    YT_VERIFY(node->GetSchedulerState() == ENodeState::Online);

    if (limits.GetUserSlots() > 0) {
        if (node->GetResourceLimits().GetUserSlots() == 0 && node->GetMasterState() == NNodeTrackerClient::ENodeState::Online) {
            ExecNodeCount_ += 1;
        }
        node->SetResourceLimits(limits);
        node->SetResourceUsage(usage);
        node->SetDiskResources(std::move(diskResources));
    } else {
        if (node->GetResourceLimits().GetUserSlots() > 0 && node->GetMasterState() == NNodeTrackerClient::ENodeState::Online) {
            ExecNodeCount_ -= 1;
        }
        node->SetResourceLimits({});
        node->SetResourceUsage({});
    }

    if (node->GetMasterState() == NNodeTrackerClient::ENodeState::Online) {
        // Clear cache if node has come with non-zero usage.
        if (oldResourceLimits.GetUserSlots() == 0 && node->GetResourceUsage().GetUserSlots() > 0) {
            CachedResourceStatisticsByTags_->Clear();
        }

        if (!Dominates(node->GetResourceLimits(), node->GetResourceUsage())) {
            if (!node->GetResourcesOvercommitStartTime()) {
                node->SetResourcesOvercommitStartTime(TInstant::Now());
            }
        } else {
            node->SetResourcesOvercommitStartTime(TInstant());
        }
    }
}

void TNodeShard::BeginNodeHeartbeatProcessing(const TExecNodePtr& node)
{
    YT_VERIFY(!node->GetHasOngoingHeartbeat());
    node->SetHasOngoingHeartbeat(true);
    node->SetLastSeenTime(TInstant::Now());

    ConcurrentHeartbeatComplexity_ += node->GetSchedulingHeartbeatComplexity();
    ConcurrentHeartbeatCount_ += 1;
}

void TNodeShard::EndNodeHeartbeatProcessing(const TExecNodePtr& node)
{
    YT_VERIFY(node->GetHasOngoingHeartbeat());

    TForbidContextSwitchGuard guard;

    node->SetHasOngoingHeartbeat(false);

    ConcurrentHeartbeatComplexity_ -= node->GetSchedulingHeartbeatComplexity();
    ConcurrentHeartbeatCount_ -= 1;

    node->SetLastSeenTime(TInstant::Now());

    if (node->GetPendingAllocationsAbortionReason()) {
        DoAbortAllAllocationsAtNode(node, *node->GetPendingAllocationsAbortionReason());
        node->SetPendingAllocationsAbortionReason({});
    }

    if (node->GetHasPendingUnregistration()) {
        DoUnregisterNode(node);
    }
}

void TNodeShard::ProcessScheduledAndPreemptedAllocations(
    const ISchedulingContextPtr& schedulingContext,
    NProto::NNode::TRspHeartbeat* response)
{
    std::vector<TAllocationId> startedAllocations;
    for (const auto& allocation : schedulingContext->StartedAllocations()) {
        auto* operationState = FindOperationState(allocation->GetOperationId());
        if (!operationState) {
            YT_LOG_DEBUG(
                "Allocation cannot be started since operation is no longer known (AllocationId: %v, OperationId: %v)",
                allocation->GetId(),
                allocation->GetOperationId());
            continue;
        }

        if (operationState->ForbidNewAllocations) {
            YT_LOG_DEBUG(
                "Allocation cannot be started since new allocations are forbidden (AllocationId: %v, OperationId: %v)",
                allocation->GetId(),
                allocation->GetOperationId());
            if (!operationState->ControllerTerminated) {
                const auto& controller = operationState->Controller;
                controller->OnNonscheduledAllocationAborted(
                    allocation->GetId(),
                    EAbortReason::SchedulingOperationSuspended,
                    allocation->GetControllerEpoch());
                AllocationsToSubmitToStrategy_[allocation->GetId()] = TAllocationUpdate{
                    EAllocationUpdateStatus::Finished,
                    allocation->GetOperationId(),
                    allocation->GetId(),
                    allocation->GetTreeId(),
                    TJobResources(),
                    allocation->GetNode()->NodeDescriptor().GetDataCenter(),
                    allocation->GetNode()->GetInfinibandCluster()
                };
                operationState->AllocationsToSubmitToStrategy.insert(allocation->GetId());
            }
            continue;
        }

        const auto& controller = operationState->Controller;
        auto agent = controller->FindAgent();
        if (!agent) {
            YT_LOG_DEBUG(
                "Cannot start allocation: agent is no longer known (AllocationId: %v, OperationId: %v)",
                allocation->GetId(),
                allocation->GetOperationId());
            continue;
        }
        if (agent->GetIncarnationId() != allocation->GetIncarnationId()) {
            YT_LOG_DEBUG(
                "Cannot start allocation: wrong agent incarnation "
                "(AllocationId: %v, OperationId: %v, ExpectedIncarnationId: %v, ActualIncarnationId: %v)",
                allocation->GetId(),
                allocation->GetOperationId(),
                allocation->GetIncarnationId(),
                agent->GetIncarnationId());
            continue;
        }

        RegisterAllocation(allocation);

        auto* startInfo = response->add_allocations_to_start();
        ToProto(startInfo->mutable_allocation_id(), allocation->GetId());
        ToProto(startInfo->mutable_operation_id(), allocation->GetOperationId());
        *startInfo->mutable_resource_limits() = ToNodeResources(allocation->ResourceUsage());

        if (Config_->SendFullControllerAgentDescriptorsForAllocations) {
            SetControllerAgentDescriptor(agent, startInfo->mutable_controller_agent_descriptor());
        } else {
            SetControllerAgentIncarnationId(agent, startInfo->mutable_controller_agent_descriptor());
        }
    }

    for (const auto& preemptedAllocation : schedulingContext->PreemptedAllocations()) {
        auto& allocation = preemptedAllocation.Allocation;
        auto preemptionTimeout = preemptedAllocation.PreemptionTimeout;
        if (!FindOperationState(allocation->GetOperationId()) || allocation->GetUnregistered()) {
            YT_LOG_DEBUG(
                "Cannot preempt allocation since operation is no longer known or the allocation is unregistered (AllocationId: %v, OperationId: %v)",
                allocation->GetId(),
                allocation->GetOperationId());
            continue;
        }

        ProcessPreemptedAllocation(response, allocation, preemptionTimeout);
    }
}

void TNodeShard::OnAllocationRunning(const TAllocationPtr& allocation, NProto::TAllocationStatus* status)
{
    YT_VERIFY(status);

    auto now = GetCpuInstant();
    if (now < allocation->GetRunningAllocationUpdateDeadline()) {
        return;
    }
    allocation->SetRunningAllocationUpdateDeadline(now + DurationToCpuDuration(Config_->RunningAllocationsUpdatePeriod));

    allocation->ResourceUsage() = ToJobResources(status->resource_usage());

    YT_VERIFY(Dominates(allocation->ResourceUsage(), TJobResources()));

    auto* operationState = FindOperationState(allocation->GetOperationId());
    if (operationState) {
        auto it = AllocationsToSubmitToStrategy_.find(allocation->GetId());
        if (it == AllocationsToSubmitToStrategy_.end() || it->second.Status != EAllocationUpdateStatus::Finished) {
            AllocationsToSubmitToStrategy_[allocation->GetId()] = TAllocationUpdate{
                EAllocationUpdateStatus::Running,
                allocation->GetOperationId(),
                allocation->GetId(),
                allocation->GetTreeId(),
                allocation->ResourceUsage(),
                allocation->GetNode()->NodeDescriptor().GetDataCenter(),
                allocation->GetNode()->GetInfinibandCluster()
            };

            operationState->AllocationsToSubmitToStrategy.insert(allocation->GetId());
        }
    }
}

void TNodeShard::OnAllocationFinished(const TAllocationPtr& allocation)
{
    if (const auto allocationState = allocation->GetState();
        allocationState == EAllocationState::Finishing ||
        allocationState == EAllocationState::Finished)
    {
        return;
    }

    SetFinishedState(allocation);

    UnregisterAllocation(allocation);
}

void TNodeShard::OnAllocationAborted(
    const TAllocationPtr& allocation,
    const TError& error,
    EAbortReason abortReason)
{
    YT_VERIFY(!error.IsOK());

    if (auto allocationState = allocation->GetState();
        allocationState == EAllocationState::Finishing ||
        allocationState == EAllocationState::Finished)
    {
        return;
    }

    SetFinishedState(allocation);

    if (auto* operationState = FindOperationState(allocation->GetOperationId())) {
        const auto& controller = operationState->Controller;
        controller->OnAllocationAborted(allocation, error, /*scheduled*/ true, abortReason);
    }

    EmplaceOrCrash(allocation->GetNode()->AllocationsToAbort(), allocation->GetId(), abortReason);

    UnregisterAllocation(allocation);
}

void TNodeShard::SubmitAllocationsToStrategy()
{
    YT_PROFILE_TIMING("/scheduler/strategy_job_processing_time") {
        if (!AllocationsToSubmitToStrategy_.empty()) {
            THashSet<TAllocationId> allocationsToPostpone;
            THashMap<TAllocationId, EAbortReason> allocationsToAbort;
            auto allocationUpdates = GetValues(AllocationsToSubmitToStrategy_);
            ManagerHost_->GetStrategy()->ProcessAllocationUpdates(
                allocationUpdates,
                &allocationsToPostpone,
                &allocationsToAbort);

            for (const auto& [allocationId, abortReason] : allocationsToAbort) {
                auto error = TError("Aborting allocation by strategy request")
                    << TErrorAttribute("abort_reason", abortReason);
                AbortAllocation(allocationId, error, abortReason);
            }

            std::vector<std::pair<TOperationId, TAllocationId>> allocationsToRemove;
            for (const auto& allocation : allocationUpdates) {
                if (!allocationsToPostpone.contains(allocation.AllocationId)) {
                    allocationsToRemove.emplace_back(allocation.OperationId, allocation.AllocationId);
                }
            }

            for (const auto& [operationId, allocationId] : allocationsToRemove) {
                auto* operationState = FindOperationState(operationId);
                if (operationState) {
                    operationState->AllocationsToSubmitToStrategy.erase(allocationId);
                }

                EraseOrCrash(AllocationsToSubmitToStrategy_, allocationId);
            }
        }
        SubmitToStrategyAllocationCount_.store(AllocationsToSubmitToStrategy_.size());
    }
}

void TNodeShard::UpdateProfilingCounter(const TAllocationPtr& allocation, int value)
{
    auto allocationState = allocation->GetState();

    YT_VERIFY(allocationState <= EAllocationState::Running);

    auto createGauge = [&] {
        return SchedulerProfiler.WithTags(TTagSet(TTagList{
                {ProfilingPoolTreeKey, allocation->GetTreeId()},
                {"state", FormatEnum(allocationState)}}))
            .Gauge("/allocations/running_allocation_count");
    };

    TAllocationCounterKey key(allocationState, allocation->GetTreeId());

    auto it = AllocationCounter_.find(key);
    if (it == AllocationCounter_.end()) {
        it = AllocationCounter_.emplace(
            std::move(key),
            std::pair(
                0,
                createGauge())).first;
    }

    auto& [count, gauge] = it->second;
    count += value;
    gauge.Update(count);
}

void TNodeShard::SetAllocationState(const TAllocationPtr& allocation, const EAllocationState state)
{
    YT_VERIFY(state != EAllocationState::Scheduled);

    UpdateProfilingCounter(allocation, -1);
    allocation->SetState(state);
    UpdateProfilingCounter(allocation, 1);
}

void TNodeShard::SetFinishedState(const TAllocationPtr& allocation)
{
    UpdateProfilingCounter(allocation, -1);
    allocation->SetState(EAllocationState::Finished);
}

void TNodeShard::ProcessOperationInfoHeartbeat(
    const TScheduler::TCtxNodeHeartbeat::TTypedRequest* request,
    TScheduler::TCtxNodeHeartbeat::TTypedResponse* response)
{
    for (const auto& protoOperationId : request->operations_ids_to_request_info()) {
        auto operationId = FromProto<TOperationId>(protoOperationId);

        auto* protoOperationInfo = response->add_operation_infos();
        *protoOperationInfo->mutable_operation_id() = protoOperationId;

        const auto* operationState = FindOperationState(operationId);
        if (!operationState) {
            protoOperationInfo->set_running(WaitingForRegisterOperationIds_.contains(operationId));
            continue;
        }

        protoOperationInfo->set_running(true);

        auto agent = operationState->Controller->FindAgent();
        if (!agent) {
            continue;
        }

        if (Config_->SendFullControllerAgentDescriptorsForAllocations) {
            SetControllerAgentDescriptor(agent, protoOperationInfo->mutable_controller_agent_descriptor());
        } else {
            SetControllerAgentIncarnationId(agent, protoOperationInfo->mutable_controller_agent_descriptor());
        }
    }
}

void TNodeShard::SetMinSpareResources(
    TScheduler::TCtxNodeHeartbeat::TTypedResponse* response)
{
    auto minSpareResources = Config_->MinSpareAllocationResourcesOnNode
        ? ToJobResources(*Config_->MinSpareAllocationResourcesOnNode, TJobResources())
        : TJobResources();
    ToProto(response->mutable_min_spare_resources(), minSpareResources);
}

bool TNodeShard::ShouldSendRegisteredControllerAgents(TScheduler::TCtxNodeHeartbeat::TTypedRequest* request)
{
    if (Config_->AlwaysSendControllerAgentDescriptors) {
        return true;
    }

    THashSet<TIncarnationId> nodeAgentIncarnationIds;
    for (const auto& protoAgentDescriptor : request->registered_controller_agents()) {
        InsertOrCrash(
            nodeAgentIncarnationIds,
            FromProto<NScheduler::TIncarnationId>(protoAgentDescriptor.incarnation_id()));
    }

    return nodeAgentIncarnationIds != RegisteredAgentIncarnationIds_;
}

void TNodeShard::AddRegisteredControllerAgentsToResponse(auto* response)
{
    response->set_registered_controller_agents_sent(true);
    for (const auto& [agentId, agentInfo] : RegisteredAgents_) {
        auto agentDescriptorProto = response->add_registered_controller_agents();
        SetControllerAgentDescriptor(agentId, agentInfo.Addresses, agentInfo.IncarnationId, agentDescriptorProto);
    }

    HeartbeatRegisteredControllerAgentsBytes_.Increment(
        response->registered_controller_agents().SpaceUsedExcludingSelfLong());
}

void TNodeShard::RegisterAllocation(const TAllocationPtr& allocation)
{
    auto& operationState = GetOperationState(allocation->GetOperationId());

    auto node = allocation->GetNode();

    EmplaceOrCrash(operationState.Allocations, allocation->GetId(), allocation);
    EmplaceOrCrash(node->Allocations(), allocation);
    EmplaceOrCrash(node->IdToAllocation(), allocation->GetId(), allocation);
    ++ActiveAllocationCount_;

    UpdateProfilingCounter(allocation, 1);

    YT_LOG_DEBUG(
        "Allocation registered (AllocationId: %v, Revived: %v, OperationId: %v, ControllerEpoch: %v, SchedulingIndex: %v)",
        allocation->GetId(),
        allocation->IsRevived(),
        allocation->GetOperationId(),
        allocation->GetControllerEpoch(),
        allocation->GetSchedulingIndex());

    if (allocation->IsRevived()) {
        allocation->GetNode()->AllocationsToAbort().erase(allocation->GetId());
    }
}

void TNodeShard::UnregisterAllocation(const TAllocationPtr& allocation, bool causedByRevival)
{
    if (allocation->GetUnregistered()) {
        return;
    }

    auto allocationId = allocation->GetId();

    allocation->SetUnregistered(true);

    auto* operationState = FindOperationState(allocation->GetOperationId());
    const auto& node = allocation->GetNode();

    EraseOrCrash(node->Allocations(), allocation);
    EraseOrCrash(node->IdToAllocation(), allocationId);
    --ActiveAllocationCount_;

    if (operationState && operationState->Allocations.erase(allocationId)) {
        AllocationsToSubmitToStrategy_[allocationId] =
            TAllocationUpdate{
                EAllocationUpdateStatus::Finished,
                allocation->GetOperationId(),
                allocationId,
                allocation->GetTreeId(),
                TJobResources(),
                allocation->GetNode()->NodeDescriptor().GetDataCenter(),
                allocation->GetNode()->GetInfinibandCluster()};
        operationState->AllocationsToSubmitToStrategy.insert(allocationId);

        YT_LOG_DEBUG_IF(
            !causedByRevival,
            "Allocation unregistered (AllocationId: %v, OperationId: %v)",
            allocationId,
            allocation->GetOperationId());
    } else {
        YT_LOG_DEBUG_IF(
            !causedByRevival,
            "Dangling allocation unregistered (AllocationId: %v, OperationId: %v)",
            allocationId,
            allocation->GetOperationId());
    }
}

void TNodeShard::SendPreemptedAllocationToNode(
    NProto::NNode::TRspHeartbeat* response,
    const TAllocationPtr& allocation,
    TDuration preemptionTimeout) const
{
    YT_LOG_DEBUG(
        "Add allocation to preempt (AllocationId: %v, PreemptionTimeout: %v)",
        allocation->GetId(),
        preemptionTimeout);
    AddAllocationToPreempt(
        response,
        allocation->GetId(),
        preemptionTimeout,
        allocation->GetPreemptionReason(),
        allocation->GetPreemptedFor());
}

void TNodeShard::ProcessPreemptedAllocation(
    NProto::NNode::TRspHeartbeat* response,
    const TAllocationPtr& allocation,
    TDuration preemptionTimeout)
{
    PreemptAllocation(allocation, DurationToCpuDuration(preemptionTimeout));
    SendPreemptedAllocationToNode(response, allocation, preemptionTimeout);
}

void TNodeShard::PreemptAllocation(const TAllocationPtr& allocation, TCpuDuration preemptionTimeout)
{
    YT_LOG_DEBUG(
        "Preempting allocation (AllocationId: %v, OperationId: %v, TreeId: %v, Reason: %v)",
        allocation->GetId(),
        allocation->GetOperationId(),
        allocation->GetTreeId(),
        allocation->GetPreemptionReason());

    DoPreemptAllocation(allocation, preemptionTimeout);
}

// TODO(pogorelov): Refactor preemption processing
void TNodeShard::DoPreemptAllocation(
    const TAllocationPtr& allocation,
    TCpuDuration preemptionTimeout)
{
    YT_LOG_DEBUG(
        "Preempting allocation (PreemptionTimeout: %.3g, AllocationId: %v, OperationId: %v)",
        CpuDurationToDuration(preemptionTimeout).SecondsFloat(),
        allocation->GetId(),
        allocation->GetOperationId());

    allocation->SetPreempted(true);

    if (preemptionTimeout != 0) {
        allocation->SetPreemptionTimeout(preemptionTimeout);
    }
}

void TNodeShard::ProcessAllocationsToAbort(NProto::NNode::TRspHeartbeat* response, const TExecNodePtr& node)
{
    for (auto& [allocationId, abortReason] : node->AllocationsToAbort()) {
        YT_LOG_DEBUG(
            "Sent allocation abort request to node (Reason: %v, AllocationId: %v)",
            abortReason,
            allocationId);
        AddAllocationToAbort(response, {allocationId, abortReason});
    }

    node->AllocationsToAbort().clear();
}

TExecNodePtr TNodeShard::FindNodeByAllocation(TAllocationId allocationId)
{
    auto nodeId = NodeIdFromAllocationId(allocationId);
    auto it = IdToNode_.find(nodeId);
    return it == IdToNode_.end() ? nullptr : it->second;
}

bool TNodeShard::IsAllocationAborted(TAllocationId allocationId, const TExecNodePtr& node)
{
    return node->AllocationsToAbort().contains(allocationId);
}

TAllocationPtr TNodeShard::FindAllocation(TAllocationId allocationId, const TExecNodePtr& node)
{
    const auto& idToAllocation = node->IdToAllocation();
    auto it = idToAllocation.find(allocationId);
    return it == idToAllocation.end() ? nullptr : it->second;
}

TAllocationPtr TNodeShard::FindAllocation(TAllocationId allocationId)
{
    auto node = FindNodeByAllocation(allocationId);
    if (!node) {
        return nullptr;
    }
    return FindAllocation(allocationId, node);
}

TAllocationPtr TNodeShard::GetAllocationOrThrow(TAllocationId allocationId)
{
    auto allocation = FindAllocation(allocationId);
    if (!allocation) {
        THROW_ERROR_EXCEPTION(
            NScheduler::EErrorCode::NoSuchAllocation,
            "No such allocation %v",
            allocationId);
    }
    return allocation;
}

TNodeShard::TOperationState* TNodeShard::FindOperationState(TOperationId operationId) noexcept
{
    return const_cast<TNodeShard::TOperationState*>(const_cast<const TNodeShard*>(this)->FindOperationState(operationId));
}

const TNodeShard::TOperationState* TNodeShard::FindOperationState(TOperationId operationId) const noexcept
{
    auto it = IdToOperationState_.find(operationId);
    return it != IdToOperationState_.end() ? &it->second : nullptr;
}

TNodeShard::TOperationState& TNodeShard::GetOperationState(TOperationId operationId) noexcept
{
    return const_cast<TNodeShard::TOperationState&>(const_cast<const TNodeShard*>(this)->GetOperationState(operationId));
}

const TNodeShard::TOperationState& TNodeShard::GetOperationState(TOperationId operationId) const noexcept
{
    return GetOrCrash(IdToOperationState_, operationId);
}

NYson::TYsonString TNodeShard::BuildNodeYson(const TExecNodePtr& node) const
{
    const auto& strategy = ManagerHost_->GetStrategy();
    return BuildYsonStringFluently<EYsonType::MapFragment>()
        .Item(node->GetDefaultAddress()).BeginMap()
            .Do([&] (TFluentMap fluent) {
                node->BuildAttributes(fluent);
            })
            .Do([&] (TFluentMap fluent) {
                strategy->BuildSchedulingAttributesForNode(node->GetId(), node->GetDefaultAddress(), node->Tags(), fluent);
            })
        .EndMap()
        .Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
