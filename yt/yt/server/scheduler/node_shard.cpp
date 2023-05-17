#include "node_shard.h"
#include "node_manager.h"
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

#include <yt/yt/ytlib/job_tracker_client/proto/job_tracker_service.pb.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

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
using namespace NJobProberClient;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

using NNodeTrackerClient::TNodeId;
using NScheduler::NProto::TSchedulerJobResultExt;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

// COMPAT(pogorelov)

TAllocationId GetJobId(const NProto::TAllocationStatus* status)
{
    return FromProto<TAllocationId>(status->allocation_id());
}

auto* MutableJobId(NProto::NNode::TAllocationToInterrupt* proto)
{
    return proto->mutable_allocation_id();
}

auto* MutableJobId(NProto::NNode::TAllocationStartInfo* proto)
{
    return proto->mutable_allocation_id();
}

auto& GetJobs(NProto::NNode::TReqHeartbeat* request)
{
    return request->allocations();
}

auto* MutableJobs(NProto::NNode::TReqHeartbeat* request)
{
    return request->mutable_allocations();
}

auto& UnconfirmedJobs(NProto::NNode::TReqHeartbeat* request)
{
    return request->unconfirmed_allocations();
}

EAllocationState GetAllocationState(NProto::TAllocationStatus* status)
{
    YT_VERIFY(status->has_state());

    return CheckedEnumCast<EAllocationState>(status->state());
}

void SetControllerAgentInfo(
    const TAgentId& agentId,
    const TAddressMap& addresses,
    TIncarnationId incarnationId,
    auto proto)
{
    ToProto(proto->mutable_addresses(), addresses);
    ToProto(proto->mutable_incarnation_id(), incarnationId);
    ToProto(proto->mutable_agent_id(), agentId);
}

void SetControllerAgentInfo(const TControllerAgentPtr& agent, auto proto)
{
    SetControllerAgentInfo(agent->GetId(), agent->GetAgentAddresses(), agent->GetIncarnationId(), proto);
}

auto* AddJobsToInterrupt(NProto::NNode::TRspHeartbeat* response)
{
    return response->add_allocations_to_interrupt();
}

auto* AddJobsToStart(NProto::NNode::TRspHeartbeat* response)
{
    return response->add_allocations_to_start();
}

void AddJobToInterrupt(
    TScheduler::TCtxNodeHeartbeat::TTypedResponse* response,
    TJobId jobId,
    TDuration duration,
    EInterruptReason interruptionReason,
    const std::optional<TString>& preemptionReason,
    const std::optional<TPreemptedFor>& preemptedFor)
{
    auto jobToInterrupt = AddJobsToInterrupt(response);
    ToProto(MutableJobId(jobToInterrupt), jobId);
    jobToInterrupt->set_timeout(ToProto<i64>(duration));
    jobToInterrupt->set_interruption_reason(static_cast<int>(interruptionReason));

    if (preemptionReason) {
        jobToInterrupt->set_preemption_reason(*preemptionReason);
    }

    if (preemptedFor) {
        ToProto(jobToInterrupt->mutable_preempted_for(), *preemptedFor);
    }
}

std::optional<EAbortReason> ParseAbortReason(const TError& error, TJobId jobId, const NLogging::TLogger& Logger)
{
    auto abortReasonString = error.Attributes().Find<TString>("abort_reason");
    if (!abortReasonString) {
        return {};
    }

    auto abortReason = TryParseEnum<EAbortReason>(*abortReasonString);
    if (!abortReason) {
        YT_LOG_DEBUG(
            "Failed to parse abort reason from result (JobId: %v, UnparsedAbortReason: %v)",
            jobId,
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
    , RemoveOutdatedScheduleJobEntryExecutor_(New<TPeriodicExecutor>(
        GetInvoker(),
        BIND(&TNodeShard::RemoveOutdatedScheduleJobEntries, MakeWeak(this)),
        Config_->ScheduleJobEntryCheckPeriod))
    , SubmitJobsToStrategyExecutor_(New<TPeriodicExecutor>(
        GetInvoker(),
        BIND(&TNodeShard::SubmitJobsToStrategy, MakeWeak(this)),
        Config_->NodeShardSubmitJobsToStrategyPeriod))
{
    SoftConcurrentHeartbeatLimitReachedCounter_ = SchedulerProfiler
        .WithTag("limit_type", "soft")
        .Counter("/node_heartbeat/concurrent_limit_reached_count");
    HardConcurrentHeartbeatLimitReachedCounter_ = SchedulerProfiler
        .WithTag("limit_type", "hard")
        .Counter("/node_heartbeat/concurrent_limit_reached_count");
    HeartbeatWithScheduleJobsCounter_ = SchedulerProfiler
        .Counter("/node_heartbeat/with_schedule_jobs_count");
    HeartbeatJobCount_ = SchedulerProfiler
        .Counter("/node_heartbeat/job_count");
    HeartbeatCount_ = SchedulerProfiler
        .Counter("/node_heartbeat/count");
    HeartbeatRequestProtoMessageBytes_ = SchedulerProfiler
        .Counter("/node_heartbeat/request/proto_message_bytes");
    HeartbeatResponseProtoMessageBytes_ = SchedulerProfiler
        .Counter("/node_heartbeat/response/proto_message_bytes");
    HeartbeatRegesteredControllerAgentsBytes_ = SchedulerProfiler
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

    SubmitJobsToStrategyExecutor_->SetPeriod(config->NodeShardSubmitJobsToStrategyPeriod);
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
    SubmitJobsToStrategyExecutor_->Start();

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

    IdToOpertionState_.clear();

    IdToNode_.clear();
    ExecNodeCount_ = 0;
    TotalNodeCount_ = 0;

    ActiveJobCount_ = 0;

    AllocationCounter_.clear();

    JobsToSubmitToStrategy_.clear();

    ConcurrentHeartbeatCount_ = 0;

    JobReporterQueueIsTooLargeNodeCount_ = 0;

    JobIdToScheduleEntry_.clear();
    OperationIdToJobIterators_.clear();

    RegisteredAgents_.clear();

    SubmitJobsToStrategy();
}

void TNodeShard::RegisterOperation(
    TOperationId operationId,
    TControllerEpoch controllerEpoch,
    const IOperationControllerPtr& controller,
    bool jobsReady)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    EmplaceOrCrash(
        IdToOpertionState_,
        operationId,
        TOperationState(controller, jobsReady, ++CurrentEpoch_, controllerEpoch));

    WaitingForRegisterOperationIds_.erase(operationId);

    YT_LOG_DEBUG("Operation registered at node shard (OperationId: %v, JobsReady: %v)",
        operationId,
        jobsReady);
}

void TNodeShard::StartOperationRevival(TOperationId operationId, TControllerEpoch newControllerEpoch)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto& operationState = GetOperationState(operationId);
    operationState.JobsReady = false;
    operationState.ForbidNewJobs = false;
    operationState.OperationUnreadyLoggedJobIds = THashSet<TJobId>();
    operationState.ControllerEpoch = newControllerEpoch;

    YT_LOG_DEBUG("Operation revival started at node shard (OperationId: %v, JobCount: %v, NewControllerEpoch: %v)",
        operationId,
        operationState.Jobs.size(),
        newControllerEpoch);

    auto jobs = operationState.Jobs;
    for (const auto& [jobId, job] : jobs) {
        UnregisterJob(job, /* enableLogging */ false);
        JobsToSubmitToStrategy_.erase(jobId);
    }

    for (const auto jobId : operationState.JobsToSubmitToStrategy) {
        JobsToSubmitToStrategy_.erase(jobId);
    }
    operationState.JobsToSubmitToStrategy.clear();

    RemoveOperationScheduleJobEntries(operationId);

    YT_VERIFY(operationState.Jobs.empty());
}

void TNodeShard::FinishOperationRevival(
    TOperationId operationId,
    const std::vector<TJobPtr>& jobs,
    bool controlJobLifetimeAtScheduler)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto& operationState = GetOperationState(operationId);

    YT_VERIFY(!operationState.JobsReady);
    operationState.JobsReady = true;
    operationState.ForbidNewJobs = false;
    operationState.ControllerTerminated = false;
    operationState.OperationUnreadyLoggedJobIds = THashSet<TJobId>();
    operationState.ControlJobLifetimeAtScheduler = controlJobLifetimeAtScheduler;

    for (const auto& job : jobs) {
        auto node = GetOrRegisterNode(
            job->GetRevivalNodeId(),
            TNodeDescriptor(job->GetRevivalNodeAddress()),
            ENodeState::Online);
        job->SetNode(node);
        // COMPAT(pogorelov)
        if (controlJobLifetimeAtScheduler) {
            SetJobWaitingForConfirmation(job);
        }
        RemoveRecentlyFinishedJob(job->GetId());
        RegisterJob(job);
    }

    YT_LOG_DEBUG(
        "Operation revival finished at node shard (OperationId: %v, RevivedJobCount: %v, ControlJobLifetimeAtScheduler: %v)",
        operationId,
        jobs.size(),
        controlJobLifetimeAtScheduler);

    // Give some time for nodes to confirm the jobs.
    if (controlJobLifetimeAtScheduler) {
        TDelayedExecutor::Submit(
            BIND(&TNodeShard::AbortUnconfirmedJobs, MakeWeak(this), operationId, operationState.ShardEpoch, jobs)
                .Via(GetInvoker()),
            Config_->JobRevivalAbortTimeout);
    }
}

void TNodeShard::ResetOperationRevival(TOperationId operationId, bool controlJobLifetimeAtScheduler)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto& operationState = GetOperationState(operationId);

    operationState.JobsReady = true;
    operationState.ForbidNewJobs = false;
    operationState.ControllerTerminated = false;
    operationState.OperationUnreadyLoggedJobIds = THashSet<TJobId>();
    operationState.ControlJobLifetimeAtScheduler = controlJobLifetimeAtScheduler;

    YT_LOG_DEBUG(
        "Operation revival state reset at node shard (OperationId: %v, ControlJobLifetimeAtScheduler: %v)",
        operationId,
        controlJobLifetimeAtScheduler);
}

void TNodeShard::UnregisterOperation(TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto it = IdToOpertionState_.find(operationId);
    YT_VERIFY(it != IdToOpertionState_.end());
    auto& operationState = it->second;

    for (const auto& job : operationState.Jobs) {
        YT_VERIFY(job.second->GetUnregistered());
    }

    for (const auto jobId : operationState.JobsToSubmitToStrategy) {
        JobsToSubmitToStrategy_.erase(jobId);
    }

    SetOperationJobsReleaseDeadline(&operationState);

    IdToOpertionState_.erase(it);

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

void TNodeShard::AbortJobsAtNode(TNodeId nodeId, EAbortReason reason)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto it = IdToNode_.find(nodeId);
    if (it != IdToNode_.end()) {
        const auto& node = it->second;
        AbortAllJobsAtNode(node, reason);
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

    auto nodeId = request->node_id();
    auto descriptor = FromProto<TNodeDescriptor>(request->node_descriptor());
    const auto& resourceLimits = request->resource_limits();
    const auto& resourceUsage = request->resource_usage();

    context->SetRequestInfo("NodeId: %v, NodeAddress: %v, ResourceUsage: %v, JobCount: %v, Confirmation: {C: %v, U: %v}",
        nodeId,
        descriptor.GetDefaultAddress(),
        ManagerHost_->FormatHeartbeatResourceUsage(
            ToJobResources(resourceUsage),
            ToJobResources(resourceLimits),
            request->disk_resources()
        ),
        GetJobs(request).size(),
        request->confirmed_job_count(),
        UnconfirmedJobs(request).size());

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
            request->disk_resources());
    }

    TLeaseManager::RenewLease(node->GetHeartbeatLease(), Config_->NodeHeartbeatTimeout);
    TLeaseManager::RenewLease(node->GetRegistrationLease(), Config_->NodeRegistrationTimeout);

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

        if (ConcurrentHeartbeatCount_ >= Config_->HardConcurrentHeartbeatLimit) {
            isThrottlingActive = true;
            YT_LOG_INFO("Hard heartbeat limit reached (NodeAddress: %v, Limit: %v, Count: %v)",
                node->GetDefaultAddress(),
                Config_->HardConcurrentHeartbeatLimit,
                ConcurrentHeartbeatCount_);
            HardConcurrentHeartbeatLimitReachedCounter_.Increment();
        } else if (ConcurrentHeartbeatCount_ >= Config_->SoftConcurrentHeartbeatLimit &&
            node->GetLastSeenTime() + Config_->HeartbeatProcessBackoff > TInstant::Now())
        {
            isThrottlingActive = true;
            YT_LOG_DEBUG("Soft heartbeat limit reached (NodeAddress: %v, Limit: %v, Count: %v)",
                node->GetDefaultAddress(),
                Config_->SoftConcurrentHeartbeatLimit,
                ConcurrentHeartbeatCount_);
            SoftConcurrentHeartbeatLimitReachedCounter_.Increment();
        }

        response->set_operation_archive_version(ManagerHost_->GetOperationArchiveVersion());

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

    std::vector<TJobPtr> runningJobs;
    bool hasWaitingJobs = false;
    YT_PROFILE_TIMING("/scheduler/analysis_time") {
        ProcessHeartbeatJobs(
            node,
            request,
            response,
            &runningJobs,
            &hasWaitingJobs);
    }

    bool skipScheduleJobs = false;
    if (hasWaitingJobs || isThrottlingActive) {
        if (hasWaitingJobs) {
            YT_LOG_DEBUG("Waiting jobs found, suppressing new jobs scheduling (NodeAddress: %v)",
                node->GetDefaultAddress());
        }
        if (isThrottlingActive) {
            YT_LOG_DEBUG("Throttling is active, suppressing new jobs scheduling (NodeAddress: %v)",
                node->GetDefaultAddress());
        }
        skipScheduleJobs = true;
    }

    response->set_scheduling_skipped(skipScheduleJobs);

    if (Config_->EnableJobAbortOnZeroUserSlots && node->GetResourceLimits().GetUserSlots() == 0) {
        // Abort all jobs on node immediately, if it has no user slots.
        // Make a copy, the collection will be modified.
        auto jobs = node->Jobs();
        const auto& address = node->GetDefaultAddress();

        auto error = TError("Node without user slots")
            << TErrorAttribute("abort_reason", EAbortReason::NodeWithZeroUserSlots);
        for (const auto& job : jobs) {
            YT_LOG_DEBUG("Aborting job on node without user slots (Address: %v, JobId: %v, OperationId: %v)",
                address,
                job->GetId(),
                job->GetOperationId());

            OnJobAborted(job, error);
        }
    }

    SubmitJobsToStrategy();

    auto mediumDirectory = Bootstrap_
        ->GetClient()
        ->GetNativeConnection()
        ->GetMediumDirectory();
    auto schedulingContext = CreateSchedulingContext(
        Id_,
        Config_,
        node,
        runningJobs,
        mediumDirectory);
    Y_UNUSED(WaitFor(ManagerHost_->GetStrategy()->ProcessSchedulingHeartbeat(schedulingContext, skipScheduleJobs)));

    ProcessScheduledAndPreemptedJobs(
        schedulingContext,
        /*requestContext*/ context);

    ProcessOperationInfoHeartbeat(request, response);
    SetMinSpareResources(response);

    AddRegisteredControllerAgentsToResponse(response);

    context->SetResponseInfo(
        "NodeId: %v, NodeAddress: %v, IsThrottling: %v",
        nodeId,
        descriptor.GetDefaultAddress(),
        isThrottlingActive);

    TStringBuilder schedulingAttributesBuilder;
    TDelimitedStringBuilderWrapper delimitedSchedulingAttributesBuilder(&schedulingAttributesBuilder);
    ManagerHost_->GetStrategy()->BuildSchedulingAttributesStringForNode(
        nodeId,
        node->GetDefaultAddress(),
        node->Tags(),
        delimitedSchedulingAttributesBuilder);
    context->SetRawResponseInfo(schedulingAttributesBuilder.Flush(), /*incremental*/ true);

    if (!skipScheduleJobs) {
        HeartbeatWithScheduleJobsCounter_.Increment();

        node->SetResourceUsage(schedulingContext->ResourceUsage());

        const auto& statistics = schedulingContext->GetSchedulingStatistics();
        if (statistics.ScheduleWithPreemption) {
            node->SetLastPreemptiveHeartbeatStatistics(statistics);
        } else {
            node->SetLastNonPreemptiveHeartbeatStatistics(statistics);
        }

        // NB: Some jobs maybe considered aborted after processing scheduled jobs.
        SubmitJobsToStrategy();

        // TODO(eshcherbin): It's possible to shorten this message by writing preemptible info
        // only when preemptive scheduling has been attempted.
        context->SetIncrementalResponseInfo(
            "StartedJobs: {All: %v, ByPreemption: %v}, PreemptedJobs: %v, "
            "PreemptibleInfo: %v, SsdPriorityPreemption: {Enabled: %v, Media: %v}, "
            "ScheduleJobAttempts: %v, OperationCountByPreemptionPriority: %v",
            schedulingContext->StartedJobs().size(),
            statistics.ScheduledDuringPreemption,
            schedulingContext->PreemptedJobs().size(),
            FormatPreemptibleInfoCompact(statistics),
            statistics.SsdPriorityPreemptionEnabled,
            statistics.SsdPriorityPreemptionMedia,
            FormatScheduleJobAttemptsCompact(statistics),
            FormatOperationCountByPreemptionPriorityCompact(statistics.OperationCountByPreemptionPriority));
    } else {
        context->SetIncrementalResponseInfo("PreemptedJobs: %v", schedulingContext->PreemptedJobs().size());
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

void TNodeShard::RemoveOperationScheduleJobEntries(const TOperationId operationId)
{
    auto range = OperationIdToJobIterators_.equal_range(operationId);
    for (auto it = range.first; it != range.second; ++it) {
        JobIdToScheduleEntry_.erase(it->second);
    }
    OperationIdToJobIterators_.erase(operationId);
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
                execNode->GetLastSeenTime() + Config_->MaxNodeUnseenPeriodToAbortJobs < now)
            {
                AbortAllJobsAtNode(execNode, EAbortReason::NodeOffline);
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
            execNode->Tags() = std::move(tags);
            SubtractNodeResources(execNode);
            // State change must happen before aborting jobs.
            UpdateNodeState(execNode, newState, execNode->GetSchedulerState());

            AbortAllJobsAtNode(execNode, EAbortReason::NodeOffline);
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

                // State change must happen before aborting jobs.
                auto previousSchedulerState = execNode->GetSchedulerState();
                UpdateNodeState(execNode, /* newMasterState */ newState, /* newSchedulerState */ ENodeState::Offline, error);
                if (oldState == NNodeTrackerClient::ENodeState::Online && previousSchedulerState == ENodeState::Online) {
                    SubtractNodeResources(execNode);

                    AbortAllJobsAtNode(execNode, EAbortReason::NodeOffline);
                }
            } else {
                if (oldState != NNodeTrackerClient::ENodeState::Online && newState == NNodeTrackerClient::ENodeState::Online) {
                    AddNodeResources(execNode);
                }
                execNode->Tags() = std::move(tags);
                UpdateNodeState(execNode, /* newMasterState */ newState, /* newSchedulerState */ execNode->GetSchedulerState());
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

void TNodeShard::AbortOperationJobs(TOperationId operationId, const TError& abortError, bool controllerTerminated)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    if (controllerTerminated) {
        RemoveOperationScheduleJobEntries(operationId);
    }

    auto* operationState = FindOperationState(operationId);
    if (!operationState) {
        return;
    }

    operationState->ControllerTerminated = controllerTerminated;
    operationState->ForbidNewJobs = true;
    auto jobs = operationState->Jobs;
    for (const auto& [jobId, job] : jobs) {
        YT_LOG_DEBUG(abortError, "Aborting job (JobId: %v, OperationId: %v)", jobId, operationId);
        OnJobAborted(job, abortError);
    }

    for (const auto& job : operationState->Jobs) {
        YT_VERIFY(job.second->GetUnregistered());
    }
}

void TNodeShard::ResumeOperationJobs(TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto* operationState = FindOperationState(operationId);
    if (!operationState || operationState->ControllerTerminated) {
        return;
    }

    operationState->ForbidNewJobs = false;
}

TNodeDescriptor TNodeShard::GetJobNode(TJobId jobId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto job = FindJob(jobId);

    if (job) {
        return job->GetNode()->NodeDescriptor();
    } else {
        auto node = FindNodeByJob(jobId);
        if (!node) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::NoSuchJob,
                "Job %v not found", jobId);
        }

        return node->NodeDescriptor();
    }
}

void TNodeShard::DumpJobInputContext(TJobId jobId, const TYPath& path, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto job = GetJobOrThrow(jobId);

    WaitFor(ManagerHost_->ValidateOperationAccess(user, job->GetOperationId(), EPermissionSet(EPermission::Read)))
        .ThrowOnError();

    YT_LOG_DEBUG("Saving input contexts (JobId: %v, OperationId: %v, Path: %v, User: %v)",
        job->GetId(),
        job->GetOperationId(),
        path,
        user);

    auto proxy = CreateJobProberProxy(job);
    auto req = proxy.DumpInputContext();
    ToProto(req->mutable_job_id(), jobId);

    auto rspOrError = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        rspOrError,
        "Error saving input context of job %v of operation %v into %v",
        job->GetId(),
        job->GetOperationId(),
        path);

    const auto& rsp = rspOrError.Value();
    auto chunkIds = FromProto<std::vector<TChunkId>>(rsp->chunk_ids());
    YT_VERIFY(chunkIds.size() == 1);

    auto asyncResult = ManagerHost_->AttachJobContext(path, chunkIds.front(), job->GetOperationId(), jobId, user);
    WaitFor(asyncResult)
        .ThrowOnError();

    YT_LOG_DEBUG("Input contexts saved (JobId: %v, OperationId: %v)",
        job->GetId(),
        job->GetOperationId());
}

void TNodeShard::AbandonJob(TJobId jobId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto job = FindJob(jobId);
    if (!job) {
        YT_LOG_DEBUG("Requested to abandon an unknown job, ignored (JobId: %v)", jobId);
        return;
    }

    YT_LOG_DEBUG("Abandoning job (JobId: %v)", jobId);

    DoAbandonJob(job);
}

void TNodeShard::AbortJobByUserRequest(TJobId jobId, std::optional<TDuration> interruptTimeout, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto job = GetJobOrThrow(jobId);

    WaitFor(ManagerHost_->ValidateOperationAccess(user, job->GetOperationId(), EPermissionSet(EPermission::Manage)))
        .ThrowOnError();

    if (auto allocationState = job->GetAllocationState();
        allocationState != EAllocationState::Running &&
        allocationState != EAllocationState::Waiting)
    {
        THROW_ERROR_EXCEPTION("Cannot abort job %v of operation %v since it is not running",
            jobId,
            job->GetOperationId());
    }

    if (interruptTimeout.value_or(TDuration::Zero()) != TDuration::Zero()) {
        YT_LOG_DEBUG("Trying to interrupt job by user request (JobId: %v, InterruptTimeout: %v)",
            jobId,
            interruptTimeout);

        auto proxy = CreateJobProberProxy(job);
        auto req = proxy.Interrupt();
        ToProto(req->mutable_job_id(), jobId);

        req->set_timeout(ToProto<i64>(*interruptTimeout));
        req->set_interruption_reason(static_cast<int>(EInterruptReason::UserRequest));

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error interrupting job %v",
            jobId);

        YT_LOG_INFO("User interrupt requested (JobId: %v, InterruptTimeout: %v)",
            jobId,
            interruptTimeout);

        DoInterruptJob(job, EInterruptReason::UserRequest, DurationToCpuDuration(*interruptTimeout), user);
    } else {
        YT_LOG_DEBUG("Aborting job by user request (JobId: %v, OperationId: %v, User: %v)",
            jobId,
            job->GetOperationId(),
            user);

        auto error = TError("Job aborted by user request")
            << TErrorAttribute("abort_reason", EAbortReason::UserRequest)
            << TErrorAttribute("user", user);

        auto proxy = CreateJobProberProxy(job);
        auto req = proxy.Abort();
        ToProto(req->mutable_job_id(), jobId);
        ToProto(req->mutable_error(), error);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error aborting job %v",
            jobId);

        YT_LOG_INFO("User abort requested (JobId: %v)", jobId);
    }
}

void TNodeShard::AbortJob(TJobId jobId, const TError& error)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto job = FindJob(jobId);
    if (!job) {
        YT_LOG_DEBUG(error, "Requested to abort an unknown job, ignored (JobId: %v)", jobId);
        return;
    }

    YT_LOG_DEBUG(error, "Aborting job by internal request (JobId: %v, OperationId: %v)",
        jobId,
        job->GetOperationId());

    OnJobAborted(job, error);
}

void TNodeShard::AbortJobs(const std::vector<TJobId>& jobIds, const TError& error)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    for (auto jobId : jobIds) {
        AbortJob(jobId, error);
    }
}

void TNodeShard::FailJob(TJobId jobId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto job = FindJob(jobId);
    if (!job) {
        YT_LOG_DEBUG("Requested fail an unknown job, ignored (JobId: %v)", jobId);
        return;
    }

    YT_LOG_DEBUG("Failing job by internal request (JobId: %v, OperationId: %v)",
        jobId,
        job->GetOperationId());

    job->SetFailRequested(true);
}

void TNodeShard::ReleaseJob(TJobId jobId, TReleaseJobFlags releaseFlags)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    // NB: While we kept job id in operation controller, its execution node
    // could have been unregistered.
    auto nodeId = NodeIdFromJobId(jobId);
    auto execNode = FindNodeByJob(jobId);
    if (execNode &&
        execNode->GetMasterState() == NNodeTrackerClient::ENodeState::Online &&
        execNode->GetSchedulerState() == ENodeState::Online)
    {
        auto it = execNode->RecentlyFinishedJobs().find(jobId);
        if (it == execNode->RecentlyFinishedJobs().end()) {
            YT_LOG_DEBUG("Job release skipped since job has been removed already (JobId: %v, NodeId: %v, NodeAddress: %v)",
                jobId,
                nodeId,
                execNode->GetDefaultAddress());
        } else {
            YT_LOG_DEBUG("Job released and will be removed (JobId: %v, NodeId: %v, NodeAddress: %v, %v)",
                jobId,
                nodeId,
                execNode->GetDefaultAddress(),
                releaseFlags);
            auto& recentlyFinishedJobInfo = it->second;
            recentlyFinishedJobInfo.ReleaseFlags = releaseFlags;
        }
    } else {
        YT_LOG_DEBUG("Execution node was unregistered for a job that should be removed (JobId: %v, NodeId: %v)",
            jobId,
            nodeId);
    }
}

void TNodeShard::BuildNodesYson(TFluentMap fluent)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    for (const auto& [nodeId, node] : IdToNode_) {
        BuildNodeYson(node, fluent);
    }
}

TOperationId TNodeShard::FindOperationIdByJobId(TJobId jobId, bool considerFinished)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = FindJob(jobId);
    if (job) {
        return job->GetOperationId();
    }

    if (!considerFinished) {
        return {};
    }

    auto node = FindNodeByJob(jobId);
    if (!node) {
        return {};
    }

    auto jobIt = node->RecentlyFinishedJobs().find(jobId);
    if (jobIt == node->RecentlyFinishedJobs().end()) {
        return {};
    } else {
        return jobIt->second.OperationId;
    }
}

TNodeShard::TResourceStatistics TNodeShard::CalculateResourceStatistics(const TSchedulingTagFilter& filter)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto descriptors = CachedExecNodeDescriptors_.Acquire();

    TResourceStatistics statistics;
    for (const auto& [nodeId, descriptor] : *descriptors) {
        if (descriptor.Online && descriptor.CanSchedule(filter)) {
            statistics.Usage += descriptor.ResourceUsage;
            statistics.Limits += descriptor.ResourceLimits;
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

int TNodeShard::GetActiveJobCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ActiveJobCount_;
}

int TNodeShard::GetSubmitToStrategyJobCount() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return SubmitToStrategyJobCount_;
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

TFuture<TControllerScheduleJobResultPtr> TNodeShard::BeginScheduleJob(
    TIncarnationId incarnationId,
    TOperationId operationId,
    TJobId jobId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto pair = JobIdToScheduleEntry_.emplace(jobId, TScheduleJobEntry());
    YT_VERIFY(pair.second);

    auto& entry = pair.first->second;
    entry.Promise = NewPromise<TControllerScheduleJobResultPtr>();
    entry.IncarnationId = incarnationId;
    entry.OperationId = operationId;
    entry.OperationIdToJobIdsIterator = OperationIdToJobIterators_.emplace(operationId, pair.first);
    entry.StartTime = GetCpuInstant();

    return entry.Promise.ToFuture();
}

void TNodeShard::EndScheduleJob(const NProto::TScheduleJobResponse& response)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto jobId = FromProto<TJobId>(response.job_id());
    auto operationId = FromProto<TOperationId>(response.operation_id());

    auto it = JobIdToScheduleEntry_.find(jobId);
    if (it == std::cend(JobIdToScheduleEntry_)) {
        YT_LOG_WARNING("No schedule entry for job, probably job was scheduled by controller too late (OperationId: %v, JobId: %v)",
            operationId,
            jobId);
        return;
    }

    auto& entry = it->second;
    YT_VERIFY(operationId == entry.OperationId);

    auto scheduleJobDuration = CpuDurationToDuration(GetCpuInstant() - entry.StartTime);
    if (scheduleJobDuration > Config_->ScheduleJobDurationLoggingThreshold) {
        YT_LOG_DEBUG("Job schedule response received (OperationId: %v, JobId: %v, Success: %v, Duration: %v)",
            operationId,
            jobId,
            response.success(),
            scheduleJobDuration.MilliSeconds());
    }

    auto result = New<TControllerScheduleJobResult>();
    if (response.success()) {
        result->StartDescriptor.emplace(
            jobId,
            FromProto<TJobResourcesWithQuota>(response.resource_limits()),
            response.interruptible());
    }
    for (const auto& protoCounter : response.failed()) {
        result->Failed[static_cast<EScheduleJobFailReason>(protoCounter.reason())] = protoCounter.value();
    }
    FromProto(&result->Duration, response.duration());
    result->IncarnationId = entry.IncarnationId;
    result->ControllerEpoch = response.controller_epoch();

    entry.Promise.Set(std::move(result));

    OperationIdToJobIterators_.erase(entry.OperationIdToJobIdsIterator);
    JobIdToScheduleEntry_.erase(it);
}

void TNodeShard::RemoveOutdatedScheduleJobEntries()
{
    std::vector<TJobId> jobIdsToRemove;
    auto now = TInstant::Now();
    for (const auto& [jobId, entry] : JobIdToScheduleEntry_) {
        if (CpuInstantToInstant(entry.StartTime) + Config_->ScheduleJobEntryRemovalTimeout < now) {
            jobIdsToRemove.push_back(jobId);
        }
    }

    for (auto jobId : jobIdsToRemove) {
        auto it = JobIdToScheduleEntry_.find(jobId);
        if (it == std::cend(JobIdToScheduleEntry_)) {
            return;
        }

        auto& entry = it->second;

        OperationIdToJobIterators_.erase(entry.OperationIdToJobIdsIterator);
        JobIdToScheduleEntry_.erase(it);
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

TControllerEpoch TNodeShard::GetJobControllerEpoch(TJobId jobId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    if (auto job = FindJob(jobId)) {
        return job->GetControllerEpoch();
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

bool TNodeShard::AreNewJobsForbiddenForOperation(const TOperationId operationId) const noexcept
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    const auto& operationState = GetOperationState(operationId);
    return operationState.ForbidNewJobs;
}

int TNodeShard::GetOnGoingHeartbeatsCount() const noexcept
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
}

void TNodeShard::UnregisterAgent(TAgentId id)
{
    YT_LOG_DEBUG("Agent unregistered from node shard (AgentId: %v)", id);

    EraseOrCrash(RegisteredAgents_, id);
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

void TNodeShard::UpdateJobTimeStatisticsIfNeeded(const TJobPtr& job, TRunningJobTimeStatistics timeStatistics)
{
    if (job->GetPreemptibleProgressTime() < timeStatistics.PreemptibleProgressTime) {
        job->SetPreemptibleProgressTime(timeStatistics.PreemptibleProgressTime);
    }
}

void TNodeShard::UpdateRunningJobsStatistics(const std::vector<TRunningJobStatisticsUpdate>& updates)
{
    YT_LOG_DEBUG("Update running job time statistics (UpdateCount: %v)", std::size(updates));

    for (auto [jobId, timeStatistics] : updates) {
        if (const auto& job = FindJob(jobId)) {
            UpdateJobTimeStatisticsIfNeeded(job, timeStatistics);
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

    // We intentionally do not abort jobs here, it will happen when RegistrationLease expired or
    // at node attributes update by separate timeout.
    UpdateNodeState(node, /* newMasterState */ node->GetMasterState(), /* newSchedulerState */ ENodeState::Offline);

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

    AbortAllJobsAtNode(node, EAbortReason::NodeOffline);

    auto jobsToRemove = node->RecentlyFinishedJobs();
    for (const auto& [jobId, job] : jobsToRemove) {
        RemoveRecentlyFinishedJob(jobId);
    }
    YT_VERIFY(node->RecentlyFinishedJobs().empty());

    if (node->GetJobReporterQueueIsTooLarge()) {
        --JobReporterQueueIsTooLargeNodeCount_;
    }

    node->SetSchedulerState(ENodeState::Offline);

    const auto& address = node->GetDefaultAddress();

    ManagerHost_->GetStrategy()->UnregisterNode(node->GetId(), address);

    YT_LOG_INFO("Node unregistered (Address: %v)", address);
}

void TNodeShard::AbortAllJobsAtNode(const TExecNodePtr& node, EAbortReason reason)
{
    if (node->GetHasOngoingHeartbeat()) {
        YT_LOG_INFO("Jobs abortion postponed until heartbeat is finished (Address: %v)",
            node->GetDefaultAddress());
        node->SetPendingJobsAbortionReason(reason);
    } else {
        DoAbortAllJobsAtNode(node, reason);
    }
}

void TNodeShard::DoAbortAllJobsAtNode(const TExecNodePtr& node, EAbortReason reason)
{
    std::vector<TJobId> jobIds;
    for (const auto& job : node->Jobs()) {
        jobIds.push_back(job->GetId());
    }
    const auto& address = node->GetDefaultAddress();
    YT_LOG_DEBUG("Aborting all jobs on a node (Address: %v, Reason: %v, JobIds: %v)",
        address,
        reason,
        jobIds);

    // Make a copy, the collection will be modified.
    auto jobs = node->Jobs();
    auto error = TError("All jobs on the node were aborted by scheduler")
        << TErrorAttribute("abort_reason", reason);
    for (const auto& job : jobs) {
        OnJobAborted(job, error);
    }
}

void TNodeShard::AbortUnconfirmedJobs(
    TOperationId operationId,
    TShardEpoch shardEpoch,
    const std::vector<TJobPtr>& jobs)
{
    const auto* operationState = FindOperationState(operationId);
    if (!operationState || operationState->ShardEpoch != shardEpoch) {
        return;
    }

    std::vector<TJobPtr> unconfirmedJobs;
    for (const auto& job : jobs) {
        if (job->GetWaitingForConfirmation()) {
            unconfirmedJobs.emplace_back(job);
        }
    }

    if (unconfirmedJobs.empty()) {
        YT_LOG_INFO("All revived jobs were confirmed (OperationId: %v, RevivedJobCount: %v)",
            operationId,
            jobs.size());
        return;
    }

    YT_LOG_WARNING("Aborting revived jobs that were not confirmed (OperationId: %v, RevivedJobCount: %v, "
        "JobRevivalAbortTimeout: %v, UnconfirmedJobCount: %v)",
        operationId,
        jobs.size(),
        Config_->JobRevivalAbortTimeout,
        unconfirmedJobs.size());

    auto error = TError("Job not confirmed after timeout")
        << TErrorAttribute("abort_reason", EAbortReason::RevivalConfirmationTimeout);
    for (const auto& job : unconfirmedJobs) {
        YT_LOG_DEBUG("Aborting revived job that was not confirmed (OperationId: %v, JobId: %v)",
            operationId,
            job->GetId());
        OnJobAborted(job, error);
        if (auto node = job->GetNode()) {
            ResetJobWaitingForConfirmation(job);
        }
    }
}

// TODO(eshcherbin): This method has become too big -- gotta split it.
void TNodeShard::ProcessHeartbeatJobs(
    const TExecNodePtr& node,
    TScheduler::TCtxNodeHeartbeat::TTypedRequest* request,
    TScheduler::TCtxNodeHeartbeat::TTypedResponse* response,
    std::vector<TJobPtr>* runningJobs,
    bool* hasWaitingJobs)
{
    YT_VERIFY(runningJobs->empty());

    auto now = GetCpuInstant();

    bool shouldLogOngoingJobs = false;
    auto lastJobsLogTime = node->GetLastJobsLogTime();
    if (!lastJobsLogTime || now > *lastJobsLogTime + DurationToCpuDuration(Config_->JobsLoggingPeriod)) {
        shouldLogOngoingJobs = true;
        node->SetLastJobsLogTime(now);
    }

    bool checkMissingJobs = false;
    auto lastCheckMissingJobsTime = node->GetLastCheckMissingJobsTime();
    if ((!lastCheckMissingJobsTime ||
        now > *lastCheckMissingJobsTime + DurationToCpuDuration(Config_->MissingJobsCheckPeriod)) &&
        node->UnconfirmedJobIds().empty())
    {
        checkMissingJobs = true;
        node->SetLastCheckMissingJobsTime(now);
    }

    const auto& nodeId = node->GetId();
    const auto& nodeAddress = node->GetDefaultAddress();

    if (!node->UnconfirmedJobIds().empty()) {
        YT_LOG_DEBUG("Requesting node to include stored jobs in the next heartbeat (NodeId: %v, NodeAddress: %v)",
            nodeId,
            nodeAddress);
        // If it is a first time we get the heartbeat from a given node,
        // there will definitely be some jobs that are missing. No need to abort
        // them.
        for (const auto& jobId : node->UnconfirmedJobIds()) {
            const auto jobPtr = FindJob(jobId, node);
            auto* operationState = FindOperationState(jobPtr->GetOperationId());
            YT_VERIFY(operationState);

            auto agent = operationState->Controller->FindAgent();
            if (!agent) {
                YT_LOG_DEBUG("Cannot send unconfirmed job since agent is no longer known (JobId: %v, OperationId: %v)",
                    jobPtr->GetId(),
                    jobPtr->GetOperationId());
                continue;
            }

            auto* jobToConfirm = response->add_jobs_to_confirm();
            ToProto(jobToConfirm->mutable_job_id(), jobId);
            SetControllerAgentInfo(agent, jobToConfirm->mutable_controller_agent_descriptor());
        }
    }

    if (checkMissingJobs) {
        for (const auto& job : node->Jobs()) {
            // Verify that all flags are in the initial state.
            YT_VERIFY(!job->GetFoundOnNode());
        }
    }

    THashSet<TJobId> recentlyFinishedJobIdsToRemove;
    {
        auto now = GetCpuInstant();
        for (const auto& [jobId, jobInfo] : node->RecentlyFinishedJobs()) {
            if (jobInfo.ReleaseFlags) {
                YT_LOG_DEBUG("Requesting node to remove released job "
                    "(JobId: %v, NodeId: %v, NodeAddress: %v, %v)",
                    jobId,
                    nodeId,
                    nodeAddress,
                    *jobInfo.ReleaseFlags);
                recentlyFinishedJobIdsToRemove.insert(jobId);
                ToProto(response->add_jobs_to_remove(), NControllerAgent::TJobToRelease{jobId, *jobInfo.ReleaseFlags});
            } else if (now > jobInfo.EvictionDeadline) {
                YT_LOG_DEBUG("Removing job from recently finished due to timeout for release "
                    "(JobId: %v, NodeId: %v, NodeAddress: %v)",
                    jobId,
                    nodeId,
                    nodeAddress);
                recentlyFinishedJobIdsToRemove.insert(jobId);
                ToProto(response->add_jobs_to_remove(), NControllerAgent::TJobToRelease{jobId});
            }
        }
        for (auto jobId : recentlyFinishedJobIdsToRemove) {
            RemoveRecentlyFinishedJob(jobId);
        }
    }

    // COMPAT(pogorelov)
    THashSet<TAllocationId> specFetchFailedAllocations;
    specFetchFailedAllocations.reserve(std::size(request->spec_fetch_failed_allocations()));

    for (const auto& specFetchFailedAllocationProto : request->spec_fetch_failed_allocations()) {
        auto allocationId = NYT::FromProto<TAllocationId>(specFetchFailedAllocationProto.allocation_id());
        auto operationId = NYT::FromProto<TOperationId>(specFetchFailedAllocationProto.operation_id());
        auto specFetchError = NYT::FromProto<TError>(specFetchFailedAllocationProto.error());

        YT_LOG_DEBUG(
            "Node has failed to get job spec, abort job (JobId: %v, OperationId: %v, Error: %v)",
            allocationId,
            operationId,
            specFetchError);

        if (auto job = FindJob(allocationId)) {
            auto error = (TError("Failed to get job spec")
                << TErrorAttribute("abort_reason", EAbortReason::GetSpecFailed))
                << specFetchError;
            OnJobAborted(job, error, EAbortReason::GetSpecFailed);
        }

        specFetchFailedAllocations.emplace(allocationId);
    }

    // Used for debug logging.
    TAllocationStateToJobList ongoingJobsByAllocationState;
    std::vector<TJobId> recentlyFinishedJobIdsToLog;
    for (auto& jobStatus : *MutableJobs(request)) {
        auto jobId = GetJobId(&jobStatus);

        // COMPAT(pogorelov)
        if (specFetchFailedAllocations.contains(jobId)) {
            continue;
        }

        TJobPtr job;
        try {
            job = ProcessJobHeartbeat(
                node,
                recentlyFinishedJobIdsToRemove,
                response,
                &jobStatus);
        } catch (const std::exception& ex) {
            if (Config_->CrashOnJobHeartbeatProcessingException) {
                YT_LOG_FATAL(ex, "Failed to process job heartbeat (JobId: %v)", jobId);
            } else {
                YT_LOG_WARNING(ex, "Failed to process job heartbeat (JobId: %v)", jobId);
                throw;
            }
        }

        if (job) {
            if (checkMissingJobs) {
                job->SetFoundOnNode(true);
            }
            switch (job->GetAllocationState()) {
                case EAllocationState::Running: {
                    runningJobs->push_back(job);
                    ongoingJobsByAllocationState[job->GetAllocationState()].push_back(job);
                    break;
                }
                case EAllocationState::Waiting:
                    *hasWaitingJobs = true;
                    ongoingJobsByAllocationState[job->GetAllocationState()].push_back(job);
                    break;
                default:
                    break;
            }
        } else {
            auto jobId = GetJobId(&jobStatus);
            auto operationId = FromProto<TOperationId>(jobStatus.operation_id());
            auto operation = FindOperationState(operationId);
            if (!(operation && operation->OperationUnreadyLoggedJobIds.contains(jobId))
                && node->RecentlyFinishedJobs().contains(jobId))
            {
                recentlyFinishedJobIdsToLog.push_back(jobId);
            }
        }
    }
    HeartbeatJobCount_.Increment(GetJobs(request).size());
    HeartbeatCount_.Increment();

    YT_LOG_DEBUG_UNLESS(
        recentlyFinishedJobIdsToLog.empty(),
        "Jobs are skipped since they were recently finished and are currently being stored "
        "(JobIds: %v)",
        recentlyFinishedJobIdsToLog);

    if (shouldLogOngoingJobs) {
        LogOngoingJobsAt(CpuInstantToInstant(now), node, ongoingJobsByAllocationState);
    }

    if (checkMissingJobs) {
        std::vector<TJobPtr> missingJobs;
        for (const auto& job : node->Jobs()) {
            // Jobs that are waiting for confirmation may never be considered missing.
            // They are removed in two ways: by explicit unconfirmation of the node
            // or after revival confirmation timeout.
            YT_VERIFY(!job->GetWaitingForConfirmation());
            if (!job->GetFoundOnNode()) {
                // This situation is possible if heartbeat from node has timed out,
                // but we have scheduled some jobs.
                // TODO(ignat):  YT-15875: consider deadline from node.
                YT_LOG_INFO("Job is missing (Address: %v, JobId: %v, OperationId: %v)",
                    node->GetDefaultAddress(),
                    job->GetId(),
                    job->GetOperationId());
                missingJobs.push_back(job);
            } else {
                job->SetFoundOnNode(false);
            }
        }

        auto error = TError("Job vanished")
            << TErrorAttribute("abort_reason", EAbortReason::Vanished);
        for (const auto& job : missingJobs) {
            YT_LOG_DEBUG("Aborting vanished job (JobId: %v, OperationId: %v)", job->GetId(), job->GetOperationId());
            OnJobAborted(job, error);
        }
    }

    auto error = TError("Job not confirmed by node")
        << TErrorAttribute("abort_reason", EAbortReason::Unconfirmed);
    for (auto jobId : FromProto<std::vector<TJobId>>(UnconfirmedJobs(request))) {
        auto job = FindJob(jobId);
        if (!job) {
            // This may happen if we received heartbeat after job was removed by some different reasons
            // (like confirmation timeout).
            continue;
        }

        YT_LOG_DEBUG("Aborting unconfirmed job (JobId: %v, OperationId: %v)", jobId, job->GetOperationId());
        OnJobAborted(job, error);

        ResetJobWaitingForConfirmation(job);
    }
}

void TNodeShard::LogOngoingJobsAt(TInstant now, const TExecNodePtr& node, const TAllocationStateToJobList& ongoingJobsByAllocationState) const
{
    const auto& strategy = ManagerHost_->GetStrategy();
    for (auto allocationState : TEnumTraits<EAllocationState>::GetDomainValues()) {
        const auto& jobs = ongoingJobsByAllocationState[allocationState];
        if (jobs.empty()) {
            continue;
        }

        TStringBuilder attributesBuilder;
        TDelimitedStringBuilderWrapper delimitedAttributesBuilder(&attributesBuilder);
        strategy->BuildSchedulingAttributesStringForOngoingJobs(node->GetDefaultAddress(), node->Tags(), jobs, now, delimitedAttributesBuilder);

        YT_LOG_DEBUG("Jobs are %lv (%v)",
            allocationState,
            attributesBuilder.Flush());
    }
}

template <class TRspHeartbeat, class TStatus>
TJobPtr TNodeShard::ProcessJobHeartbeat(
    const TExecNodePtr& node,
    const THashSet<TJobId>& recentlyFinishedJobIdsToRemove,
    TRspHeartbeat* response,
    TStatus* jobStatus)
{
    auto jobId = GetJobId(jobStatus);
    auto operationId = FromProto<TOperationId>(jobStatus->operation_id());

    auto allocationState = GetAllocationState(jobStatus);

    const auto& address = node->GetDefaultAddress();

    auto job = FindJob(jobId, node);
    auto operationState = FindOperationState(operationId);

    if (!job) {
        auto Logger = SchedulerLogger.WithTag("Address: %v, JobId: %v, OperationId: %v, AllocationState: %v",
            address,
            jobId,
            operationId,
            allocationState);

        // We can decide what to do with the job of an operation only when all
        // TJob structures of the operation are materialized. Also we should
        // not remove the completed jobs that were not saved to the snapshot.
        if ((operationState && !operationState->JobsReady) ||
            WaitingForRegisterOperationIds_.contains(operationId))
        {
            if (operationState && !operationState->OperationUnreadyLoggedJobIds.contains(jobId)) {
                YT_LOG_DEBUG("Job is skipped since operation jobs are not ready yet");
                operationState->OperationUnreadyLoggedJobIds.insert(jobId);
            }
            return nullptr;
        }

        if (node->RecentlyFinishedJobs().contains(jobId) || recentlyFinishedJobIdsToRemove.contains(jobId)) {
            // NB(eshcherbin): This event is logged one level above.
            return nullptr;
        }

        // We don't abort unknown allocations currently when ControlJobLifetimeAtScheduler option is turned off, since CA will abort such jobs.
        // We will abort such allocations when allocation can run many jobs.
        switch (allocationState) {
            case EAllocationState::Finished:
                if ((operationState && operationState->ControlJobLifetimeAtScheduler) || Config_->ControlUnknownOperationJobsLifetime) {
                    ToProto(response->add_jobs_to_remove(), {jobId});
                    YT_LOG_DEBUG("Unknown job has finished, remove it");
                } else {
                    YT_LOG_DEBUG("Unknown job has finished");
                }
                break;

            case EAllocationState::Running:
                if ((operationState && operationState->ControlJobLifetimeAtScheduler) || Config_->ControlUnknownOperationJobsLifetime) {
                    AddAllocationToAbort(response, {jobId, /*AbortReason*/ std::nullopt});

                    YT_LOG_DEBUG("Unknown job is running, abort it");
                } else {
                    YT_LOG_DEBUG("Unknown job is running");
                }
                break;

            case EAllocationState::Waiting:
                if ((operationState && operationState->ControlJobLifetimeAtScheduler) || Config_->ControlUnknownOperationJobsLifetime) {
                    AddAllocationToAbort(response, {jobId, /*AbortReason*/ std::nullopt});

                    YT_LOG_DEBUG("Unknown job is waiting, abort it");
                } else {
                    YT_LOG_DEBUG("Unknown job is waiting");
                }
                break;

            case EAllocationState::Finishing:
                YT_LOG_DEBUG("Unknown job is finishing");
                break;

            default:
                YT_ABORT();
        }
        return nullptr;
    }

    auto guard = TCodicilGuard{job->CodicilString()};

    const auto& Logger = job->Logger();

    YT_VERIFY(operationState);

    // Check if the job is running on a proper node.
    if (node->GetId() != job->GetNode()->GetId()) {
        // Job has moved from one node to another. No idea how this could happen.
        switch (allocationState) {
            case EAllocationState::Finishing:
                // Job is already finishing, do nothing.
                break;
            case EAllocationState::Finished:
                if (operationState->ControlJobLifetimeAtScheduler) {
                    ToProto(response->add_jobs_to_remove(), {jobId});
                }
                YT_LOG_WARNING(
                    "Job status report was expected from other node (ExpectedAddress: %v)",
                    node->GetDefaultAddress());
                break;
            case EAllocationState::Waiting:
            case EAllocationState::Running:
                AddAllocationToAbort(response, {jobId, EAbortReason::JobOnUnexpectedNode});
                YT_LOG_WARNING(
                    "Job status report was expected from other node, abort scheduled (ExpectedAddress: %v)",
                    node->GetDefaultAddress());
                break;
            default:
                YT_ABORT();
        }
        return nullptr;
    }

    if (job->GetWaitingForConfirmation()) {
        YT_LOG_DEBUG("Job confirmed (AllocationState: %v)", allocationState);
        ResetJobWaitingForConfirmation(job);
    }

    bool stateChanged = allocationState != job->GetAllocationState();

    switch (allocationState) {
        case EAllocationState::Finished: {
            if (auto error = FromProto<TError>(jobStatus->result().error());
                ParseAbortReason(error, jobId, Logger).value_or(EAbortReason::Scheduler) == EAbortReason::GetSpecFailed)
            {
                YT_LOG_DEBUG("Node has failed to get job spec, abort job");

                OnJobAborted(job, error, EAbortReason::GetSpecFailed);
                ToProto(response->add_jobs_to_remove(), {jobId});
            } else {
                YT_LOG_DEBUG("Job finished, storage scheduled");

                if (operationState->ControlJobLifetimeAtScheduler) {
                    AddRecentlyFinishedJob(job);
                    ToProto(response->add_jobs_to_store(), jobId);
                }
                OnJobFinished(job);
            }

            break;
        }

        case EAllocationState::Running:
        case EAllocationState::Waiting:
            if (stateChanged) {
                SetAllocationState(job, allocationState);
            }
            switch (allocationState) {
                case EAllocationState::Running:
                    YT_LOG_DEBUG_IF(stateChanged, "Job is now running");
                    OnJobRunning(job, jobStatus);
                    break;

                case EAllocationState::Waiting:
                    YT_LOG_DEBUG_IF(stateChanged, "Job is now waiting");
                    break;
                default:
                    YT_ABORT();
            }

            if (job->GetFailRequested()) {
                if (allocationState == EAllocationState::Running) {
                    YT_LOG_DEBUG("Job fail requested");
                    ToProto(response->add_jobs_to_fail(), jobId);
                }
            } else if (job->IsInterrupted()) {
                SendInterruptedJobToNode(
                    response,
                    job,
                    CpuDurationToDuration(job->GetInterruptionTimeout()));
            }

            break;

        case EAllocationState::Finishing:
            YT_LOG_DEBUG("Job is finishing");
            break;

        default:
            YT_ABORT();
    }

    return job;
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
        // Check that we succesfully reset all resource limits to zero for node with zero user slots.
        YT_VERIFY(node->GetResourceLimits() == TJobResources());
    }
}

void TNodeShard::UpdateNodeResources(
    const TExecNodePtr& node,
    const TJobResources& limits,
    const TJobResources& usage,
    const NNodeTrackerClient::NProto::TDiskResources& diskResources)
{
    auto oldResourceLimits = node->GetResourceLimits();

    YT_VERIFY(node->GetSchedulerState() == ENodeState::Online);

    if (limits.GetUserSlots() > 0) {
        if (node->GetResourceLimits().GetUserSlots() == 0 && node->GetMasterState() == NNodeTrackerClient::ENodeState::Online) {
            ExecNodeCount_ += 1;
        }
        node->SetResourceLimits(limits);
        node->SetResourceUsage(usage);
        node->SetDiskResources(diskResources);
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

    ConcurrentHeartbeatCount_ += 1;
}

void TNodeShard::EndNodeHeartbeatProcessing(const TExecNodePtr& node)
{
    YT_VERIFY(node->GetHasOngoingHeartbeat());

    TForbidContextSwitchGuard guard;

    node->SetHasOngoingHeartbeat(false);

    ConcurrentHeartbeatCount_ -= 1;
    node->SetLastSeenTime(TInstant::Now());

    if (node->GetPendingJobsAbortionReason()) {
        DoAbortAllJobsAtNode(node, *node->GetPendingJobsAbortionReason());
        node->SetPendingJobsAbortionReason({});
    }

    if (node->GetHasPendingUnregistration()) {
        DoUnregisterNode(node);
    }
}

template <class TCtxNodeHeartbeatPtr>
void TNodeShard::ProcessScheduledAndPreemptedJobs(
    const ISchedulingContextPtr& schedulingContext,
    const TCtxNodeHeartbeatPtr& rpcContext)
{
    auto* response = &rpcContext->Response();

    std::vector<TJobId> startedJobs;
    for (const auto& job : schedulingContext->StartedJobs()) {
        auto* operationState = FindOperationState(job->GetOperationId());
        if (!operationState) {
            YT_LOG_DEBUG("Job cannot be started since operation is no longer known (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());
            continue;
        }

        if (operationState->ForbidNewJobs) {
            YT_LOG_DEBUG("Job cannot be started since new jobs are forbidden (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());
            if (!operationState->ControllerTerminated) {
                const auto& controller = operationState->Controller;
                controller->OnNonscheduledJobAborted(
                    job->GetId(),
                    EAbortReason::SchedulingOperationSuspended,
                    job->GetControllerEpoch());
                JobsToSubmitToStrategy_[job->GetId()] = TJobUpdate{
                    EJobUpdateStatus::Finished,
                    job->GetOperationId(),
                    job->GetId(),
                    job->GetTreeId(),
                    TJobResources(),
                    job->GetNode()->NodeDescriptor().GetDataCenter(),
                    job->GetNode()->GetInfinibandCluster()
                };
                operationState->JobsToSubmitToStrategy.insert(job->GetId());
            }
            continue;
        }

        const auto& controller = operationState->Controller;
        auto agent = controller->FindAgent();
        if (!agent) {
            YT_LOG_DEBUG("Cannot start job: agent is no longer known (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());
            continue;
        }
        if (agent->GetIncarnationId() != job->GetIncarnationId()) {
            YT_LOG_DEBUG("Cannot start job: wrong agent incarnation (JobId: %v, OperationId: %v, ExpectedIncarnationId: %v, "
                "ActualIncarnationId: %v)",
                job->GetId(),
                job->GetOperationId(),
                job->GetIncarnationId(),
                agent->GetIncarnationId());
            continue;
        }

        RegisterJob(job);

        auto* startInfo = AddJobsToStart(response);
        ToProto(MutableJobId(startInfo), job->GetId());
        ToProto(startInfo->mutable_operation_id(), job->GetOperationId());
        *startInfo->mutable_resource_limits() = ToNodeResources(job->ResourceUsage());

        SetControllerAgentInfo(agent, startInfo->mutable_controller_agent_descriptor());
    }

    for (const auto& preemptedJob : schedulingContext->PreemptedJobs()) {
        auto& job = preemptedJob.Job;
        auto interruptTimeout = preemptedJob.InterruptTimeout;
        if (!FindOperationState(job->GetOperationId()) || job->GetUnregistered()) {
            YT_LOG_DEBUG("Cannot preempt job since operation is no longer known or the job is unregistered (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());
            continue;
        }

        ProcessPreemptedJob(response, job, interruptTimeout);
    }
}

template <class TJobStatus>
void TNodeShard::OnJobRunning(const TJobPtr& job, TJobStatus* status)
{
    YT_VERIFY(status);

    auto timeStatistics = FromProto<NJobAgent::TTimeStatistics>(status->time_statistics());
    UpdateJobTimeStatisticsIfNeeded(job, TRunningJobTimeStatistics{
        timeStatistics.ExecDuration.value_or(TDuration{}) +
        timeStatistics.PrepareDuration.value_or(TDuration{})});

    auto now = GetCpuInstant();
    if (now < job->GetRunningJobUpdateDeadline()) {
        return;
    }
    job->SetRunningJobUpdateDeadline(now + DurationToCpuDuration(Config_->RunningJobsUpdatePeriod));

    job->ResourceUsage() = ToJobResources(status->resource_usage());

    YT_VERIFY(Dominates(job->ResourceUsage(), TJobResources()));

    auto* operationState = FindOperationState(job->GetOperationId());
    if (operationState) {
        auto it = JobsToSubmitToStrategy_.find(job->GetId());
        if (it == JobsToSubmitToStrategy_.end() || it->second.Status != EJobUpdateStatus::Finished) {
            JobsToSubmitToStrategy_[job->GetId()] = TJobUpdate{
                EJobUpdateStatus::Running,
                job->GetOperationId(),
                job->GetId(),
                job->GetTreeId(),
                job->ResourceUsage(),
                job->GetNode()->NodeDescriptor().GetDataCenter(),
                job->GetNode()->GetInfinibandCluster()
            };

            operationState->JobsToSubmitToStrategy.insert(job->GetId());
        }
    }
}

void TNodeShard::OnJobFinished(const TJobPtr& job)
{
    if (const auto allocationState = job->GetAllocationState();
        allocationState == EAllocationState::Finishing ||
        allocationState == EAllocationState::Finished)
    {
        return;
    }

    SetFinishedState(job);

    auto* operationState = FindOperationState(job->GetOperationId());
    if (operationState) {
        const auto& controller = operationState->Controller;
        controller->OnJobFinished(job);
    }

    UnregisterJob(job);
}

void TNodeShard::OnJobAborted(const TJobPtr& job, const TError& error, std::optional<EAbortReason> abortReason)
{
    YT_VERIFY(!error.IsOK());

    if (const auto allocationState = job->GetAllocationState();
        allocationState == EAllocationState::Finishing ||
        allocationState == EAllocationState::Finished)
    {
        return;
    }

    SetFinishedState(job);

    auto* operationState = FindOperationState(job->GetOperationId());
    if (operationState) {
        const auto& controller = operationState->Controller;
        controller->OnJobAborted(job, error, /*scheduled*/ true, abortReason);
    }

    UnregisterJob(job);
}

void TNodeShard::DoAbandonJob(const TJobPtr& job)
{
    if (auto allocationState = job->GetAllocationState();
        allocationState == EAllocationState::Finishing ||
        allocationState == EAllocationState::Finished)
    {
        return;
    }

    SetFinishedState(job);

    UnregisterJob(job);
}

void TNodeShard::SubmitJobsToStrategy()
{
    YT_PROFILE_TIMING("/scheduler/strategy_job_processing_time") {
        if (!JobsToSubmitToStrategy_.empty()) {
            THashSet<TJobId> jobsToPostpone;
            std::vector<TJobId> jobsToAbort;
            auto jobUpdates = GetValues(JobsToSubmitToStrategy_);
            ManagerHost_->GetStrategy()->ProcessJobUpdates(
                jobUpdates,
                &jobsToPostpone,
                &jobsToAbort);

            for (auto jobId : jobsToAbort) {
                AbortJob(jobId, TError("Aborting job by strategy request"));
            }

            std::vector<std::pair<TOperationId, TJobId>> jobsToRemove;
            for (const auto& job : jobUpdates) {
                if (!jobsToPostpone.contains(job.JobId)) {
                    jobsToRemove.emplace_back(job.OperationId, job.JobId);
                }
            }

            for (const auto& [operationId, jobId] : jobsToRemove) {
                auto* operationState = FindOperationState(operationId);
                if (operationState) {
                    operationState->JobsToSubmitToStrategy.erase(jobId);
                }

                EraseOrCrash(JobsToSubmitToStrategy_, jobId);
            }
        }
        SubmitToStrategyJobCount_.store(JobsToSubmitToStrategy_.size());
    }
}

void TNodeShard::UpdateProfilingCounter(const TJobPtr& job, int value)
{
    const auto allocationState = job->GetAllocationState();

    // Decrement started job counter here when it will be moved to CA.
    if (allocationState == EAllocationState::Scheduled) {
        return;
    }
    YT_VERIFY(allocationState == EAllocationState::Running || allocationState == EAllocationState::Waiting);

    auto createGauge = [&] {
        return SchedulerProfiler.WithTags(TTagSet(TTagList{
                {ProfilingPoolTreeKey, job->GetTreeId()},
                {"state", FormatEnum(allocationState)}}))
            .Gauge("/allocations/running_allocation_count");
    };

    auto it = AllocationCounter_.find(allocationState);
    if (it == AllocationCounter_.end()) {
        it = AllocationCounter_.emplace(
            allocationState,
            std::make_pair(
                0,
                createGauge())).first;
    }

    auto& [count, gauge] = it->second;
    count += value;
    gauge.Update(count);
}

void TNodeShard::SetAllocationState(const TJobPtr& job, const EAllocationState state)
{
    YT_VERIFY(state != EAllocationState::Scheduled);

    UpdateProfilingCounter(job, -1);
    job->SetAllocationState(state);
    UpdateProfilingCounter(job, 1);
}

void TNodeShard::SetFinishedState(const TJobPtr& job)
{
    UpdateProfilingCounter(job, -1);
    job->SetAllocationState(EAllocationState::Finished);
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

        SetControllerAgentInfo(agent, protoOperationInfo->mutable_controller_agent_descriptor());
    }
}

void TNodeShard::SetMinSpareResources(
    TScheduler::TCtxNodeHeartbeat::TTypedResponse* response)
{
    auto minSpareResources = Config_->MinSpareJobResourcesOnNode
        ? ToJobResources(*Config_->MinSpareJobResourcesOnNode, TJobResources())
        : TJobResources();
    ToProto(response->mutable_min_spare_resources(), minSpareResources);
}

void TNodeShard::AddRegisteredControllerAgentsToResponse(auto* response)
{
    for (const auto& [agentId, agentInfo] : RegisteredAgents_) {
        auto agentDescriptorProto = response->add_registered_controller_agents();
        SetControllerAgentInfo(agentId, agentInfo.Addresses, agentInfo.IncarnationId, agentDescriptorProto);
    }

    HeartbeatRegesteredControllerAgentsBytes_.Increment(
        response->registered_controller_agents().SpaceUsedExcludingSelfLong());
}

void TNodeShard::RegisterJob(const TJobPtr& job)
{
    auto& operationState = GetOperationState(job->GetOperationId());

    auto node = job->GetNode();

    YT_VERIFY(operationState.Jobs.emplace(job->GetId(), job).second);
    YT_VERIFY(node->Jobs().insert(job).second);
    YT_VERIFY(node->IdToJob().emplace(job->GetId(), job).second);
    ++ActiveJobCount_;

    YT_LOG_DEBUG("Job registered (JobId: %v, Revived: %v, OperationId: %v, ControllerEpoch: %v, SchedulingIndex: %v)",
        job->GetId(),
        job->IsRevived(),
        job->GetOperationId(),
        job->GetControllerEpoch(),
        job->GetSchedulingIndex());
}

void TNodeShard::UnregisterJob(const TJobPtr& job, bool enableLogging)
{
    if (job->GetUnregistered()) {
        return;
    }

    job->SetUnregistered(true);

    auto* operationState = FindOperationState(job->GetOperationId());
    const auto& node = job->GetNode();

    EraseOrCrash(node->Jobs(), job);
    EraseOrCrash(node->IdToJob(), job->GetId());
    --ActiveJobCount_;

    ResetJobWaitingForConfirmation(job);

    if (operationState && operationState->Jobs.erase(job->GetId())) {
        JobsToSubmitToStrategy_[job->GetId()] =
            TJobUpdate{
                EJobUpdateStatus::Finished,
                job->GetOperationId(),
                job->GetId(),
                job->GetTreeId(),
                TJobResources(),
                job->GetNode()->NodeDescriptor().GetDataCenter(),
                job->GetNode()->GetInfinibandCluster()};
        operationState->JobsToSubmitToStrategy.insert(job->GetId());

        YT_LOG_DEBUG_IF(enableLogging, "Job unregistered (JobId: %v, OperationId: %v)",
            job->GetId(),
            job->GetOperationId());
    } else {
        YT_LOG_DEBUG_IF(enableLogging, "Dangling job unregistered (JobId: %v, OperationId: %v)",
            job->GetId(),
            job->GetOperationId());
    }
}

void TNodeShard::SetJobWaitingForConfirmation(const TJobPtr& job)
{
    job->SetWaitingForConfirmation(true);
    job->GetNode()->UnconfirmedJobIds().insert(job->GetId());
}

void TNodeShard::ResetJobWaitingForConfirmation(const TJobPtr& job)
{
    job->SetWaitingForConfirmation(false);
    job->GetNode()->UnconfirmedJobIds().erase(job->GetId());
}

void TNodeShard::AddRecentlyFinishedJob(const TJobPtr& job)
{
    auto jobId = job->GetId();
    auto node = FindNodeByJob(jobId);
    YT_VERIFY(node);

    auto *operationState = FindOperationState(job->GetOperationId());
    if (operationState) {
        auto finishedStoringEvictionDeadline =
            GetCpuInstant() + DurationToCpuDuration(Config_->FinishedJobStoringTimeout);
        EmplaceOrCrash(
            node->RecentlyFinishedJobs(),
            jobId,
            TRecentlyFinishedJobInfo{
                .OperationId = job->GetOperationId(),
                .EvictionDeadline = finishedStoringEvictionDeadline,
                .ReleaseFlags = std::nullopt
            });
        EmplaceOrCrash(operationState->RecentlyFinishedJobIds, jobId);
    }
}

void TNodeShard::RemoveRecentlyFinishedJob(TJobId jobId)
{
    auto node = FindNodeByJob(jobId);
    YT_VERIFY(node);

    auto it = node->RecentlyFinishedJobs().find(jobId);
    if (it != node->RecentlyFinishedJobs().end()) {
        const auto& jobInfo = it->second;
        auto* operationState = FindOperationState(jobInfo.OperationId);
        if (operationState) {
            operationState->RecentlyFinishedJobIds.erase(jobId);
        }
        node->RecentlyFinishedJobs().erase(it);
    }
}

void TNodeShard::SetOperationJobsReleaseDeadline(TOperationState* operationState)
{
    if (!operationState->ControlJobLifetimeAtScheduler) {
        return;
    }

    auto storingEvictionDeadline = GetCpuInstant() + DurationToCpuDuration(Config_->FinishedOperationJobStoringTimeout);

    for (auto jobId : operationState->RecentlyFinishedJobIds) {
        auto node = FindNodeByJob(jobId);
        YT_VERIFY(node);

        auto& finishedJobInfo = GetOrCrash(node->RecentlyFinishedJobs(), jobId);
        finishedJobInfo.EvictionDeadline = storingEvictionDeadline;
    }

    operationState->RecentlyFinishedJobIds.clear();
}

template <class TRspHeartbeat>
void TNodeShard::SendInterruptedJobToNode(
    TRspHeartbeat* response,
    const TJobPtr& job,
    TDuration interruptTimeout) const
{
    YT_LOG_DEBUG(
        "Add job to interrupt (JobId: %v, InterruptionReason: %v, InterruptTimeout: %v)",
        job->GetId(),
        job->GetInterruptionReason(),
        interruptTimeout);
    AddJobToInterrupt(
        response,
        job->GetId(),
        interruptTimeout,
        job->GetInterruptionReason(),
        job->GetPreemptionReason(),
        job->GetPreemptedFor());
}

template <class TRspHeartbeat>
void TNodeShard::ProcessPreemptedJob(TRspHeartbeat* response, const TJobPtr& job, TDuration interruptTimeout)
{
    PreemptJob(job, DurationToCpuDuration(interruptTimeout));
    SendInterruptedJobToNode(response, job, interruptTimeout);
}

void TNodeShard::PreemptJob(const TJobPtr& job, TCpuDuration interruptTimeout)
{
    YT_LOG_DEBUG("Preempting job (JobId: %v, OperationId: %v, TreeId: %v, Interruptible: %v, Reason: %v)",
        job->GetId(),
        job->GetOperationId(),
        job->GetTreeId(),
        job->GetInterruptible(),
        job->GetPreemptionReason());

    DoInterruptJob(job, EInterruptReason::Preemption, interruptTimeout);
}

// TODO(pogorelov): Refactor interruption
void TNodeShard::DoInterruptJob(
    const TJobPtr& job,
    EInterruptReason reason,
    TCpuDuration interruptTimeout,
    const std::optional<TString>& interruptUser)
{
    YT_VERIFY(reason != EInterruptReason::None);

    YT_LOG_DEBUG("Interrupting job (Reason: %v, InterruptionTimeout: %.3g, JobId: %v, OperationId: %v, User: %v)",
        reason,
        CpuDurationToDuration(interruptTimeout).SecondsFloat(),
        job->GetId(),
        job->GetOperationId(),
        interruptUser);

    job->SetInterruptionReason(reason);

    if (interruptTimeout != 0) {
        job->SetInterruptionTimeout(interruptTimeout);
    }
}

void TNodeShard::InterruptJob(TJobId jobId, EInterruptReason reason)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = FindJob(jobId);
    if (job) {
        DoInterruptJob(job, reason);
    }
}

TExecNodePtr TNodeShard::FindNodeByJob(TJobId jobId)
{
    auto nodeId = NodeIdFromJobId(jobId);
    auto it = IdToNode_.find(nodeId);
    return it == IdToNode_.end() ? nullptr : it->second;
}

TJobPtr TNodeShard::FindJob(TJobId jobId, const TExecNodePtr& node)
{
    const auto& idToJob = node->IdToJob();
    auto it = idToJob.find(jobId);
    return it == idToJob.end() ? nullptr : it->second;
}

TJobPtr TNodeShard::FindJob(TJobId jobId)
{
    auto node = FindNodeByJob(jobId);
    if (!node) {
        return nullptr;
    }
    return FindJob(jobId, node);
}

TJobPtr TNodeShard::GetJobOrThrow(TJobId jobId)
{
    auto job = FindJob(jobId);
    if (!job) {
        THROW_ERROR_EXCEPTION(
            NScheduler::EErrorCode::NoSuchJob,
            "No such job %v",
            jobId);
    }
    return job;
}

TJobProberServiceProxy TNodeShard::CreateJobProberProxy(const TJobPtr& job)
{
    const auto& address = job->GetNode()->NodeDescriptor().GetAddressOrThrow(Bootstrap_->GetLocalNetworks());
    return ManagerHost_->CreateJobProberProxy(address);
}

TNodeShard::TOperationState* TNodeShard::FindOperationState(TOperationId operationId) noexcept
{
    return const_cast<TNodeShard::TOperationState*>(const_cast<const TNodeShard*>(this)->FindOperationState(operationId));
}

const TNodeShard::TOperationState* TNodeShard::FindOperationState(TOperationId operationId) const noexcept
{
    auto it = IdToOpertionState_.find(operationId);
    return it != IdToOpertionState_.end() ? &it->second : nullptr;
}

TNodeShard::TOperationState& TNodeShard::GetOperationState(TOperationId operationId) noexcept
{
    return const_cast<TNodeShard::TOperationState&>(const_cast<const TNodeShard*>(this)->GetOperationState(operationId));
}

const TNodeShard::TOperationState& TNodeShard::GetOperationState(TOperationId operationId) const noexcept
{
    return GetOrCrash(IdToOpertionState_, operationId);
}

void TNodeShard::BuildNodeYson(const TExecNodePtr& node, TFluentMap fluent)
{
    const auto& strategy = ManagerHost_->GetStrategy();
    fluent
        .Item(node->GetDefaultAddress()).BeginMap()
            .Do([&] (TFluentMap fluent) {
                node->BuildAttributes(fluent);
            })
            .Do([&] (TFluentMap fluent) {
                strategy->BuildSchedulingAttributesForNode(node->GetId(), node->GetDefaultAddress(), node->Tags(), fluent);
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
