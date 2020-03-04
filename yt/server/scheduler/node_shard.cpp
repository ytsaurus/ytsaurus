#include "node_shard.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "operation_controller.h"
#include "controller_agent.h"
#include "bootstrap.h"
#include "helpers.h"

#include <yt/server/lib/exec_agent/public.h>

#include <yt/server/lib/scheduler/config.h>
#include <yt/server/lib/scheduler/helpers.h>

#include <yt/server/lib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/server/lib/shell/config.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/job_proxy/public.h>

#include <yt/ytlib/job_tracker_client/proto/job_tracker_service.pb.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/finally.h>

#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;
using namespace NCypressClient;
using namespace NConcurrency;
using namespace NJobProberClient;
using namespace NJobTrackerClient;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NShell;
using namespace NYTree;
using namespace NYson;

using NJobTrackerClient::TReleaseJobFlags;
using NNodeTrackerClient::TNodeId;
using NScheduler::NProto::TSchedulerJobResultExt;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = SchedulerProfiler;

static NProfiling::TAggregateGauge AnalysisTimeCounter;
static NProfiling::TAggregateGauge StrategyJobProcessingTimeCounter;
static NProfiling::TAggregateGauge ScheduleTimeCounter;
static NProfiling::TAggregateGauge GracefulPreemptionTimeCounter;

////////////////////////////////////////////////////////////////////////////////

namespace {
    TTagId GetTreeProfilingTag(const TString& treeId)
    {
        static THashMap<TString, TTagId> tagIds;

        auto it = tagIds.find(treeId);
        if (it == tagIds.end()) {
            it = tagIds.emplace(
                treeId,
                TProfileManager::Get()->RegisterTag("tree", treeId)
            ).first;
        }
        return it->second;
    }

    TTagId GetJobErrorProfilingTag(const TString& jobError)
    {
        static THashMap<TString, TTagId> tagIds;

        auto it = tagIds.find(jobError);
        if (it == tagIds.end()) {
            it = tagIds.emplace(
                jobError,
                TProfileManager::Get()->RegisterTag("job_error", jobError)
            ).first;
        }
        return it->second;
    }

    TMonotonicCounter GetJobErrorCounter(const TString& treeId, const TString& jobError)
    {
        static THashMap<std::tuple<TString, TString>, TMonotonicCounter> counters;

        auto pair = std::make_tuple(treeId, jobError);
        auto it = counters.find(pair);
        if (it == counters.end()) {
            TTagIdList tags;
            tags.push_back(GetTreeProfilingTag(treeId));
            tags.push_back(GetJobErrorProfilingTag(jobError));

            it = counters.emplace(
                pair,
                TMonotonicCounter("/aborted_job_errors", tags)
            ).first;
        }

        return it->second;
    }

    void ProfileAbortedJobErrors(const TJobPtr& job, const TError& error)
    {
        auto checkErrorCode = [&] (auto errorCode) {
            if (error.FindMatching(errorCode)) {
                auto counter = GetJobErrorCounter(job->GetTreeId(), FormatEnum(errorCode));
                Profiler.Increment(counter);
            }
        };

        checkErrorCode(NRpc::EErrorCode::TransportError);
        checkErrorCode(NNet::EErrorCode::ResolveTimedOut);
    }
}

////////////////////////////////////////////////////////////////////////////////

TNodeShard::TNodeShard(
    int id,
    TSchedulerConfigPtr config,
    INodeShardHost* host,
    TBootstrap* bootstrap)
    : Id_(id)
    , Config_(std::move(config))
    , Host_(host)
    , Bootstrap_(bootstrap)
    , ActionQueue_(New<TActionQueue>(Format("NodeShard:%v", id)))
    , CachedExecNodeDescriptorsRefresher_(New<TPeriodicExecutor>(
        GetInvoker(),
        BIND(&TNodeShard::UpdateExecNodeDescriptors, MakeWeak(this)),
        Config_->NodeShardExecNodesCacheUpdatePeriod))
    , CachedResourceStatisticsByTags_(New<TSyncExpiringCache<TSchedulingTagFilter, TResourceStatistics>>(
        BIND(&TNodeShard::CalculateResourceStatistics, MakeStrong(this)),
        Config_->SchedulingTagFilterExpireTimeout,
        GetInvoker()))
    , Logger(NLogging::TLogger(SchedulerLogger)
        .AddTag("NodeShardId: %v", Id_))
    , SubmitJobsToStrategyExecutor_(New<TPeriodicExecutor>(
        GetInvoker(),
        BIND(&TNodeShard::SubmitJobsToStrategy, MakeWeak(this)),
        Config_->NodeShardSubmitJobsToStrategyPeriod))
{ }

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

IInvokerPtr TNodeShard::OnMasterConnected()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    DoCleanup();

    YT_VERIFY(!Connected_);
    Connected_ = true;

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

    CachedExecNodeDescriptorsRefresher_->Stop();

    for (const auto& [nodeId, node] : IdToNode_) {
        TLeaseManager::CloseLease(node->GetRegistrationLease());
        TLeaseManager::CloseLease(node->GetHeartbeatLease());
    }

    IdToOpertionState_.clear();

    IdToNode_.clear();
    ExecNodeCount_ = 0;
    TotalNodeCount_ = 0;

    ActiveJobCount_ = 0;

    {
        TWriterGuard guard(JobCounterLock_);
        JobCounter_.clear();
        AbortedJobCounter_.clear();
        CompletedJobCounter_.clear();
    }

    JobsToSubmitToStrategy_.clear();

    ConcurrentHeartbeatCount_ = 0;

    JobIdToScheduleEntry_.clear();
    OperationIdToJobIterators_.clear();

    SubmitJobsToStrategy();
}

void TNodeShard::RegisterOperation(
    TOperationId operationId,
    const IOperationControllerPtr& controller,
    bool jobsReady)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    YT_VERIFY(IdToOpertionState_.emplace(
        operationId,
        TOperationState(controller, jobsReady, CurrentEpoch_++)
    ).second);

    YT_LOG_DEBUG("Operation registered at node shard (OperationId: %v, JobsReady: %v)",
        operationId,
        jobsReady);
}

void TNodeShard::StartOperationRevival(TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto& operationState = GetOperationState(operationId);
    operationState.JobsReady = false;
    operationState.ForbidNewJobs = false;
    operationState.OperationUnreadyLoggedJobIds = THashSet<TJobId>();

    YT_LOG_DEBUG("Operation revival started at node shard (OperationId: %v, JobCount: %v)",
        operationId,
        operationState.Jobs.size());

    auto jobs = operationState.Jobs;
    for (const auto& [jobId, job] : jobs) {
        UnregisterJob(job, /* enableLogging */ false);
        JobsToSubmitToStrategy_.erase(jobId);
    }

    for (auto jobId : operationState.JobsToSubmitToStrategy) {
        JobsToSubmitToStrategy_.erase(jobId);
    }
    operationState.JobsToSubmitToStrategy.clear();

    {
        auto range = OperationIdToJobIterators_.equal_range(operationId);
        for (auto it = range.first; it != range.second; ++it) {
            JobIdToScheduleEntry_.erase(it->second);
        }
        OperationIdToJobIterators_.erase(operationId);
    }

    YT_VERIFY(operationState.Jobs.empty());
}

void TNodeShard::FinishOperationRevival(TOperationId operationId, const std::vector<TJobPtr>& jobs)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto& operationState = GetOperationState(operationId);

    YT_VERIFY(!operationState.JobsReady);
    operationState.JobsReady = true;
    operationState.ForbidNewJobs = false;
    operationState.Terminated = false;
    operationState.OperationUnreadyLoggedJobIds = THashSet<TJobId>();

    for (const auto& job : jobs) {
        auto node = GetOrRegisterNode(
            job->GetRevivalNodeId(),
            TNodeDescriptor(job->GetRevivalNodeAddress()),
            ENodeState::Online);
        job->SetNode(node);
        SetJobWaitingForConfirmation(job);
        RemoveRecentlyFinishedJob(job->GetId());
        RegisterJob(job);
    }

    YT_LOG_DEBUG("Operation revival finished at node shard (OperationId: %v, RevivedJobCount: %v)",
        operationId,
        jobs.size());

    // Give some time for nodes to confirm the jobs.
    TDelayedExecutor::Submit(
        BIND(&TNodeShard::AbortUnconfirmedJobs, MakeWeak(this), operationId, operationState.Epoch, jobs)
            .Via(GetInvoker()),
        Config_->JobRevivalAbortTimeout);
}

void TNodeShard::ResetOperationRevival(TOperationId operationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YT_VERIFY(Connected_);

    auto& operationState = GetOperationState(operationId);

    operationState.JobsReady = true;
    operationState.ForbidNewJobs = false;
    operationState.Terminated = false;
    operationState.OperationUnreadyLoggedJobIds = THashSet<TJobId>();

    YT_LOG_DEBUG("Operation revival state reset at node shard (OperationId: %v)",
        operationId);
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

void TNodeShard::ProcessHeartbeat(const TScheduler::TCtxNodeHeartbeatPtr& context)
{
    GetInvoker()->Invoke(
        BIND([=, this_ = MakeStrong(this)] {
            VERIFY_INVOKER_AFFINITY(GetInvoker());

            try {
                ValidateConnected();
                SwitchTo(CancelableInvoker_);

                DoProcessHeartbeat(context);
            } catch (const TErrorException& error) {
                context->Reply(error);
            }
        }));
}

void TNodeShard::DoProcessHeartbeat(const TScheduler::TCtxNodeHeartbeatPtr& context)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvoker_);

    auto* request = &context->Request();
    auto* response = &context->Response();

    int jobReporterWriteFailuresCount = 0;
    if (request->has_job_reporter_write_failures_count()) {
        jobReporterWriteFailuresCount = request->job_reporter_write_failures_count();
    }
    JobReporterWriteFailuresCount_.fetch_add(jobReporterWriteFailuresCount, std::memory_order_relaxed);

    auto nodeId = request->node_id();
    auto descriptor = FromProto<TNodeDescriptor>(request->node_descriptor());
    const auto& resourceLimits = request->resource_limits();
    const auto& resourceUsage = request->resource_usage();

    context->SetRequestInfo("NodeId: %v, NodeAddress: %v, ResourceUsage: %v, JobCount: %v, Confirmation: {C: %v, U: %v}",
        nodeId,
        descriptor.GetDefaultAddress(),
        Host_->FormatResourceUsage(
            TJobResources(resourceUsage),
            TJobResources(resourceLimits),
            request->disk_resources()
        ),
        request->jobs().size(),
        request->confirmed_job_count(),
        request->unconfirmed_jobs().size());

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
        UpdateNodeResources(node,
            request->resource_limits(),
            request->resource_usage(),
            request->disk_resources());
    }

    TLeaseManager::RenewLease(node->GetHeartbeatLease());
    TLeaseManager::RenewLease(node->GetRegistrationLease());

    if (node->GetMasterState() != NNodeTrackerClient::ENodeState::Online || node->GetSchedulerState() != ENodeState::Online) {
        auto error = TError("Node is not online");
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

    bool isThrottlingActive = false;
    if (ConcurrentHeartbeatCount_ >= Config_->HardConcurrentHeartbeatLimit) {
        isThrottlingActive = true;
        YT_LOG_INFO("Hard heartbeat limit reached (NodeAddress: %v, Limit: %v)",
            node->GetDefaultAddress(),
            Config_->HardConcurrentHeartbeatLimit);
    } else if (ConcurrentHeartbeatCount_ >= Config_->SoftConcurrentHeartbeatLimit &&
        node->GetLastSeenTime() + Config_->HeartbeatProcessBackoff > TInstant::Now())
    {
        isThrottlingActive = true;
        YT_LOG_INFO("Soft heartbeat limit reached (NodeAddress: %v, Limit: %v)",
            node->GetDefaultAddress(),
            Config_->SoftConcurrentHeartbeatLimit);
    }

    response->set_enable_job_reporter(Config_->EnableJobReporter);
    response->set_enable_job_spec_reporter(Config_->EnableJobSpecReporter);
    response->set_enable_job_stderr_reporter(Config_->EnableJobStderrReporter);
    response->set_enable_job_profile_reporter(Config_->EnableJobProfileReporter);
    response->set_enable_job_fail_context_reporter(Config_->EnableJobFailContextReporter);
    response->set_operation_archive_version(Host_->GetOperationArchiveVersion());

    BeginNodeHeartbeatProcessing(node);
    auto finallyGuard = Finally([&, cancelableContext = CancelableContext_] {
        if (!cancelableContext->IsCanceled()) {
            EndNodeHeartbeatProcessing(node);
        }
    });

    std::vector<TJobPtr> runningJobs;
    bool hasWaitingJobs = false;
    PROFILE_AGGREGATED_TIMING (AnalysisTimeCounter) {
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
            YT_LOG_DEBUG("Waiting jobs found, suppressing new jobs scheduling");
        }
        if (isThrottlingActive) {
            YT_LOG_DEBUG("Throttling is active, suppressing new jobs scheduling");
        }
        skipScheduleJobs = true;
    }

    response->set_scheduling_skipped(skipScheduleJobs);

    if (Config_->EnableJobAbortOnZeroUserSlots && node->GetResourceLimits().GetUserSlots() == 0) {
        // Abort all jobs on node immediately, if it has no user slots.
        // Make a copy, the collection will be modified.
        auto jobs = node->Jobs();
        const auto& address = node->GetDefaultAddress();
        for (const auto& job : jobs) {
            YT_LOG_DEBUG("Aborting job on node without user slots (Address: %v, JobId: %v, OperationId: %v)",
                address,
                job->GetId(),
                job->GetOperationId());
            auto status = JobStatusFromError(
                TError("Node without user slots")
                << TErrorAttribute("abort_reason", EAbortReason::NodeWithZeroUserSlots));
            OnJobAborted(job, &status, /* byScheduler */ true);
        }
    }

    auto mediumDirectory = Bootstrap_
        ->GetMasterClient()
        ->GetNativeConnection()
        ->GetMediumDirectory();
    auto schedulingContext = CreateSchedulingContext(
        Id_,
        Config_,
        node,
        runningJobs,
        mediumDirectory);

    PROFILE_AGGREGATED_TIMING (GracefulPreemptionTimeCounter) {
        for (const auto& job : runningJobs) {
            if (job->GetPreemptionMode() == EPreemptionMode::Graceful && !job->GetGracefullyPreempted()) {
                Host_->GetStrategy()->PreemptJobsGracefully(schedulingContext);
                break;
            }
        }
    }

    PROFILE_AGGREGATED_TIMING (StrategyJobProcessingTimeCounter) {
        SubmitJobsToStrategy();
    }

    if (!skipScheduleJobs) {
        PROFILE_AGGREGATED_TIMING (ScheduleTimeCounter) {
            Y_UNUSED(WaitFor(Host_->GetStrategy()->ScheduleJobs(schedulingContext)));
        }

        const auto& statistics = schedulingContext->GetSchedulingStatistics();

        node->SetResourceUsage(schedulingContext->ResourceUsage());
        node->SetLastHeartbeatStatistics(statistics);

        ProcessScheduledJobs(
            schedulingContext,
            /* requestContext */ context);

        // NB: some jobs maybe considered aborted after processing scheduled jobs.
        PROFILE_AGGREGATED_TIMING (StrategyJobProcessingTimeCounter) {
            SubmitJobsToStrategy();
        }

        context->SetResponseInfo(
            "NodeId: %v, NodeAddress: %v, "
            "StartedJobs: %v, PreemptedJobs: %v, GracefullyPreemptedJobs: %v, "
            "JobsScheduledDuringPreemption: %v, PreemptableJobs: %v, PreemptableResources: %v, "
            "ControllerScheduleJobCount: %v, NonPreemptiveScheduleJobAttempts: %v, "
            "PreemptiveScheduleJobAttempts: %v, HasAggressivelyStarvingElements: %v",
            nodeId,
            descriptor.GetDefaultAddress(),
            schedulingContext->StartedJobs().size(),
            schedulingContext->PreemptedJobs().size(),
            schedulingContext->GracefullyPreemptedJobs().size(),
            statistics.ScheduledDuringPreemption,
            statistics.PreemptableJobCount,
            FormatResources(statistics.ResourceUsageDiscount),
            statistics.ControllerScheduleJobCount,
            statistics.NonPreemptiveScheduleJobAttempts,
            statistics.PreemptiveScheduleJobAttempts,
            statistics.HasAggressivelyStarvingElements);
    } else {
        context->SetResponseInfo(
            "NodeId: %v, NodeAddress: %v, GracefullyPreemptedJobs: %v",
            nodeId,
            descriptor.GetDefaultAddress(),
            schedulingContext->GracefullyPreemptedJobs().size());
    }

    context->Reply();
}

TRefCountedExecNodeDescriptorMapPtr TNodeShard::GetExecNodeDescriptors()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    UpdateExecNodeDescriptors();

    {
        TReaderGuard guard(CachedExecNodeDescriptorsLock_);
        return CachedExecNodeDescriptors_;
    }
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

    {
        TWriterGuard guard(CachedExecNodeDescriptorsLock_);
        std::swap(CachedExecNodeDescriptors_, result);
    }
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

        YT_LOG_DEBUG("Handling node attributes (NodeId: %v, NodeAddress: %v, ObjectId: %v, NewState: %v)",
            nodeId,
            address,
            objectId,
            newState);

        YT_VERIFY(Host_->GetNodeShardId(nodeId) == Id_);

        auto nodeIt = IdToNode_.find(nodeId);
        if (nodeIt == IdToNode_.end()) {
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
            YT_LOG_WARNING("Node is not registered at scheduler but online at master (NodeId: %v, NodeAddress: %v)",
                nodeId,
                address);
        }

        if (newState == NNodeTrackerClient::ENodeState::Online) {
            TLeaseManager::RenewLease(execNode->GetRegistrationLease());
            if (execNode->GetSchedulerState() == ENodeState::Offline &&
                execNode->GetLastSeenTime() + Config_->MaxNodeUnseenPeriodToAbortJobs < now)
            {
                AbortAllJobsAtNode(execNode);
            }
        }

        execNode->SetIOWeights(ioWeights);

        auto oldState = execNode->GetMasterState();
        auto tags = attributes.Get<THashSet<TString>>("tags");

        if (oldState == NNodeTrackerClient::ENodeState::Online && newState != NNodeTrackerClient::ENodeState::Online) {
            // NOTE: Tags will be validated when node become online, no need in additional check here.
            execNode->Tags() = tags;
            SubtractNodeResources(execNode);
            AbortAllJobsAtNode(execNode);
            UpdateNodeState(execNode, newState, execNode->GetSchedulerState());
            ++nodeChangesCount;
            continue;
        } else if (oldState != newState) {
            UpdateNodeState(execNode, newState, execNode->GetSchedulerState());
        }

        if ((oldState != NNodeTrackerClient::ENodeState::Online && newState == NNodeTrackerClient::ENodeState::Online) || execNode->Tags() != tags) {
            auto updateResult = WaitFor(Host_->RegisterOrUpdateNode(nodeId, address, tags));
            if (!updateResult.IsOK()) {
                auto error = TError("Node tags update failed")
                    << TErrorAttribute("node_id", nodeId)
                    << TErrorAttribute("address", address)
                    << TErrorAttribute("tags", tags)
                    << updateResult;
                YT_LOG_WARNING(error);
                errors.push_back(error);

                if (oldState == NNodeTrackerClient::ENodeState::Online && execNode->GetSchedulerState() == ENodeState::Online) {
                    SubtractNodeResources(execNode);
                    AbortAllJobsAtNode(execNode);
                }
                UpdateNodeState(execNode, /* newMasterState */ newState, /* newSchedulerState */ ENodeState::Offline, error);
            } else {
                if (oldState != NNodeTrackerClient::ENodeState::Online && newState == NNodeTrackerClient::ENodeState::Online) {
                    AddNodeResources(execNode);
                }
                execNode->Tags() = tags;
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

void TNodeShard::AbortOperationJobs(TOperationId operationId, const TError& abortReason, bool terminated)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto* operationState = FindOperationState(operationId);
    if (!operationState) {
        return;
    }

    operationState->Terminated = terminated;
    operationState->ForbidNewJobs = true;
    auto jobs = operationState->Jobs;
    for (const auto& job : jobs) {
        auto status = JobStatusFromError(abortReason);
        OnJobAborted(job.second, &status, /* byScheduler */ true, terminated);
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
    if (!operationState || operationState->Terminated) {
        return;
    }

    operationState->ForbidNewJobs = false;
}

TNodeDescriptor TNodeShard::GetJobNode(TJobId jobId, const TString& user, EPermissionSet requiredPermissions)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto job = FindJob(jobId);

    TExecNodePtr node;
    TOperationId operationId;

    if (!job) {
        node = FindNodeByJob(jobId);
        if (!node) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::NoSuchJob,
                "Job %v not found", jobId);
        }

        auto it = node->RecentlyFinishedJobs().find(jobId);
        if (it == node->RecentlyFinishedJobs().end()) {
            THROW_ERROR_EXCEPTION(
                NScheduler::EErrorCode::NoSuchJob,
                "Job %v not found", jobId);
        }

        operationId = it->second.OperationId;
    } else {
        node = job->GetNode();
        operationId = job->GetOperationId();
    }

    Host_->ValidateOperationAccess(user, operationId, requiredPermissions);

    return node->NodeDescriptor();
}

TYsonString TNodeShard::StraceJob(TJobId jobId, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto job = GetJobOrThrow(jobId);

    Host_->ValidateOperationAccess(user, job->GetOperationId(), EPermissionSet(EPermission::Read));

    YT_LOG_DEBUG("Getting strace dump (JobId: %v, OperationId: %v)",
        job->GetId(),
        job->GetOperationId());

    auto proxy = CreateJobProberProxy(job);
    auto req = proxy.Strace();
    ToProto(req->mutable_job_id(), jobId);

    auto rspOrError = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting strace dump of job %v",
        jobId);

    const auto& rsp = rspOrError.Value();

    YT_LOG_DEBUG("Strace dump received (JobId: %v, OperationId: %v)",
        job->GetId(),
        job->GetOperationId());

    return TYsonString(rsp->trace());
}

void TNodeShard::DumpJobInputContext(TJobId jobId, const TYPath& path, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto job = GetJobOrThrow(jobId);

    Host_->ValidateOperationAccess(user, job->GetOperationId(), EPermissionSet(EPermission::Read));

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

    auto asyncResult = Host_->AttachJobContext(path, chunkIds.front(), job->GetOperationId(), jobId, user);
    WaitFor(asyncResult)
        .ThrowOnError();

    YT_LOG_DEBUG("Input contexts saved (JobId: %v, OperationId: %v)",
        job->GetId(),
        job->GetOperationId());
}

void TNodeShard::SignalJob(TJobId jobId, const TString& signalName, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto job = GetJobOrThrow(jobId);

    Host_->ValidateOperationAccess(user, job->GetOperationId(), EPermissionSet(EPermission::Manage));

    YT_LOG_DEBUG("Sending job signal (JobId: %v, OperationId: %v, Signal: %v)",
        job->GetId(),
        job->GetOperationId(),
        signalName);

    auto proxy = CreateJobProberProxy(job);
    auto req = proxy.SignalJob();
    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_signal_name(), signalName);

    auto rspOrError = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error sending signal %v to job %v",
        signalName,
        jobId);

    YT_LOG_DEBUG("Job signal sent (JobId: %v, OperationId: %v)",
        job->GetId(),
        job->GetOperationId());
}

void TNodeShard::AbandonJob(TJobId jobId, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto job = GetJobOrThrow(jobId);

    Host_->ValidateOperationAccess(user, job->GetOperationId(), EPermissionSet(EPermission::Manage));

    YT_LOG_DEBUG("Abandoning job by user request (JobId: %v, OperationId: %v, User: %v)",
        job->GetId(),
        job->GetOperationId(),
        user);

    switch (job->GetType()) {
        case EJobType::Map:
        case EJobType::OrderedMap:
        case EJobType::SortedReduce:
        case EJobType::JoinReduce:
        case EJobType::PartitionMap:
        case EJobType::ReduceCombiner:
        case EJobType::PartitionReduce:
        case EJobType::Vanilla:
            break;
        default:
            THROW_ERROR_EXCEPTION("Cannot abandon job %v of operation %v since it has type %Qlv",
                job->GetId(),
                job->GetOperationId(),
                job->GetType());
    }

    if (job->GetState() != EJobState::Running &&
        job->GetState() != EJobState::Waiting)
    {
        THROW_ERROR_EXCEPTION("Cannot abandon job %v of operation %v since it is not running",
            job->GetId(),
            job->GetOperationId());
    }

    OnJobCompleted(job, nullptr /* jobStatus */, true /* abandoned */);
}

void TNodeShard::AbortJobByUserRequest(TJobId jobId, std::optional<TDuration> interruptTimeout, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    ValidateConnected();

    auto job = GetJobOrThrow(jobId);

    Host_->ValidateOperationAccess(user, job->GetOperationId(), EPermissionSet(EPermission::Manage));

    if (job->GetState() != EJobState::Running &&
        job->GetState() != EJobState::Waiting)
    {
        THROW_ERROR_EXCEPTION("Cannot abort job %v of operation %v since it is not running",
            jobId,
            job->GetOperationId());
    }

    if (interruptTimeout.value_or(TDuration::Zero()) != TDuration::Zero()) {
        if (!job->GetInterruptible()) {
            THROW_ERROR_EXCEPTION("Cannot interrupt job %v of type %Qlv "
                "because such job type does not support interruption or \"interruption_signal\" is not set",
                jobId,
                job->GetType());
        }

        YT_LOG_DEBUG("Trying to interrupt job by user request (JobId: %v, InterruptTimeout: %v)",
            jobId,
            interruptTimeout);

        auto proxy = CreateJobProberProxy(job);
        auto req = proxy.Interrupt();
        ToProto(req->mutable_job_id(), jobId);

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
        YT_LOG_DEBUG("Requested to abort an unknown job, ignored (JobId: %v)", jobId);
        return;
    }

    YT_LOG_DEBUG(error, "Aborting job by internal request (JobId: %v, OperationId: %v)",
        jobId,
        job->GetOperationId());

    auto status = JobStatusFromError(error);
    OnJobAborted(job, &status, /* byScheduler */ true);
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

TOperationId TNodeShard::FindOperationIdByJobId(TJobId jobId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = FindJob(jobId);
    return job ? job->GetOperationId() : TOperationId();
}

TNodeShard::TResourceStatistics TNodeShard::CalculateResourceStatistics(const TSchedulingTagFilter& filter)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TResourceStatistics statistics;

    TRefCountedExecNodeDescriptorMapPtr descriptors;
    {
        TReaderGuard guard(CachedExecNodeDescriptorsLock_);
        descriptors = CachedExecNodeDescriptors_;
    }

    for (const auto& [nodeId, descriptor] : *descriptors) {
        if (descriptor.Online && descriptor.CanSchedule(filter)) {
            statistics.Usage += descriptor.ResourceUsage;
            statistics.Limits += descriptor.ResourceLimits;
        }
    }
    return statistics;
}

TJobResources TNodeShard::GetResourceLimits(const TSchedulingTagFilter& filter)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CachedResourceStatisticsByTags_->Get(filter).Limits;
}

TJobResources TNodeShard::GetResourceUsage(const TSchedulingTagFilter& filter)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CachedResourceStatisticsByTags_->Get(filter).Usage;
}

int TNodeShard::GetActiveJobCount()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ActiveJobCount_;
}

TJobCounter TNodeShard::GetJobCounter()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(JobCounterLock_);

    return JobCounter_;
}

TAbortedJobCounter TNodeShard::GetAbortedJobCounter()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(JobCounterLock_);

    return AbortedJobCounter_;
}

TCompletedJobCounter TNodeShard::GetCompletedJobCounter()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(JobCounterLock_);

    return CompletedJobCounter_;
}

TJobTimeStatisticsDelta TNodeShard::GetJobTimeStatisticsDelta()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TWriterGuard guard(JobTimeStatisticsDeltaLock_);

    auto result = JobTimeStatisticsDelta_;
    JobTimeStatisticsDelta_.Reset();
    return result;
}

int TNodeShard::GetExecNodeCount()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ExecNodeCount_;
}

int TNodeShard::GetTotalNodeCount()
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
    YT_VERIFY(it != JobIdToScheduleEntry_.end());
    auto& entry = it->second;
    YT_VERIFY(operationId == entry.OperationId);

    YT_LOG_DEBUG("Job schedule response received (OperationId: %v, JobId: %v, Success: %v, Duration: %v)",
        operationId,
        jobId,
        response.has_job_type(),
        CpuDurationToDuration(GetCpuInstant() - entry.StartTime).MilliSeconds());

    auto result = New<TControllerScheduleJobResult>();
    if (response.has_job_type()) {
        result->StartDescriptor.emplace(
            jobId,
            static_cast<EJobType>(response.job_type()),
            FromProto<TJobResourcesWithQuota>(response.resource_limits()),
            response.interruptible());
    }
    for (const auto& protoCounter : response.failed()) {
        result->Failed[static_cast<EScheduleJobFailReason>(protoCounter.reason())] = protoCounter.value();
    }
    FromProto(&result->Duration, response.duration());
    result->IncarnationId = entry.IncarnationId;

    entry.Promise.Set(std::move(result));

    OperationIdToJobIterators_.erase(entry.OperationIdToJobIdsIterator);
    JobIdToScheduleEntry_.erase(it);
}

int TNodeShard::ExtractJobReporterWriteFailuresCount()
{
    return JobReporterWriteFailuresCount_.exchange(0);
}

int TNodeShard::GetJobReporterQueueIsTooLargeNodeCount()
{
    return JobReporterQueueIsTooLargeNodeCount_.load();
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

    YT_VERIFY(IdToNode_.insert(std::make_pair(node->GetId(), node)).second);

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

    AbortAllJobsAtNode(node);

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

    Host_->UnregisterNode(node->GetId(), address);

    YT_LOG_INFO("Node unregistered (Address: %v)", address);
}

void TNodeShard::AbortAllJobsAtNode(const TExecNodePtr& node)
{
    // Make a copy, the collection will be modified.
    auto jobs = node->Jobs();
    const auto& address = node->GetDefaultAddress();
    for (const auto& job : jobs) {
        YT_LOG_DEBUG("Aborting job on an offline node (Address: %v, JobId: %v, OperationId: %v)",
            address,
            job->GetId(),
            job->GetOperationId());
        auto status = JobStatusFromError(
            TError("Node offline")
            << TErrorAttribute("abort_reason", EAbortReason::NodeOffline));
        OnJobAborted(job, &status, /* byScheduler */ true);
    }
}

void TNodeShard::AbortUnconfirmedJobs(
    TOperationId operationId,
    TEpoch epoch,
    const std::vector<TJobPtr>& jobs)
{
    const auto* operationState = FindOperationState(operationId);
    if (!operationState || operationState->Epoch != epoch) {
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

    auto status = JobStatusFromError(
        TError("Job not confirmed after timeout")
            << TErrorAttribute("abort_reason", EAbortReason::RevivalConfirmationTimeout));
    for (const auto& job : unconfirmedJobs) {
        YT_LOG_DEBUG("Aborting revived job that was not confirmed (OperationId: %v, JobId: %v)",
            operationId,
            job->GetId());
        OnJobAborted(job, &status, /* byScheduler */ true);
        if (auto node = job->GetNode()) {
            ResetJobWaitingForConfirmation(job);
        }
    }
}

// TODO(eshcherbin): This method has become too big -- gotta split it.
void TNodeShard::ProcessHeartbeatJobs(
    const TExecNodePtr& node,
    NJobTrackerClient::NProto::TReqHeartbeat* request,
    NJobTrackerClient::NProto::TRspHeartbeat* response,
    std::vector<TJobPtr>* runningJobs,
    bool* hasWaitingJobs)
{
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
        ToProto(response->mutable_jobs_to_confirm(), node->UnconfirmedJobIds());
        // If it is a first time we get the heartbeat from a given node,
        // there will definitely be some jobs that are missing. No need to abort
        // them.
    }

    for (const auto& job : node->Jobs()) {
        // Verify that all flags are in the initial state.
        YT_VERIFY(!checkMissingJobs || !job->GetFoundOnNode());
    }

    THashSet<TJobId> recentlyFinishedJobIdsToRemove;
    {
        auto now = GetCpuInstant();
        for (const auto& [jobId, jobInfo] : node->RecentlyFinishedJobs()) {
            if (jobInfo.ReleaseFlags){
                YT_LOG_DEBUG("Requesting node to remove released job "
                    "(JobId: %v, NodeId: %v, NodeAddress: %v, %v)",
                    jobId,
                    nodeId,
                    nodeAddress,
                    *jobInfo.ReleaseFlags);
                recentlyFinishedJobIdsToRemove.insert(jobId);
                ToProto(response->add_jobs_to_remove(), TJobToRelease{jobId, *jobInfo.ReleaseFlags});
            } else if (now > jobInfo.EvictionDeadline) {
                YT_LOG_DEBUG("Removing job from recently finished due to timeout for release "
                    "(JobId: %v, NodeId: %v, NodeAddress: %v)",
                    jobId,
                    nodeId,
                    nodeAddress);
                recentlyFinishedJobIdsToRemove.insert(jobId);
                ToProto(response->add_jobs_to_remove(), TJobToRelease{jobId});
            }
        }
        for (auto jobId : recentlyFinishedJobIdsToRemove) {
            RemoveRecentlyFinishedJob(jobId);
        }
    }

    // Used for debug logging.
    THashMap<EJobState, std::vector<TJobId>> jobStateToOngoingJobIds;
    std::vector<TJobId> recentlyFinishedJobIdsToLog;
    for (auto& jobStatus : *request->mutable_jobs()) {
        YT_VERIFY(jobStatus.has_job_type());
        auto jobType = EJobType(jobStatus.job_type());
        // Skip jobs that are not issued by the scheduler.
        if (jobType < FirstSchedulerJobType || jobType > LastSchedulerJobType) {
            continue;
        }

        auto job = ProcessJobHeartbeat(
            node,
            recentlyFinishedJobIdsToRemove,
            response,
            &jobStatus);
        if (job) {
            if (checkMissingJobs) {
                job->SetFoundOnNode(true);
            }
            switch (job->GetState()) {
                case EJobState::Running:
                    runningJobs->push_back(job);
                    jobStateToOngoingJobIds[job->GetState()].push_back(job->GetId());
                    break;
                case EJobState::Waiting:
                    *hasWaitingJobs = true;
                    jobStateToOngoingJobIds[job->GetState()].push_back(job->GetId());
                    break;
                default:
                    break;
            }
        } else {
            auto jobId = FromProto<TJobId>(jobStatus.job_id());
            auto operationId = FromProto<TOperationId>(jobStatus.operation_id());
            auto operation = FindOperationState(operationId);
            if (!(operation && operation->OperationUnreadyLoggedJobIds.contains(jobId))
                && node->RecentlyFinishedJobs().contains(jobId))
            {
                recentlyFinishedJobIdsToLog.push_back(jobId);
            }
        }
    }

    YT_LOG_DEBUG_UNLESS(recentlyFinishedJobIdsToLog.empty(),
        "Jobs are skipped since they were recently finished and are currently being stored"
        "(JobIds: %v)",
        recentlyFinishedJobIdsToLog);

    if (shouldLogOngoingJobs) {
        for (const auto& [jobState, jobIds] : jobStateToOngoingJobIds) {
            YT_LOG_DEBUG_IF(!jobIds.empty(), "Jobs are %lv (JobIds: %v)", jobState, jobIds);
        }
    }

    if (checkMissingJobs) {
        std::vector<TJobPtr> missingJobs;
        for (const auto& job : node->Jobs()) {
            YT_VERIFY(!job->GetWaitingForConfirmation());
            // Jobs that are waiting for confirmation may never be considered missing.
            // They are removed in two ways: by explicit unconfirmation of the node
            // or after revival confirmation timeout.
            if (!job->GetFoundOnNode()) {
                YT_LOG_ERROR("Job is missing (Address: %v, JobId: %v, OperationId: %v)",
                    node->GetDefaultAddress(),
                    job->GetId(),
                    job->GetOperationId());
                missingJobs.push_back(job);
            } else {
                job->SetFoundOnNode(false);
            }
        }

        for (const auto& job : missingJobs) {
            auto status = JobStatusFromError(TError("Job vanished"));
            OnJobAborted(job, &status, /* byScheduler */ true);
        }
    }

    for (auto jobId : FromProto<std::vector<TJobId>>(request->unconfirmed_jobs())) {
        auto job = FindJob(jobId);
        if (!job) {
            // This may happen if we received heartbeat after job was removed by some different reasons
            // (like confirmation timeout).
            continue;
        }

        auto status = JobStatusFromError(TError("Job not confirmed by node"));
        OnJobAborted(job, &status, /* byScheduler */ true);

        ResetJobWaitingForConfirmation(job);
    }
}

NLogging::TLogger TNodeShard::CreateJobLogger(
    TJobId jobId,
    TOperationId operationId,
    EJobState state,
    const TString& address)
{
    auto logger = Logger;
    logger.AddTag("Address: %v, JobId: %v, OperationId: %v, State: %v",
        address,
        jobId,
        operationId,
        state);
    return logger;
}

TJobPtr TNodeShard::ProcessJobHeartbeat(
    const TExecNodePtr& node,
    const THashSet<TJobId>& recentlyFinishedJobIdsToRemove,
    NJobTrackerClient::NProto::TRspHeartbeat* response,
    TJobStatus* jobStatus)
{
    auto jobId = FromProto<TJobId>(jobStatus->job_id());
    auto operationId = FromProto<TOperationId>(jobStatus->operation_id());
    auto state = EJobState(jobStatus->state());
    const auto& address = node->GetDefaultAddress();

    auto Logger = CreateJobLogger(jobId, operationId, state, address);

    auto job = FindJob(jobId, node);
    auto operation = FindOperationState(operationId);
    if (!job) {
        // We can decide what to do with the job of an operation only when all
        // TJob structures of the operation are materialized. Also we should
        // not remove the completed jobs that were not saved to the snapshot.
        if (operation && !operation->JobsReady) {
            if (!operation->OperationUnreadyLoggedJobIds.contains(jobId)) {
                YT_LOG_DEBUG("Job is skipped since operation jobs are not ready yet");
                operation->OperationUnreadyLoggedJobIds.insert(jobId);
            }
            return nullptr;
        }

        if (node->RecentlyFinishedJobs().contains(jobId) || recentlyFinishedJobIdsToRemove.contains(jobId)) {
            // NB(eshcherbin): This event is logged one level above.
            return nullptr;
        }

        switch (state) {
            case EJobState::Completed:
                YT_LOG_DEBUG("Unknown job has completed, removal scheduled");
                // COMPAT: make ArchiveJobSpec optional and remove.
                ToProto(response->add_jobs_to_remove(), {jobId, false /* ArchiveJobSpec */});
                break;

            case EJobState::Failed:
                YT_LOG_DEBUG("Unknown job has failed, removal scheduled");
                // COMPAT: make ArchiveJobSpec optional and remove.
                ToProto(response->add_jobs_to_remove(), {jobId, false /* ArchiveJobSpec */});
                break;

            case EJobState::Aborted:
                YT_LOG_DEBUG(FromProto<TError>(jobStatus->result().error()), "Job aborted, removal scheduled");
                // COMPAT: make ArchiveJobSpec optional and remove.
                ToProto(response->add_jobs_to_remove(), {jobId, false /* ArchiveJobSpec */});
                break;

            case EJobState::Running:
                YT_LOG_DEBUG("Unknown job is running, abort scheduled");
                ToProto(response->add_jobs_to_abort(), jobId);
                break;

            case EJobState::Waiting:
                YT_LOG_DEBUG("Unknown job is waiting, abort scheduled");
                ToProto(response->add_jobs_to_abort(), jobId);
                break;

            case EJobState::Aborting:
                YT_LOG_DEBUG("Job is aborting");
                break;

            default:
                YT_ABORT();
        }
        return nullptr;
    }

    auto codicilGuard = MakeOperationCodicilGuard(job->GetOperationId());

    Logger.AddTag("Type: %v",
        job->GetType());

    // Check if the job is running on a proper node.
    if (node->GetId() != job->GetNode()->GetId()) {
        const auto& expectedAddress = job->GetNode()->GetDefaultAddress();
        // Job has moved from one node to another. No idea how this could happen.
        if (state == EJobState::Aborting) {
            // Do nothing, job is already terminating.
        } else if (state == EJobState::Completed || state == EJobState::Failed || state == EJobState::Aborted) {
            ToProto(response->add_jobs_to_remove(), {jobId, false /* ArchiveJobSpec */});
            YT_LOG_WARNING("Job status report was expected from %v, removal scheduled",
                expectedAddress);
        } else {
            ToProto(response->add_jobs_to_abort(), jobId);
            YT_LOG_WARNING("Job status report was expected from %v, abort scheduled",
                expectedAddress);
        }
        return nullptr;
    }

    if (job->GetWaitingForConfirmation()) {
        YT_LOG_DEBUG("Job confirmed (JobId: %v, State: %v)",
            jobId,
            state);
        ResetJobWaitingForConfirmation(job);
    }

    bool stateChanged = (state != job->GetState());

    switch (state) {
        case EJobState::Completed: {
            YT_LOG_DEBUG("Job completed, storage scheduled");
            AddRecentlyFinishedJob(job);
            OnJobCompleted(job, jobStatus);
            ToProto(response->add_jobs_to_store(), jobId);
            break;
        }

        case EJobState::Failed: {
            auto error = FromProto<TError>(jobStatus->result().error());
            YT_LOG_DEBUG(error, "Job failed, storage scheduled");
            AddRecentlyFinishedJob(job);
            OnJobFailed(job, jobStatus);
            ToProto(response->add_jobs_to_store(), jobId);
            break;
        }

        case EJobState::Aborted: {
            auto error = FromProto<TError>(jobStatus->result().error());
            YT_LOG_DEBUG(error, "Job aborted, storage scheduled");
            AddRecentlyFinishedJob(job);
            if (job->GetPreempted() &&
                (error.FindMatching(NExecAgent::EErrorCode::AbortByScheduler) ||
                error.FindMatching(NJobProxy::EErrorCode::JobNotPrepared)))
            {
                auto error = TError("Job preempted")
                    << TErrorAttribute("abort_reason", EAbortReason::Preemption)
                    << TErrorAttribute("preemption_reason", job->GetPreemptionReason());
                auto status = JobStatusFromError(error);
                OnJobAborted(job, &status, /* byScheduler */ false);
            } else {
                OnJobAborted(job, jobStatus, /* byScheduler */ false);
            }
            ToProto(response->add_jobs_to_store(), jobId);
            break;
        }

        case EJobState::Running:
        case EJobState::Waiting:
            if (job->GetState() == EJobState::Aborted) {
                YT_LOG_DEBUG("Aborting job");
                ToProto(response->add_jobs_to_abort(), jobId);
            } else {
                SetJobState(job, state);
                switch (state) {
                    case EJobState::Running:
                        YT_LOG_DEBUG_IF(stateChanged, "Job is now running");
                        OnJobRunning(job, jobStatus, stateChanged);
                        if (job->GetInterruptDeadline() != 0 && GetCpuInstant() > job->GetInterruptDeadline()) {
                            YT_LOG_DEBUG("Interrupted job deadline reached, aborting (InterruptDeadline: %v)",
                                CpuInstantToInstant(job->GetInterruptDeadline()));
                            ToProto(response->add_jobs_to_abort(), jobId);
                        } else if (job->GetFailRequested()) {
                            YT_LOG_DEBUG("Job fail requested");
                            ToProto(response->add_jobs_to_fail(), jobId);
                        } else if (job->GetInterruptReason() != EInterruptReason::None) {
                            ToProto(response->add_jobs_to_interrupt(), jobId);
                        }
                        break;

                    case EJobState::Waiting:
                        YT_LOG_DEBUG_IF(stateChanged, "Job is now waiting", state);
                        break;

                    default:
                        YT_ABORT();
                }
            }
            break;

        case EJobState::Aborting:
            YT_LOG_DEBUG("Job is aborting");
            break;

        default:
            YT_ABORT();
    }

    return job;
}

void TNodeShard::SubtractNodeResources(const TExecNodePtr& node)
{
    TWriterGuard guard(ResourcesLock_);

    TotalNodeCount_ -= 1;
    if (node->GetResourceLimits().GetUserSlots() > 0) {
        ExecNodeCount_ -= 1;
    }
}

void TNodeShard::AddNodeResources(const TExecNodePtr& node)
{
    TWriterGuard guard(ResourcesLock_);

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
        TWriterGuard guard(ResourcesLock_);

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

    ConcurrentHeartbeatCount_ += 1;
}

void TNodeShard::EndNodeHeartbeatProcessing(const TExecNodePtr& node)
{
    YT_VERIFY(node->GetHasOngoingHeartbeat());
    node->SetHasOngoingHeartbeat(false);

    ConcurrentHeartbeatCount_ -= 1;
    node->SetLastSeenTime(TInstant::Now());

    if (node->GetHasPendingUnregistration()) {
        DoUnregisterNode(node);
    }
}

void TNodeShard::ProcessScheduledJobs(
    const ISchedulingContextPtr& schedulingContext,
    const TScheduler::TCtxNodeHeartbeatPtr& rpcContext)
{
    auto* response = &rpcContext->Response();

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
            if (!operationState->Terminated) {
                const auto& controller = operationState->Controller;
                controller->OnNonscheduledJobAborted(job->GetId(), EAbortReason::SchedulingOperationSuspended);
                JobsToSubmitToStrategy_[job->GetId()] =
                    TJobUpdate{
                        EJobUpdateStatus::Finished,
                        job->GetOperationId(),
                        job->GetId(),
                        job->GetTreeId(),
                        TJobResources()};
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
        IncreaseProfilingCounter(job, 1);

        controller->OnJobStarted(job);

        auto* startInfo = response->add_jobs_to_start();
        ToProto(startInfo->mutable_job_id(), job->GetId());
        ToProto(startInfo->mutable_operation_id(), job->GetOperationId());
        *startInfo->mutable_resource_limits() = job->ResourceUsage().ToNodeResources();
        ToProto(startInfo->mutable_spec_service_addresses(), agent->GetAgentAddresses());
    }

    for (const auto& job : schedulingContext->PreemptedJobs()) {
        if (!FindOperationState(job->GetOperationId()) || job->GetUnregistered()) {
            YT_LOG_DEBUG("Cannot preempt job: operation is no longer known (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());
            continue;
        }

        if (job->GetInterruptible() && Config_->JobInterruptTimeout != TDuration::Zero()) {
            if (!job->GetPreempted()) {
                PreemptJob(job, DurationToCpuDuration(Config_->JobInterruptTimeout));
                ToProto(response->add_jobs_to_interrupt(), job->GetId());
            }
            // Else do nothing: job was already interrupted, by deadline not reached yet.
        } else {
            PreemptJob(job, std::nullopt);
            ToProto(response->add_jobs_to_abort(), job->GetId());
        }
    }

    for (const auto& job : schedulingContext->GracefullyPreemptedJobs()) {
        if (!job->GetGracefullyPreempted()) {
            DoInterruptJob(job, EInterruptReason::GracefulPreemption, DurationToCpuDuration(Config_->GracefulPreemptionJobInterruptTimeout));
            job->SetGracefullyPreempted(true);
            ToProto(response->add_jobs_to_interrupt(), job->GetId());
        }
    }
}

void TNodeShard::OnJobRunning(const TJobPtr& job, TJobStatus* status, bool shouldLogJob)
{
    YT_VERIFY(status);

    if (!status->has_statistics()) {
        return;
    }

    auto now = GetCpuInstant();
    if (now < job->GetRunningJobUpdateDeadline()) {
        return;
    }
    job->SetRunningJobUpdateDeadline(now + DurationToCpuDuration(Config_->RunningJobsUpdatePeriod));

    auto delta = status->resource_usage() - job->ResourceUsage();

    auto it = JobsToSubmitToStrategy_.find(job->GetId());
    if (it == JobsToSubmitToStrategy_.end() || it->second.Status != EJobUpdateStatus::Finished) {
        JobsToSubmitToStrategy_[job->GetId()] =
            TJobUpdate{
                EJobUpdateStatus::Running,
                job->GetOperationId(),
                job->GetId(),
                job->GetTreeId(),
                delta};
    }
    job->ResourceUsage() = status->resource_usage();

    auto* operationState = FindOperationState(job->GetOperationId());
    if (operationState) {
        const auto& controller = operationState->Controller;
        controller->OnJobRunning(job, status, shouldLogJob);
        operationState->JobsToSubmitToStrategy.insert(job->GetId());
    }
}

void TNodeShard::OnJobCompleted(const TJobPtr& job, TJobStatus* status, bool abandoned)
{
    YT_VERIFY(abandoned == !status);

    if (job->GetState() == EJobState::Running ||
        job->GetState() == EJobState::Waiting ||
        job->GetState() == EJobState::None)
    {
        // The value of status may be nullptr on abandoned jobs.
        if (status) {
            const auto& result = status->result();
            const auto& schedulerResultExt = result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            if (schedulerResultExt.unread_chunk_specs_size() == 0) {
                job->SetInterruptReason(EInterruptReason::None);
            } else if (job->IsRevived()) {
                // NB: We lose the original interrupt reason during the revival,
                // so we set it to Unknown.
                job->SetInterruptReason(EInterruptReason::Unknown);
            }
        } else {
            job->SetInterruptReason(EInterruptReason::None);
        }

        SetJobState(job, EJobState::Completed);

        OnJobFinished(job);

        auto* operationState = FindOperationState(job->GetOperationId());
        if (operationState) {
            const auto& controller = operationState->Controller;
            controller->OnJobCompleted(job, status, abandoned);
        }

        UnregisterJob(job);
    }
}

void TNodeShard::OnJobFailed(const TJobPtr& job, TJobStatus* status)
{
    YT_VERIFY(status);

    if (job->GetState() == EJobState::Running ||
        job->GetState() == EJobState::Waiting ||
        job->GetState() == EJobState::None)
    {
        SetJobState(job, EJobState::Failed);

        OnJobFinished(job);

        auto* operationState = FindOperationState(job->GetOperationId());
        if (operationState) {
            const auto& controller = operationState->Controller;
            controller->OnJobFailed(job, status);
        }

        UnregisterJob(job);
    }
}

void TNodeShard::OnJobAborted(const TJobPtr& job, TJobStatus* status, bool byScheduler, bool operationTerminated)
{
    YT_VERIFY(status);

    // Only update the status for the first time.
    // Typically the scheduler decides to abort the job on its own.
    // In this case we should ignore the status returned from the node
    // and avoid notifying the controller twice.
    if (job->GetState() == EJobState::Running ||
        job->GetState() == EJobState::Waiting ||
        job->GetState() == EJobState::None)
    {
        auto error = FromProto<TError>(status->result().error());
        ProfileAbortedJobErrors(job, error);

        job->SetAbortReason(GetAbortReason(error));
        SetJobState(job, EJobState::Aborted);

        OnJobFinished(job);

        auto* operationState = FindOperationState(job->GetOperationId());
        if (operationState && !operationTerminated) {
            const auto& controller = operationState->Controller;
            controller->OnJobAborted(job, status, byScheduler);
        }

        UnregisterJob(job);
    }
}

void TNodeShard::OnJobFinished(const TJobPtr& job)
{
    job->SetFinishTime(TInstant::Now());
    auto duration = job->GetDuration();

    {
        TWriterGuard guard(JobTimeStatisticsDeltaLock_);
        switch (job->GetState()) {
            case EJobState::Completed:
                JobTimeStatisticsDelta_.CompletedJobTimeDelta += duration.MicroSeconds();
                break;
            case EJobState::Failed:
                JobTimeStatisticsDelta_.FailedJobTimeDelta += duration.MicroSeconds();
                break;
            case EJobState::Aborted:
                JobTimeStatisticsDelta_.AbortedJobTimeDelta += duration.MicroSeconds();
                break;
            default:
                YT_ABORT();
        }
    }
}

void TNodeShard::SubmitJobsToStrategy()
{
    PROFILE_AGGREGATED_TIMING (StrategyJobProcessingTimeCounter) {
        if (!JobsToSubmitToStrategy_.empty()) {
            std::vector<TJobId> jobsToAbort;
            std::vector<std::pair<TOperationId, TJobId>> jobsToRemove;
            auto jobUpdates = GetValues(JobsToSubmitToStrategy_);
            Host_->GetStrategy()->ProcessJobUpdates(
                jobUpdates,
                &jobsToRemove,
                &jobsToAbort);

            for (auto jobId : jobsToAbort) {
                AbortJob(jobId, TError("Aborting job by strategy request"));
            }

            for (const auto& [operationId, jobId] : jobsToRemove) {
                auto* operationState = FindOperationState(operationId);
                if (operationState) {
                    operationState->JobsToSubmitToStrategy.erase(jobId);
                }

                YT_VERIFY(JobsToSubmitToStrategy_.erase(jobId) == 1);
            }
        }
    }
}

void TNodeShard::IncreaseProfilingCounter(const TJobPtr& job, int value)
{
    TWriterGuard guard(JobCounterLock_);
    if (job->GetState() == EJobState::Aborted) {
        AbortedJobCounter_[std::make_tuple(job->GetType(), job->GetState(), job->GetAbortReason())] += value;
    } else if (job->GetState() == EJobState::Completed) {
        CompletedJobCounter_[std::make_tuple(job->GetType(), job->GetState(), job->GetInterruptReason())] += value;
    } else {
        JobCounter_[std::make_tuple(job->GetType(), job->GetState())] += value;
    }
}

void TNodeShard::SetJobState(const TJobPtr& job, EJobState state)
{
    IncreaseProfilingCounter(job, -1);
    job->SetState(state);
    IncreaseProfilingCounter(job, 1);
}

void TNodeShard::RegisterJob(const TJobPtr& job)
{
    auto& operationState = GetOperationState(job->GetOperationId());

    auto node = job->GetNode();

    YT_VERIFY(operationState.Jobs.emplace(job->GetId(), job).second);
    YT_VERIFY(node->Jobs().insert(job).second);
    YT_VERIFY(node->IdToJob().insert(std::make_pair(job->GetId(), job)).second);
    ++ActiveJobCount_;

    YT_LOG_DEBUG("Job registered (JobId: %v, JobType: %v, Revived: %v, OperationId: %v)",
        job->GetId(),
        job->GetType(),
        job->IsRevived(),
        job->GetOperationId());
}

void TNodeShard::UnregisterJob(const TJobPtr& job, bool enableLogging)
{
    if (job->GetUnregistered()) {
        return;
    }

    job->SetUnregistered(true);

    auto* operationState = FindOperationState(job->GetOperationId());
    const auto& node = job->GetNode();

    YT_VERIFY(node->Jobs().erase(job) == 1);
    YT_VERIFY(node->IdToJob().erase(job->GetId()) == 1);
    --ActiveJobCount_;

    ResetJobWaitingForConfirmation(job);

    if (operationState && operationState->Jobs.erase(job->GetId())) {
        JobsToSubmitToStrategy_[job->GetId()] =
            TJobUpdate{
                EJobUpdateStatus::Finished,
                job->GetOperationId(),
                job->GetId(),
                job->GetTreeId(),
                TJobResources()};
        operationState->JobsToSubmitToStrategy.insert(job->GetId());

        YT_LOG_DEBUG_IF(enableLogging, "Job unregistered (JobId: %v, OperationId: %v, State: %v)",
            job->GetId(),
            job->GetOperationId(),
            job->GetState());
    } else {
        YT_LOG_DEBUG_IF(enableLogging, "Dangling job unregistered (JobId: %v, OperationId: %v, State: %v)",
            job->GetId(),
            job->GetOperationId(),
            job->GetState());
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
        YT_VERIFY(node->RecentlyFinishedJobs().insert(
            {jobId, TRecentlyFinishedJobInfo{job->GetOperationId(), finishedStoringEvictionDeadline}}).second);
        YT_VERIFY(operationState->RecentlyFinishedJobIds.insert(jobId).second);
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
    auto storingEvictionDeadline = GetCpuInstant() + DurationToCpuDuration(Config_->FinishedOperationJobStoringTimeout);

    for (auto jobId : operationState->RecentlyFinishedJobIds) {
        auto node = FindNodeByJob(jobId);
        YT_VERIFY(node);

        auto& finishedJobInfo = GetOrCrash(node->RecentlyFinishedJobs(), jobId);
        finishedJobInfo.EvictionDeadline = storingEvictionDeadline;
    }

    operationState->RecentlyFinishedJobIds.clear();
}

void TNodeShard::PreemptJob(const TJobPtr& job, std::optional<TCpuDuration> interruptTimeout)
{
    YT_LOG_DEBUG("Preempting job (JobId: %v, OperationId: %v, Interruptible: %v, Reason: %v)",
        job->GetId(),
        job->GetOperationId(),
        job->GetInterruptible(),
        job->GetPreemptionReason());

    job->SetPreempted(true);

    if (interruptTimeout) {
        DoInterruptJob(job, EInterruptReason::Preemption, *interruptTimeout);
    }
}

void TNodeShard::DoInterruptJob(
    const TJobPtr& job,
    EInterruptReason reason,
    TCpuDuration interruptTimeout,
    const std::optional<TString>& interruptUser)
{
    YT_LOG_DEBUG("Interrupting job (Reason: %v, InterruptTimeout: %.3g, JobId: %v, OperationId: %v, User: %v)",
        reason,
        CpuDurationToDuration(interruptTimeout).SecondsFloat(),
        job->GetId(),
        job->GetOperationId(),
        interruptUser);

    if (job->GetInterruptReason() == EInterruptReason::None && reason != EInterruptReason::None) {
        job->SetInterruptReason(reason);
    }

    if (interruptTimeout != 0) {
        auto interruptDeadline = GetCpuInstant() + interruptTimeout;
        if (job->GetInterruptDeadline() == 0 || interruptDeadline < job->GetInterruptDeadline()) {
            job->SetInterruptDeadline(interruptDeadline);
        }
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
    auto addressWithNetwork = job->GetNode()->NodeDescriptor().GetAddressWithNetworkOrThrow(Bootstrap_->GetLocalNetworks());
    return Host_->CreateJobProberProxy(addressWithNetwork);
}

TNodeShard::TOperationState* TNodeShard::FindOperationState(TOperationId operationId)
{
    auto it = IdToOpertionState_.find(operationId);
    return it != IdToOpertionState_.end() ? &it->second : nullptr;
}

TNodeShard::TOperationState& TNodeShard::GetOperationState(TOperationId operationId)
{
    return GetOrCrash(IdToOpertionState_, operationId);
}

void TNodeShard::BuildNodeYson(const TExecNodePtr& node, TFluentMap fluent)
{
    fluent
        .Item(node->GetDefaultAddress()).BeginMap()
            .Do([&] (TFluentMap fluent) {
                node->BuildAttributes(fluent);
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
