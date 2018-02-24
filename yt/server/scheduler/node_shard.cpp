#include "node_shard.h"
#include "config.h"
#include "helpers.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "operation_controller.h"
#include "controller_agent.h"

#include <yt/server/exec_agent/public.h>

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/server/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/server/shell/config.h>

#include <yt/ytlib/job_proxy/public.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/job_tracker_client/job_tracker_service.pb.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/core/misc/finally.h>

#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/actions/cancelable_context.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

using namespace NCellScheduler;
using namespace NChunkClient;
using namespace NCypressClient;
using namespace NConcurrency;
using namespace NJobProberClient;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerServer;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NShell;
using namespace NYTree;
using namespace NYson;

using NNodeTrackerClient::TNodeId;
using NScheduler::NProto::TSchedulerJobResultExt;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = SchedulerProfiler;

static NProfiling::TAggregateCounter AnalysisTimeCounter;
static NProfiling::TAggregateCounter StrategyJobProcessingTimeCounter;
static NProfiling::TAggregateCounter ScheduleTimeCounter;

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
    , CachedResourceLimitsByTags_(New<TExpiringCache<TSchedulingTagFilter, TJobResources>>(
        BIND(&TNodeShard::CalculateResourceLimits, MakeStrong(this)),
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
}

void TNodeShard::OnMasterConnected()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    DoCleanup();

    YCHECK(!Connected_);
    Connected_ = true;

    YCHECK(!CancelableContext_);
    CancelableContext_ = New<TCancelableContext>();
    CancelableInvoker_ = CancelableContext_->CreateInvoker(GetInvoker());

    CachedExecNodeDescriptorsRefresher_->Start();
    CachedResourceLimitsByTags_->Start();
}

void TNodeShard::OnMasterDisconnected()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    DoCleanup();
}

void TNodeShard::DoCleanup()
{
    Connected_ = false;

    if (CancelableContext_) {
        CancelableContext_->Cancel();
        CancelableContext_.Reset();
    }

    CancelableInvoker_.Reset();

    CachedExecNodeDescriptorsRefresher_->Stop();
    CachedResourceLimitsByTags_->Stop();

    for (const auto& pair : IdToNode_) {
        const auto& node = pair.second;
        TLeaseManager::CloseLease(node->GetLease());
    }

    IdToOpertionState_.clear();

    IdToNode_.clear();
    ExecNodeCount_ = 0;
    TotalNodeCount_ = 0;

    ActiveJobCount_ = 0;

    {
        TWriterGuard guard(JobCounterLock_);
        for (auto state : TEnumTraits<EJobState>::GetDomainValues()) {
            for (auto type : TEnumTraits<EJobType>::GetDomainValues()) {
                JobCounter_[state][type] = 0;
                for (auto reason : TEnumTraits<EAbortReason>::GetDomainValues()) {
                    AbortedJobCounter_[reason][state][type] = 0;
                }
                for (auto reason : TEnumTraits<EInterruptReason>::GetDomainValues()) {
                    CompletedJobCounter_[reason][state][type] = 0;
                }
            }
        }
    }

    UpdatedJobs_.clear();
    CompletedJobs_.clear();

    ConcurrentHeartbeatCount_ = 0;

    JobIdToAsyncScheduleResult_.clear();

    SubmitJobsToStrategy();
}

void TNodeShard::RegisterOperation(
    const TOperationId& operationId,
    const IOperationControllerPtr& controller,
    bool willRevive)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YCHECK(Connected_);

    YCHECK(IdToOpertionState_.emplace(
        operationId,
        TOperationState(controller, !willRevive /* jobsReady */, CurrentEpoch_++)
    ).second);

    LOG_DEBUG("Operation registered at node shard (OperationId: %v, WillRevive: %v)",
        operationId,
        willRevive);
}

void TNodeShard::StartOperationRevival(const TOperationId& operationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YCHECK(Connected_);

    auto& operationState = GetOperationState(operationId);
    operationState.JobsReady = false;
    operationState.Jobs.clear();

    LOG_DEBUG("Operation revival started at node shard (OperationId: %v)",
        operationId);
}

void TNodeShard::FinishOperationRevival(const TOperationId& operationId, const std::vector<TJobPtr>& jobs)
{
    auto& operationState = GetOperationState(operationId);

    YCHECK(!operationState.JobsReady);
    operationState.JobsReady = true;
    operationState.ForbidNewJobs = false;
    operationState.Terminated = false;

    for (const auto& job : jobs) {
        auto node = GetOrRegisterNode(
            job->GetRevivalNodeId(),
            TNodeDescriptor(job->GetRevivalNodeAddress()));
        job->SetNode(node);
        job->SetWaitingForConfirmation(true);
        node->UnconfirmedJobIds().emplace(job->GetId());
        RegisterJob(job);
    }

    LOG_DEBUG("Operation revival finished at node shard (OperationId: %v, RevivedJobCount: %v)",
        operationId,
        jobs.size());

    // Give some time for nodes to confirm the jobs.
    TDelayedExecutor::Submit(
        BIND(&TNodeShard::AbortUnconfirmedJobs, MakeWeak(this), operationId, operationState.Epoch, jobs)
            .Via(GetInvoker()),
        Config_->JobRevivalAbortTimeout);
}

void TNodeShard::UnregisterOperation(const TOperationId& operationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    YCHECK(Connected_);

    auto it = IdToOpertionState_.find(operationId);
    YCHECK(it != IdToOpertionState_.end());
    auto& operationState = it->second;

    for (const auto& job : operationState.Jobs) {
        YCHECK(job.second->GetHasPendingUnregistration());
    }

    IdToOpertionState_.erase(it);

    LOG_DEBUG("Operation unregistered from node shard (OperationId: %v)",
        operationId);
}

void TNodeShard::ProcessHeartbeat(const TScheduler::TCtxNodeHeartbeatPtr& context)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    if (!Connected_) {
        context->Reply(TError(
            NRpc::EErrorCode::Unavailable,
            "Scheduler is not able to accept node heartbeats"));
        return;
    }

    CancelableInvoker_->Invoke(BIND(&TNodeShard::DoProcessHeartbeat, MakeStrong(this), context));
}

void TNodeShard::DoProcessHeartbeat(const TScheduler::TCtxNodeHeartbeatPtr& context)
{
    VERIFY_INVOKER_AFFINITY(CancelableInvoker_);

    auto* request = &context->Request();
    auto* response = &context->Response();

    auto nodeId = request->node_id();
    auto descriptor = FromProto<TNodeDescriptor>(request->node_descriptor());
    const auto& resourceLimits = request->resource_limits();
    const auto& resourceUsage = request->resource_usage();

    context->SetRequestInfo("NodeId: %v, Address: %v, ResourceUsage: %v, JobCount: %v, Confirmation: {C: %v, U: %v}",
        nodeId,
        descriptor.GetDefaultAddress(),
        FormatResourceUsage(TJobResources(resourceUsage), TJobResources(resourceLimits), request->disk_info()),
        request->jobs().size(),
        request->confirmed_job_count(),
        request->unconfirmed_jobs().size());

    YCHECK(Host_->GetNodeShardId(nodeId) == Id_);

    auto node = GetOrRegisterNode(nodeId, descriptor);

    // NB: Resource limits and usage of node should be updated even if
    // node is offline to avoid getting incorrect total limits when node becomes online.
    UpdateNodeResources(node,
        request->resource_limits(),
        request->resource_usage(),
        request->disk_info());

    if (node->GetMasterState() != ENodeState::Online) {
        context->Reply(TError("Node is not online"));
        return;
    }

    // We should process only one heartbeat at a time from the same node.
    if (node->GetHasOngoingHeartbeat()) {
        context->Reply(TError("Node already has an ongoing heartbeat"));
        return;
    }

    TLeaseManager::RenewLease(node->GetLease());

    bool isThrottlingActive = false;
    if (ConcurrentHeartbeatCount_ > Config_->HardConcurrentHeartbeatLimit) {
        isThrottlingActive = true;
        LOG_INFO("Hard heartbeat limit reached (NodeAddress: %v, Limit: %v)",
            node->GetDefaultAddress(),
            Config_->HardConcurrentHeartbeatLimit);
    } else if (ConcurrentHeartbeatCount_ > Config_->SoftConcurrentHeartbeatLimit &&
        node->GetLastSeenTime() + Config_->HeartbeatProcessBackoff > TInstant::Now())
    {
        isThrottlingActive = true;
        LOG_INFO("Soft heartbeat limit reached (NodeAddress: %v, Limit: %v)",
            node->GetDefaultAddress(),
            Config_->SoftConcurrentHeartbeatLimit);
    }

    response->set_enable_job_reporter(Config_->EnableJobReporter);
    response->set_enable_job_spec_reporter(Config_->EnableJobSpecReporter);
    response->set_operation_archive_version(Host_->GetOperationArchiveVersion());

    BeginNodeHeartbeatProcessing(node);
    auto finallyGuard = Finally([&] { EndNodeHeartbeatProcessing(node); });

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

    if (hasWaitingJobs || isThrottlingActive) {
        if (hasWaitingJobs) {
            LOG_DEBUG("Waiting jobs found, suppressing new jobs scheduling");
        }
        if (isThrottlingActive) {
            LOG_DEBUG("Throttling is active, suppressing new jobs scheduling");
        }
        response->set_scheduling_skipped(true);
    } else {
        auto schedulingContext = CreateSchedulingContext(
            Config_,
            node,
            runningJobs);

        SubmitJobsToStrategy();

        PROFILE_AGGREGATED_TIMING (StrategyJobProcessingTimeCounter) {
            SubmitJobsToStrategy();
        }

        PROFILE_AGGREGATED_TIMING (ScheduleTimeCounter) {
            node->SetHasOngoingJobsScheduling(true);
            Y_UNUSED(WaitFor(Host_->GetStrategy()->ScheduleJobs(schedulingContext)));
            node->SetHasOngoingJobsScheduling(false);
        }

        TotalResourceUsage_ -= node->GetResourceUsage();
        node->SetResourceUsage(schedulingContext->ResourceUsage());
        TotalResourceUsage_ += node->GetResourceUsage();

        ProcessScheduledJobs(
            schedulingContext,
            context);

        SubmitJobsToStrategy();

        // NB: some jobs maybe considered aborted after processing scheduled jobs.
        PROFILE_AGGREGATED_TIMING (StrategyJobProcessingTimeCounter) {
            SubmitJobsToStrategy();
        }

        response->set_scheduling_skipped(false);
    }

    std::vector<TJobPtr> jobsWithPendingUnregistration;
    for (const auto& job : node->Jobs()) {
        if (job->GetHasPendingUnregistration()) {
            jobsWithPendingUnregistration.push_back(job);
        }
    }

    for (const auto& job : jobsWithPendingUnregistration) {
        DoUnregisterJob(job);
    }

    context->Reply();
}

TRefCountedExecNodeDescriptorMapPtr TNodeShard::GetExecNodeDescriptors()
{
    UpdateExecNodeDescriptors();

    {
        TReaderGuard guard(CachedExecNodeDescriptorsLock_);
        return CachedExecNodeDescriptors_;
    }
}

void TNodeShard::UpdateExecNodeDescriptors()
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto result = New<TRefCountedExecNodeDescriptorMap>();
    result->reserve(IdToNode_.size());
    for (const auto& pair : IdToNode_) {
        const auto& node = pair.second;
        if (node->GetMasterState() == ENodeState::Online) {
            YCHECK(result->emplace(node->GetId(), node->BuildExecDescriptor()).second);
        }
    }

    {
        TWriterGuard guard(CachedExecNodeDescriptorsLock_);
        CachedExecNodeDescriptors_ = result;
    }
}

void TNodeShard::UpdateNodeState(const TExecNodePtr& node, ENodeState newState)
{
    auto oldState = node->GetMasterState();
    node->SetMasterState(newState);

    if (oldState != newState) {
        LOG_INFO("Node state changed (NodeId: %v, Address: %v, State: %v -> %v)",
            node->GetId(),
            node->NodeDescriptor().GetDefaultAddress(),
            oldState,
            newState);
    }
}

void TNodeShard::HandleNodesAttributes(const std::vector<std::pair<TString, INodePtr>>& nodeMaps)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    if (HasOngoingNodesAttributesUpdate_) {
        LOG_WARNING("Node shard is handling nodes attributes update for too long, skipping new update");
        return;
    }

    HasOngoingNodesAttributesUpdate_ = true;
    auto finallyGuard = Finally([&] { HasOngoingNodesAttributesUpdate_ = false; });

    for (const auto& nodeMap : nodeMaps) {
        const auto& address = nodeMap.first;
        const auto& attributes = nodeMap.second->Attributes();
        auto objectId = attributes.Get<TObjectId>("id");
        auto nodeId = NodeIdFromObjectId(objectId);
        auto newState = attributes.Get<ENodeState>("state");
        auto ioWeights = attributes.Get<THashMap<TString, double>>("io_weights", {});

        LOG_DEBUG("Handling node attributes (NodeId: %v, Address: %v, ObjectId: %v, NewState: %v)",
            nodeId,
            address,
            objectId,
            newState);

        YCHECK(Host_->GetNodeShardId(nodeId) == Id_);

        if (IdToNode_.find(nodeId) == IdToNode_.end()) {
            if (newState == ENodeState::Online) {
                LOG_WARNING("Node is not registered at scheduler but online at master (NodeId: %v, Address: %v)",
                    nodeId,
                    address);
            }
            continue;
        }

        auto execNode = IdToNode_[nodeId];
        execNode->SetIOWeights(ioWeights);

        auto oldState = execNode->GetMasterState();
        auto tags = attributes.Get<THashSet<TString>>("tags");

        if (oldState == ENodeState::Online && newState != ENodeState::Online) {
            // NOTE: Tags will be validated when node become online, no need in additional check here.
            execNode->Tags() = tags;
            SubtractNodeResources(execNode);
            AbortAllJobsAtNode(execNode);
            UpdateNodeState(execNode, newState);
            return;
        }

        if ((oldState != ENodeState::Online && newState == ENodeState::Online) || execNode->Tags() != tags) {
            auto updateResult = WaitFor(Host_->RegisterOrUpdateNode(nodeId, address, tags));
            if (!updateResult.IsOK()) {
                LOG_WARNING(updateResult, "Node tags update failed (NodeId: %v, Address: %v, NewTags: %v)",
                    nodeId,
                    address,
                    tags);

                if (oldState == ENodeState::Online) {
                    SubtractNodeResources(execNode);
                    AbortAllJobsAtNode(execNode);
                    UpdateNodeState(execNode, ENodeState::Offline);
                }
            } else {
                if (oldState != ENodeState::Online && newState == ENodeState::Online) {
                    AddNodeResources(execNode);
                }
                execNode->Tags() = tags;
                UpdateNodeState(execNode, newState);
            }
        }
    }
}

void TNodeShard::AbortOperationJobs(const TOperationId& operationId, const TError& abortReason, bool terminated)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto* operationState = FindOperationState(operationId);
    if (!operationState) {
        return;
    }

    operationState->Terminated = terminated;
    operationState->ForbidNewJobs = true;
    auto jobs = operationState->Jobs;
    for (const auto& job : jobs) {
        auto status = JobStatusFromError(abortReason);
        OnJobAborted(job.second, &status, terminated);
    }

    for (const auto& job : operationState->Jobs) {
        YCHECK(job.second->GetHasPendingUnregistration());
    }
}

void TNodeShard::ResumeOperationJobs(const TOperationId& operationId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto* operationState = FindOperationState(operationId);
    if (!operationState || operationState->Terminated) {
        return;
    }

    operationState->ForbidNewJobs = false;
}

TYsonString TNodeShard::StraceJob(const TJobId& jobId, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = GetJobOrThrow(jobId);

    Host_->ValidateOperationPermission(user, job->GetOperationId(), EPermission::Write);

    LOG_DEBUG("Getting strace dump (JobId: %v, OperationId: %v)",
        job->GetId(),
        job->GetOperationId());

    auto proxy = CreateJobProberProxy(job);
    auto req = proxy.Strace();
    ToProto(req->mutable_job_id(), jobId);

    auto rspOrError = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting strace dump of job %v",
        jobId);

    const auto& rsp = rspOrError.Value();

    LOG_DEBUG("Strace dump received (JobId: %v, OperationId: %v)",
        job->GetId(),
        job->GetOperationId());

    return TYsonString(rsp->trace());
}

void TNodeShard::DumpJobInputContext(const TJobId& jobId, const TYPath& path, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = GetJobOrThrow(jobId);

    Host_->ValidateOperationPermission(user, job->GetOperationId(), EPermission::Write);

    LOG_DEBUG("Saving input contexts (JobId: %v, OperationId: %v, Path: %v, User: %v)",
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
    YCHECK(chunkIds.size() == 1);

    auto asyncResult = Host_->AttachJobContext(path, chunkIds.front(), job->GetOperationId(), jobId, user);
    WaitFor(asyncResult)
        .ThrowOnError();

    LOG_DEBUG("Input contexts saved (JobId: %v, OperationId: %v)",
        job->GetId(),
        job->GetOperationId());
}

TNodeDescriptor TNodeShard::GetJobNode(const TJobId& jobId, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());
    auto job = GetJobOrThrow(jobId);

    Host_->ValidateOperationPermission(user, job->GetOperationId(), EPermission::Write);

    return job->GetNode()->NodeDescriptor();
}

void TNodeShard::SignalJob(const TJobId& jobId, const TString& signalName, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = GetJobOrThrow(jobId);

    Host_->ValidateOperationPermission(user, job->GetOperationId(), EPermission::Write);

    LOG_DEBUG("Sending job signal (JobId: %v, OperationId: %v, Signal: %v)",
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

    LOG_DEBUG("Job signal sent (JobId: %v, OperationId: %v)",
        job->GetId(),
        job->GetOperationId());
}

void TNodeShard::AbandonJob(const TJobId& jobId, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = GetJobOrThrow(jobId);

    Host_->ValidateOperationPermission(user, job->GetOperationId(), EPermission::Write);

    LOG_DEBUG("Abandoning job by user request (JobId: %v, OperationId: %v, User: %v)",
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

TYsonString TNodeShard::PollJobShell(const TJobId& jobId, const TYsonString& parameters, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = GetJobOrThrow(jobId);

    TShellParameters shellParameters;
    Deserialize(shellParameters, ConvertToNode(parameters));
    if (shellParameters.Operation == EShellOperation::Spawn) {
        Host_->ValidateOperationPermission(user, job->GetOperationId(), EPermission::Write);
    }

    LOG_DEBUG("Polling job shell (JobId: %v, OperationId: %v, Parameters: %v)",
        job->GetId(),
        job->GetOperationId(),
        ConvertToYsonString(parameters, EYsonFormat::Text));

    auto proxy = CreateJobProberProxy(job);
    auto req = proxy.PollJobShell();
    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_parameters(), parameters.GetData());

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Error polling job shell for job %v", jobId)
            << rspOrError
            << TErrorAttribute("parameters", parameters);
    }

    const auto& rsp = rspOrError.Value();
    return TYsonString(rsp->result());
}

void TNodeShard::AbortJobByUserRequest(const TJobId& jobId, TNullable<TDuration> interruptTimeout, const TString& user)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = GetJobOrThrow(jobId);

    Host_->ValidateOperationPermission(user, job->GetOperationId(), EPermission::Write);

    if (job->GetState() != EJobState::Running &&
        job->GetState() != EJobState::Waiting)
    {
        THROW_ERROR_EXCEPTION("Cannot abort job %v of operation %v since it is not running",
            jobId,
            job->GetOperationId());
    }

    if (interruptTimeout.Get(TDuration::Zero()) != TDuration::Zero()) {
        if (!job->GetInterruptible()) {
            THROW_ERROR_EXCEPTION("Cannot interrupt job %v of type %Qlv because such job type does not support interruption",
                jobId,
                job->GetType());
        }

        LOG_DEBUG("Trying to interrupt job by user request (JobId: %v, InterruptTimeout: %v)",
            jobId,
            interruptTimeout);

        auto proxy = CreateJobProberProxy(job);
        auto req = proxy.Interrupt();
        ToProto(req->mutable_job_id(), jobId);

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error interrupting job %v",
            jobId);

        LOG_INFO("User interrupt requested (JobId: %v, InterruptTimeout: %v)",
            jobId,
            interruptTimeout);

        DoInterruptJob(job, EInterruptReason::UserRequest, DurationToCpuDuration(*interruptTimeout), user);
    } else {
        LOG_DEBUG("Aborting job by user request (JobId: %v, OperationId: %v, User: %v)",
            jobId,
            job->GetOperationId(),
            user);

        auto status = JobStatusFromError(TError("Job aborted by user request")
            << TErrorAttribute("abort_reason", EAbortReason::UserRequest)
            << TErrorAttribute("user", user));
        OnJobAborted(job, &status);
    }
}

void TNodeShard::AbortJob(const TJobId& jobId, const TError& error)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = FindJob(jobId);
    if (!job) {
        LOG_DEBUG("Cannot abort an unknown job (JobId: %v)", jobId);
        return;
    }
    LOG_DEBUG(error, "Aborting job by internal request (JobId: %v, OperationId: %v)",
        jobId,
        job->GetOperationId());

    auto status = JobStatusFromError(error);
    OnJobAborted(job, &status);
}

void TNodeShard::FailJob(const TJobId& jobId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = FindJob(jobId);
    if (!job) {
        LOG_DEBUG("Cannot fail an unknown job (JobId: %v)", jobId);
        return;
    }

    LOG_DEBUG("Failing job by internal request (JobId: %v, OperationId: %v)",
        jobId,
        job->GetOperationId());

    job->SetFailRequested(true);
}

void TNodeShard::ReleaseJob(const TJobId& jobId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    // NB: While we kept job id in operation controller, its execution node
    // could have been unregistered.
    auto nodeId = NodeIdFromJobId(jobId);
    if (auto execNode = FindNodeByJob(jobId)) {
        LOG_DEBUG("Adding job that should be removed (JobId: %v, NodeId: %v, NodeAddress: %v)",
            jobId,
            nodeId,
            execNode->GetDefaultAddress());
        execNode->JobIdsToRemove().emplace_back(jobId);
    } else {
        LOG_DEBUG("Execution node was unregistered for a job that should be removed (JobId: %v, NodeId: %v)",
            jobId,
            nodeId);
    }
}

void TNodeShard::BuildNodesYson(TFluentMap fluent)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    for (const auto& pair : IdToNode_) {
        BuildNodeYson(pair.second, fluent);
    }
}

TOperationId TNodeShard::FindOperationIdByJobId(const TJobId& jobId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto job = FindJob(jobId);
    return job ? job->GetOperationId() : TOperationId();
}

TJobResources TNodeShard::GetTotalResourceLimits()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(ResourcesLock_);

    return TotalResourceLimits_;
}

TJobResources TNodeShard::GetTotalResourceUsage()
{
    VERIFY_THREAD_AFFINITY_ANY();

    TReaderGuard guard(ResourcesLock_);

    return TotalResourceUsage_;
}

TJobResources TNodeShard::CalculateResourceLimits(const TSchedulingTagFilter& filter)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TJobResources resources;

    TRefCountedExecNodeDescriptorMapPtr descriptors;
    {
        TReaderGuard guard(CachedExecNodeDescriptorsLock_);
        descriptors = CachedExecNodeDescriptors_;
    }

    for (const auto& pair : *descriptors) {
        const auto& descriptor = pair.second;
        if (descriptor.CanSchedule(filter)) {
            resources += descriptor.ResourceLimits;
        }
    }
    return resources;
}

TJobResources TNodeShard::GetResourceLimits(const TSchedulingTagFilter& filter)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (filter.IsEmpty()) {
        return TotalResourceLimits_;
    }

    return CachedResourceLimitsByTags_->Get(filter);
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

TFuture<TScheduleJobResultPtr> TNodeShard::BeginScheduleJob(const TJobId& jobId)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto promise = NewPromise<TScheduleJobResultPtr>();
    YCHECK(JobIdToAsyncScheduleResult_.emplace(jobId, promise).second);
    return promise.ToFuture();
}

void TNodeShard::EndScheduleJob(const NProto::TScheduleJobResponse& response)
{
    VERIFY_INVOKER_AFFINITY(GetInvoker());

    auto jobId = FromProto<TJobId>(response.job_id());
    LOG_DEBUG("Job schedule response received (JobId: %v, Success: %v)",
        jobId,
        response.has_job_type());

    auto result = New<TScheduleJobResult>();
    if (response.has_job_type()) {
        result->StartDescriptor.Emplace(
            jobId,
            static_cast<EJobType>(response.job_type()),
            FromProto<TJobResources>(response.resource_limits()),
            response.interruptible());
    }
    for (const auto& protoCounter : response.failed()) {
        result->Failed[static_cast<EScheduleJobFailReason>(protoCounter.reason())] = protoCounter.value();
    }
    FromProto(&result->Duration, response.duration());

    auto it = JobIdToAsyncScheduleResult_.find(jobId);
    YCHECK(it != JobIdToAsyncScheduleResult_.end());
    it->second.Set(result);
    JobIdToAsyncScheduleResult_.erase(it);
}

TExecNodePtr TNodeShard::GetOrRegisterNode(TNodeId nodeId, const TNodeDescriptor& descriptor)
{
    auto it = IdToNode_.find(nodeId);
    if (it == IdToNode_.end()) {
        return RegisterNode(nodeId, descriptor);
    }

    auto node = it->second;
    // Update the current descriptor, just in case.
    node->NodeDescriptor() = descriptor;
    return node;
}

void TNodeShard::OnNodeLeaseExpired(TNodeId nodeId)
{
    auto it = IdToNode_.find(nodeId);
    YCHECK(it != IdToNode_.end());

    // NB: Make a copy; the calls below will mutate IdToNode_ and thus invalidate it.
    auto node = it->second;

    LOG_INFO("Node lease expired, unregistering (Address: %v)",
        node->GetDefaultAddress());

    UnregisterNode(node);
}

TExecNodePtr TNodeShard::RegisterNode(TNodeId nodeId, const TNodeDescriptor& descriptor)
{
    auto node = New<TExecNode>(nodeId, descriptor);
    const auto& address = node->GetDefaultAddress();

    auto lease = TLeaseManager::CreateLease(
        Config_->NodeHeartbeatTimeout,
        BIND(&TNodeShard::OnNodeLeaseExpired, MakeWeak(this), node->GetId())
            .Via(GetInvoker()));

    node->SetLease(lease);
    YCHECK(IdToNode_.insert(std::make_pair(node->GetId(), node)).second);

    LOG_INFO("Node registered (Address: %v)", address);

    return node;
}

void TNodeShard::UnregisterNode(const TExecNodePtr& node)
{
    if (node->GetHasOngoingHeartbeat()) {
        LOG_INFO("Node unregistration postponed until heartbeat is finished (Address: %v)",
            node->GetDefaultAddress());
        node->SetHasPendingUnregistration(true);
    } else {
        DoUnregisterNode(node);
    }
}

void TNodeShard::DoUnregisterNode(const TExecNodePtr& node)
{
    if (node->GetMasterState() == ENodeState::Online) {
        SubtractNodeResources(node);
    }

    AbortAllJobsAtNode(node);

    YCHECK(IdToNode_.erase(node->GetId()) == 1);

    const auto& address = node->GetDefaultAddress();

    Host_->UnregisterNode(node->GetId(), address);

    LOG_INFO("Node unregistered (Address: %v)", address);
}

void TNodeShard::AbortAllJobsAtNode(const TExecNodePtr& node)
{
    // Make a copy, the collection will be modified.
    auto jobs = node->Jobs();
    const auto& address = node->GetDefaultAddress();
    for (const auto& job : jobs) {
        LOG_DEBUG("Aborting job on an offline node (Address: %v, JobId: %v, OperationId: %v)",
            address,
            job->GetId(),
            job->GetOperationId());
        auto status = JobStatusFromError(
            TError("Node offline")
            << TErrorAttribute("abort_reason", EAbortReason::NodeOffline));
        OnJobAborted(job, &status);
    }
}

void TNodeShard::AbortUnconfirmedJobs(
    const TOperationId& operationId,
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
        LOG_INFO("All revived jobs were confirmed (OperationId: %v, RevivedJobCount: %v)",
            operationId,
            jobs.size());
        return;
    }

    LOG_WARNING("Aborting revived jobs that were not confirmed (OperationId: %v, RevivedJobCount: %v, "
        "JobRevivalAbortTimeout: %v, UnconfirmedJobCount: %v)",
        operationId,
        jobs.size(),
        Config_->JobRevivalAbortTimeout,
        unconfirmedJobs.size());

    auto status = JobStatusFromError(
        TError("Job not confirmed after timeout")
            << TErrorAttribute("abort_reason", EAbortReason::RevivalConfirmationTimeout));
    for (const auto& job : unconfirmedJobs) {
        LOG_DEBUG("Aborting revived job that was not confirmed (OperationId: %v, JobId: %v)",
            operationId,
            job->GetId());
        OnJobAborted(job, &status);
        if (auto node = job->GetNode()) {
            node->UnconfirmedJobIds().erase(job->GetId());
        }
    }
}

void TNodeShard::ProcessHeartbeatJobs(
    const TExecNodePtr& node,
    NJobTrackerClient::NProto::TReqHeartbeat* request,
    NJobTrackerClient::NProto::TRspHeartbeat* response,
    std::vector<TJobPtr>* runningJobs,
    bool* hasWaitingJobs)
{
    auto now = GetCpuInstant();

    bool forceJobsLogging = false;
    auto lastJobsLogTime = node->GetLastJobsLogTime();
    if (!lastJobsLogTime || now > lastJobsLogTime.Get() + DurationToCpuDuration(Config_->JobsLoggingPeriod)) {
        forceJobsLogging = true;
        node->SetLastJobsLogTime(now);
    }

    bool checkMissingJobs = false;
    auto lastCheckMissingJobsTime = node->GetLastCheckMissingJobsTime();
    if ((!lastCheckMissingJobsTime ||
        now > lastCheckMissingJobsTime.Get() + DurationToCpuDuration(Config_->MissingJobsCheckPeriod)) &&
        node->UnconfirmedJobIds().empty())
    {
        checkMissingJobs = true;
        node->SetLastCheckMissingJobsTime(now);
    }

    const auto& nodeId = node->GetId();
    const auto& nodeAddress = node->GetDefaultAddress();

    if (!node->UnconfirmedJobIds().empty()) {
        LOG_DEBUG("Asking node to include stored jobs in the next heartbeat (NodeId: %v, NodeAddress: %v)",
            nodeId,
            nodeAddress);
        ToProto(response->mutable_jobs_to_confirm(), node->UnconfirmedJobIds());
        // If it is a first time we get the heartbeat from a given node,
        // there will definitely be some jobs that are missing. No need to abort
        // them.
    }

    if (checkMissingJobs) {
        // Verify that all flags are in initial state.
        for (const auto& job : node->Jobs()) {
            YCHECK(!job->GetFoundOnNode());
        }
    }

    {
        for (const auto& jobId : node->JobIdsToRemove()) {
            LOG_DEBUG("Asking node to remove job and removing it from recently completed job ids "
                "(JobId: %v, NodeId: %v, NodeAddress: %v)",
                jobId,
                nodeId,
                nodeAddress);
            node->RecentlyCompletedJobIds().erase(jobId);
            ToProto(response->add_jobs_to_remove(), jobId);
        }
        node->JobIdsToRemove().clear();
    }

    for (auto& jobStatus : *request->mutable_jobs()) {
        YCHECK(jobStatus.has_job_type());
        auto jobType = EJobType(jobStatus.job_type());
        // Skip jobs that are not issued by the scheduler.
        if (jobType <= EJobType::SchedulerFirst || jobType >= EJobType::SchedulerLast) {
            continue;
        }

        auto job = ProcessJobHeartbeat(
            node,
            request,
            response,
            &jobStatus,
            forceJobsLogging);
        if (job) {
            if (checkMissingJobs) {
                job->SetFoundOnNode(true);
            }
            switch (job->GetState()) {
                case EJobState::Running:
                    runningJobs->push_back(job);
                    break;
                case EJobState::Waiting:
                    *hasWaitingJobs = true;
                    break;
                default:
                    break;
            }
        }
    }

    if (checkMissingJobs) {
        std::vector<TJobPtr> missingJobs;
        for (const auto& job : node->Jobs()) {
            YCHECK(!job->GetWaitingForConfirmation());
            // Jobs that are waiting for confirmation may never be considered missing.
            // They are removed in two ways: by explicit unconfirmation of the node
            // or after revival confirmation timeout.
            if (!job->GetFoundOnNode()) {
                LOG_ERROR("Job is missing (Address: %v, JobId: %v, OperationId: %v)",
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
            OnJobAborted(job, &status);
        }
    }

    for (const auto& jobId : FromProto<std::vector<TJobId>>(request->unconfirmed_jobs())) {
        auto job = FindJob(jobId);
        if (!job) {
            // This may happen if we received heartbeat after job was removed by some different reasons
            // (like confirmation timeout).
            continue;
        }
        auto status = JobStatusFromError(TError("Job not confirmed by node"));
        OnJobAborted(job, &status);
        node->UnconfirmedJobIds().erase(jobId);
    }
}

NLogging::TLogger TNodeShard::CreateJobLogger(
    const TJobId& jobId,
    const TOperationId& operationId,
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
    NJobTrackerClient::NProto::TReqHeartbeat* request,
    NJobTrackerClient::NProto::TRspHeartbeat* response,
    TJobStatus* jobStatus,
    bool forceJobsLogging)
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
            LOG_DEBUG("Job is skipped because operations jobs are not ready yet");
            return nullptr;
        }
        if (node->RecentlyCompletedJobIds().has(jobId)) {
            LOG_DEBUG("Job is skipped because it was recently completed and not persisted to snapshot yet");
            return nullptr;
        }
        switch (state) {
            case EJobState::Completed:
                LOG_DEBUG("Unknown job has completed, removal scheduled");
                ToProto(response->add_jobs_to_remove(), jobId);
                break;

            case EJobState::Failed:
                LOG_DEBUG("Unknown job has failed, removal scheduled");
                ToProto(response->add_jobs_to_remove(), jobId);
                break;

            case EJobState::Aborted:
                LOG_DEBUG(FromProto<TError>(jobStatus->result().error()), "Job aborted, removal scheduled");
                ToProto(response->add_jobs_to_remove(), jobId);
                break;

            case EJobState::Running:
                LOG_DEBUG("Unknown job is running, abort scheduled");
                ToProto(response->add_jobs_to_abort(), jobId);
                break;

            case EJobState::Waiting:
                LOG_DEBUG("Unknown job is waiting, abort scheduled");
                ToProto(response->add_jobs_to_abort(), jobId);
                break;

            case EJobState::Aborting:
                LOG_DEBUG("Job is aborting");
                break;

            default:
                Y_UNREACHABLE();
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
            ToProto(response->add_jobs_to_remove(), jobId);
            LOG_WARNING("Job status report was expected from %v, removal scheduled",
                expectedAddress);
        } else {
            ToProto(response->add_jobs_to_abort(), jobId);
            LOG_WARNING("Job status report was expected from %v, abort scheduled",
                expectedAddress);
        }
        return nullptr;
    }

    if (job->GetWaitingForConfirmation()) {
        LOG_DEBUG("Job confirmed (JobId: %v, State: %v)",
            jobId,
            state);
        job->SetWaitingForConfirmation(false);
        node->UnconfirmedJobIds().erase(job->GetId());
    }

    bool shouldLogJob = (state != job->GetState()) || forceJobsLogging;
    switch (state) {
        case EJobState::Completed: {
            LOG_DEBUG("Job completed, storage scheduled");
            YCHECK(node->RecentlyCompletedJobIds().insert(jobId).second);
            OnJobCompleted(job, jobStatus);
            ToProto(response->add_jobs_to_store(), jobId);
            break;
        }

        case EJobState::Failed: {
            auto error = FromProto<TError>(jobStatus->result().error());
            LOG_DEBUG(error, "Job failed, removal scheduled");
            OnJobFailed(job, jobStatus);
            ToProto(response->add_jobs_to_remove(), jobId);
            break;
        }

        case EJobState::Aborted: {
            auto error = FromProto<TError>(jobStatus->result().error());
            LOG_DEBUG(error, "Job aborted, removal scheduled");
            if (job->GetPreempted() &&
                (error.FindMatching(NExecAgent::EErrorCode::AbortByScheduler) ||
                error.FindMatching(NJobProxy::EErrorCode::JobNotPrepared)))
            {
                auto error = TError("Job preempted")
                    << TErrorAttribute("abort_reason", EAbortReason::Preemption)
                    << TErrorAttribute("preemption_reason", job->GetPreemptionReason());
                auto status = JobStatusFromError(error);
                OnJobAborted(job, &status);
            } else {
                OnJobAborted(job, jobStatus);
            }
            ToProto(response->add_jobs_to_remove(), jobId);
            break;
        }

        case EJobState::Running:
        case EJobState::Waiting:
            if (job->GetState() == EJobState::Aborted) {
                LOG_DEBUG("Aborting job");
                ToProto(response->add_jobs_to_abort(), jobId);
            } else {
                SetJobState(job, state);
                switch (state) {
                    case EJobState::Running:
                        LOG_DEBUG_IF(shouldLogJob, "Job is running", state);
                        OnJobRunning(job, jobStatus);
                        if (job->GetInterruptDeadline() != 0 && GetCpuInstant() > job->GetInterruptDeadline()) {
                            LOG_DEBUG("Interrupted job deadline reached, aborting (InterruptDeadline: %v)",
                                CpuInstantToInstant(job->GetInterruptDeadline()));
                            ToProto(response->add_jobs_to_abort(), jobId);
                        } else if (job->GetFailRequested()) {
                            LOG_DEBUG("Job fail requested");
                            ToProto(response->add_jobs_to_fail(), jobId);
                        } else if (job->GetInterruptReason() != EInterruptReason::None) {
                            ToProto(response->add_jobs_to_interrupt(), jobId);
                        }
                        break;

                    case EJobState::Waiting:
                        LOG_DEBUG_IF(shouldLogJob, "Job is waiting", state);
                        break;

                    default:
                        Y_UNREACHABLE();
                }
            }
            break;

        case EJobState::Aborting:
            LOG_DEBUG("Job is aborting");
            break;

        default:
            Y_UNREACHABLE();
    }

    return job;
}

void TNodeShard::SubtractNodeResources(const TExecNodePtr& node)
{
    TWriterGuard guard(ResourcesLock_);

    TotalResourceLimits_ -= node->GetResourceLimits();
    TotalResourceUsage_ -= node->GetResourceUsage();
    TotalNodeCount_ -= 1;
    if (node->GetResourceLimits().GetUserSlots() > 0) {
        ExecNodeCount_ -= 1;
    }
}

void TNodeShard::AddNodeResources(const TExecNodePtr& node)
{
    TWriterGuard guard(ResourcesLock_);

    TotalResourceLimits_ += node->GetResourceLimits();
    TotalResourceUsage_ += node->GetResourceUsage();
    TotalNodeCount_ += 1;

    if (node->GetResourceLimits().GetUserSlots() > 0) {
        ExecNodeCount_ += 1;
    } else {
        // Check that we succesfully reset all resource limits to zero for node with zero user slots.
        YCHECK(node->GetResourceLimits() == ZeroJobResources());
    }
}

void TNodeShard::UpdateNodeResources(
    const TExecNodePtr& node,
    const TJobResources& limits,
    const TJobResources& usage,
    const NNodeTrackerClient::NProto::TDiskResources& diskInfo)
{
    auto oldResourceLimits = node->GetResourceLimits();
    auto oldResourceUsage = node->GetResourceUsage();

    // NB: Total limits are updated separately in heartbeat.
    if (limits.GetUserSlots() > 0) {
        if (node->GetResourceLimits().GetUserSlots() == 0 && node->GetMasterState() == ENodeState::Online) {
            ExecNodeCount_ += 1;
        }
        node->SetResourceLimits(limits);
        node->SetResourceUsage(usage);
        node->SetDiskInfo(diskInfo);
    } else {
        if (node->GetResourceLimits().GetUserSlots() > 0 && node->GetMasterState() == ENodeState::Online) {
            ExecNodeCount_ -= 1;
        }
        node->SetResourceLimits(ZeroJobResources());
        node->SetResourceUsage(ZeroJobResources());
    }

    if (node->GetMasterState() == ENodeState::Online) {
        TWriterGuard guard(ResourcesLock_);

        TotalResourceLimits_ -= oldResourceLimits;
        TotalResourceLimits_ += node->GetResourceLimits();

        TotalResourceUsage_ -= oldResourceUsage;
        TotalResourceUsage_ += node->GetResourceUsage();

        // Force update cache if node has come with non-zero usage.
        if (oldResourceLimits.GetUserSlots() == 0 && node->GetResourceUsage().GetUserSlots() > 0) {
            CachedResourceLimitsByTags_->ForceUpdate();
        }
    }
}

void TNodeShard::BeginNodeHeartbeatProcessing(const TExecNodePtr& node)
{
    YCHECK(!node->GetHasOngoingHeartbeat());
    node->SetHasOngoingHeartbeat(true);

    ConcurrentHeartbeatCount_ += 1;
}

void TNodeShard::EndNodeHeartbeatProcessing(const TExecNodePtr& node)
{
    YCHECK(node->GetHasOngoingHeartbeat());
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

    std::vector<TFuture<TSharedRef>> asyncJobSpecs;
    for (const auto& job : schedulingContext->StartedJobs()) {
        auto* operationState = FindOperationState(job->GetOperationId());
        if (!operationState) {
            LOG_DEBUG("Job cannot be started since operation is no longer known (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());
            continue;
        }

        if (operationState->ForbidNewJobs) {
            LOG_DEBUG("Job cannot be started since new jobs are forbidden (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());
            if (!operationState->Terminated) {
                const auto& controller = operationState->Controller;
                controller->OnNonscheduledJobAborted(job->GetId(), EAbortReason::SchedulingOperationSuspended);
                CompletedJobs_.emplace_back(job->GetOperationId(), job->GetId(), job->GetTreeId());
            }
            continue;
        }

        const auto& controller = operationState->Controller;
        auto agent = controller->FindAgent();
        if (!agent) {
            LOG_DEBUG("Cannot start job: agent is no longer known (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());
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
        if (!FindOperationState(job->GetOperationId()) || job->GetHasPendingUnregistration()) {
            LOG_DEBUG("Cannot preempt job: operation is no longer known (JobId: %v, OperationId: %v)",
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
            PreemptJob(job, Null);
            ToProto(response->add_jobs_to_abort(), job->GetId());
        }
    }
}

void TNodeShard::OnJobRunning(const TJobPtr& job, TJobStatus* status)
{
    YCHECK(status);

    if (!status->has_statistics()) {
        return;
    }

    auto now = GetCpuInstant();
    if (now < job->GetRunningJobUpdateDeadline()) {
        return;
    }
    job->SetRunningJobUpdateDeadline(now + DurationToCpuDuration(Config_->RunningJobsUpdatePeriod));

    auto delta = status->resource_usage() - job->ResourceUsage();
    UpdatedJobs_.emplace_back(job->GetOperationId(), job->GetId(), delta, job->GetTreeId());
    job->ResourceUsage() = status->resource_usage();

    auto* operationState = FindOperationState(job->GetOperationId());
    if (operationState) {
        const auto& controller = operationState->Controller;
        controller->OnJobRunning(job, status);
    }
}

void TNodeShard::OnJobCompleted(const TJobPtr& job, TJobStatus* status, bool abandoned)
{
    YCHECK(abandoned == !status);

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
    }

    UnregisterJob(job);
}

void TNodeShard::OnJobFailed(const TJobPtr& job, TJobStatus* status)
{
    YCHECK(status);

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
    }

    UnregisterJob(job);
}

void TNodeShard::OnJobAborted(const TJobPtr& job, TJobStatus* status, bool operationTerminated)
{
    YCHECK(status);

    // Only update the status for the first time.
    // Typically the scheduler decides to abort the job on its own.
    // In this case we should ignore the status returned from the node
    // and avoid notifying the controller twice.
    if (job->GetState() == EJobState::Running ||
        job->GetState() == EJobState::Waiting ||
        job->GetState() == EJobState::None)
    {
        job->SetAbortReason(GetAbortReason(status->result()));
        SetJobState(job, EJobState::Aborted);

        OnJobFinished(job);

        auto* operationState = FindOperationState(job->GetOperationId());
        if (operationState && !operationTerminated) {
            const auto& controller = operationState->Controller;
            controller->OnJobAborted(job, status);
        }
    }

    UnregisterJob(job);
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
                Y_UNREACHABLE();
        }
    }
}

void TNodeShard::SubmitJobsToStrategy()
{
    PROFILE_AGGREGATED_TIMING (StrategyJobProcessingTimeCounter) {
        if (!UpdatedJobs_.empty() || !CompletedJobs_.empty()) {
            std::vector<TJobId> jobsToAbort;

            Host_->GetStrategy()->ProcessUpdatedAndCompletedJobs(
                &UpdatedJobs_,
                &CompletedJobs_,
                &jobsToAbort);

            for (const auto& jobId : jobsToAbort) {
                AbortJob(jobId, TError("Aborting job by strategy request"));
            }
        }
    }
}

void TNodeShard::IncreaseProfilingCounter(const TJobPtr& job, i64 value)
{
    TWriterGuard guard(JobCounterLock_);

    TJobCounter* counter = &JobCounter_;
    if (job->GetState() == EJobState::Aborted) {
        counter = &AbortedJobCounter_[job->GetAbortReason()];
    } else if (job->GetState() == EJobState::Completed) {
        counter = &CompletedJobCounter_[job->GetInterruptReason()];
    }
    (*counter)[job->GetState()][job->GetType()] += value;
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

    YCHECK(operationState.Jobs.emplace(job->GetId(), job).second);
    YCHECK(node->Jobs().insert(job).second);
    YCHECK(node->IdToJob().insert(std::make_pair(job->GetId(), job)).second);
    ++ActiveJobCount_;

    LOG_DEBUG("Job registered (JobId: %v, JobType: %v, Revived: %v, OperationId: %v)",
        job->GetId(),
        job->GetType(),
        job->IsRevived(),
        job->GetOperationId());
}

void TNodeShard::UnregisterJob(const TJobPtr& job)
{
    auto node = job->GetNode();

    if (node->GetHasOngoingJobsScheduling()) {
        job->SetHasPendingUnregistration(true);
    } else {
        DoUnregisterJob(job);
    }
}

void TNodeShard::DoUnregisterJob(const TJobPtr& job)
{
    auto* operationState = FindOperationState(job->GetOperationId());
    auto node = job->GetNode();

    YCHECK(!node->GetHasOngoingJobsScheduling());

    YCHECK(node->Jobs().erase(job) == 1);
    YCHECK(node->IdToJob().erase(job->GetId()) == 1);
    --ActiveJobCount_;
    // We should not attempt to unregister this job again as an unconfirmed job.
    job->SetWaitingForConfirmation(false);

    if (operationState) {
        YCHECK(operationState->Jobs.erase(job->GetId()) == 1);

        CompletedJobs_.emplace_back(job->GetOperationId(), job->GetId(), job->GetTreeId());

        LOG_DEBUG("Job unregistered (JobId: %v, OperationId: %v, State: %v)",
            job->GetId(),
            job->GetOperationId(),
            job->GetState());
    } else {
        LOG_DEBUG("Dangling job unregistered (JobId: %v, OperationId: %v, State: %v)",
            job->GetId(),
            job->GetOperationId(),
            job->GetState());
    }
}

void TNodeShard::PreemptJob(const TJobPtr& job, TNullable<TCpuDuration> interruptTimeout)
{
    LOG_DEBUG("Preempting job (JobId: %v, OperationId: %v, Interruptible: %v, Reason: %v)",
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
    const TNullable<TString>& interruptUser)
{
    LOG_DEBUG("Interrupting job (Reason: %v, InterruptTimeout: %.3g, JobId: %v, OperationId: %v, User: %v)",
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

void TNodeShard::InterruptJob(const TJobId& jobId, EInterruptReason reason)
{
    auto job = FindJob(jobId);
    if (job) {
        DoInterruptJob(job, reason);
    }
}

TExecNodePtr TNodeShard::FindNodeByJob(const TJobId& jobId)
{
    auto nodeId = NodeIdFromJobId(jobId);
    auto it = IdToNode_.find(nodeId);
    return it == IdToNode_.end() ? nullptr : it->second;
}

TJobPtr TNodeShard::FindJob(const TJobId& jobId, const TExecNodePtr& node)
{
    const auto& idToJob = node->IdToJob();
    auto it = idToJob.find(jobId);
    return it == idToJob.end() ? nullptr : it->second;
}

TJobPtr TNodeShard::FindJob(const TJobId& jobId)
{
    auto node = FindNodeByJob(jobId);
    if (!node) {
        return nullptr;
    }
    return FindJob(jobId, node);
}

TJobPtr TNodeShard::GetJobOrThrow(const TJobId& jobId)
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
    auto address = job->GetNode()->NodeDescriptor().GetAddressOrThrow(Bootstrap_->GetLocalNetworks());
    return Host_->CreateJobProberProxy(address);
}

TNodeShard::TOperationState* TNodeShard::FindOperationState(const TOperationId& operationId)
{
    auto it = IdToOpertionState_.find(operationId);
    return it != IdToOpertionState_.end() ? &it->second : nullptr;
}

TNodeShard::TOperationState& TNodeShard::GetOperationState(const TOperationId& operationId)
{
    auto it = IdToOpertionState_.find(operationId);
    YCHECK(it != IdToOpertionState_.end());
    return it->second;
}

void TNodeShard::BuildNodeYson(const TExecNodePtr& node, TFluentMap fluent)
{
    fluent
        .Item(node->GetDefaultAddress()).BeginMap()
            .Do([&] (TFluentMap fluent) {
                BuildExecNodeAttributes(node, fluent);
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
