#include "controller_agent_tracker.h"
#include "scheduler.h"
#include "scheduler_strategy.h"
#include "controller_agent.h"
#include "operation.h"
#include "node_shard.h"
#include "operation_controller.h"
#include "scheduling_context.h"
#include "master_connector.h"
#include "bootstrap.h"

#include <yt/server/lib/scheduler/config.h>
#include <yt/server/lib/scheduler/controller_agent_tracker_service_proxy.h>
#include <yt/server/lib/scheduler/helpers.h>
#include <yt/server/lib/scheduler/job_metrics.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/client/api/transaction.h>

#include <yt/ytlib/controller_agent/controller_agent_service_proxy.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/lease_manager.h>

#include <yt/core/yson/public.h>

#include <util/string/join.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NControllerAgent;

using NJobTrackerClient::TReleaseJobFlags;

using std::placeholders::_1;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsAgentFailureError(const TError& error)
{
    if (IsChannelFailureError(error)) {
        return true;
    }
    auto code = error.GetCode();
    return code == NYT::EErrorCode::Timeout;
}

bool IsAgentDisconnectionError(const TError& error)
{
    return error.FindMatching(NObjectClient::EErrorCode::PrerequisiteCheckFailed).has_value();
}

template <class TResponse, class TRequest>
TFuture<TIntrusivePtr<TResponse>> InvokeAgent(
    TBootstrap* bootstrap,
    TOperationId operationId,
    const TControllerAgentPtr& agent,
    const TIntrusivePtr<TRequest>& request)
{
    YT_LOG_DEBUG("Sending request to agent (AgentId: %v, OperationId: %v, Method: %v)",
        agent->GetId(),
        operationId,
        request->GetMethod());

    ToProto(request->mutable_incarnation_id(), agent->GetIncarnationId());

    return request->Invoke().Apply(BIND([=] (const TErrorOr<TIntrusivePtr<TResponse>>& rspOrError) {
        YT_LOG_DEBUG(rspOrError, "Agent response received (AgentId: %v, OperationId: %v)",
            agent->GetId(),
            operationId);
        if (IsAgentFailureError(rspOrError) || IsAgentDisconnectionError(rspOrError)) {
            const auto& agentTracker = bootstrap->GetControllerAgentTracker();
            agentTracker->HandleAgentFailure(agent, rspOrError);
            // Unregistration should happen before actions that subscribed on this result.
            return rspOrError.ValueOrThrow();
        } else if (rspOrError.GetCode() == NControllerAgent::EErrorCode::AgentCallFailed) {
            YT_VERIFY(rspOrError.InnerErrors().size() == 1);
            THROW_ERROR rspOrError.InnerErrors()[0];
        } else {
            return rspOrError.ValueOrThrow();
        }
    }));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TOperationControllerInitializeResult* result,
    const NControllerAgent::NProto::TInitializeOperationResult& resultProto,
    TOperationId operationId,
    TBootstrap* bootstrap,
    TDuration operationTransactionPingPeriod)
{
    try {
        TForbidContextSwitchGuard contextSwitchGuard;

        FromProto(
            &result->Transactions,
            resultProto.transaction_ids(),
            std::bind(&TBootstrap::GetRemoteMasterClient, bootstrap, _1),
            operationTransactionPingPeriod);
    } catch (const std::exception& ex) {
        YT_LOG_INFO(ex, "Failed to attach operation transactions", operationId);
    }

    result->Attributes = TOperationControllerInitializeAttributes{
        TYsonString(resultProto.mutable_attributes(), EYsonType::MapFragment),
        TYsonString(resultProto.brief_spec(), EYsonType::MapFragment),
        TYsonString(resultProto.full_spec(), EYsonType::Node),
        TYsonString(resultProto.unrecognized_spec(), EYsonType::Node)
    };
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TOperationControllerPrepareResult* result, const NControllerAgent::NProto::TPrepareOperationResult& resultProto)
{
    result->Attributes = resultProto.has_attributes()
        ? TYsonString(resultProto.attributes(), EYsonType::MapFragment)
        : TYsonString();
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TOperationControllerMaterializeResult* result, const NControllerAgent::NProto::TMaterializeOperationResult& resultProto)
{
    result->Suspend = resultProto.suspend();
    result->InitialNeededResources = FromProto<TJobResources>(resultProto.initial_needed_resources());
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TOperationControllerReviveResult* result,
    const NControllerAgent::NProto::TReviveOperationResult& resultProto,
    TOperationId operationId,
    TIncarnationId incarnationId,
    EPreemptionMode preemptionMode)
{
    result->Attributes = TYsonString(resultProto.attributes(), EYsonType::MapFragment);
    result->RevivedFromSnapshot = resultProto.revived_from_snapshot();
    for (const auto& jobProto : resultProto.revived_jobs()) {
        auto job = New<TJob>(
            FromProto<TJobId>(jobProto.job_id()),
            static_cast<EJobType>(jobProto.job_type()),
            operationId,
            incarnationId,
            nullptr /* execNode */,
            FromProto<TInstant>(jobProto.start_time()),
            FromProto<TJobResources>(jobProto.resource_limits()),
            jobProto.interruptible(),
            preemptionMode,
            jobProto.tree_id(),
            jobProto.node_id(),
            jobProto.node_address());
        job->SetState(EJobState::Running);
        result->RevivedJobs.push_back(job);
    }
    result->RevivedBannedTreeIds = FromProto<THashSet<TString>>(resultProto.revived_banned_tree_ids());
    result->NeededResources = FromProto<TJobResources>(resultProto.needed_resources());
}

////////////////////////////////////////////////////////////////////////////////

void FromProto(TOperationControllerCommitResult* /* result */, const NControllerAgent::NProto::TCommitOperationResult& /* resultProto */)
{ }

////////////////////////////////////////////////////////////////////////////////

class TOperationController
    : public IOperationController
{
public:
    TOperationController(
        TBootstrap* bootstrap,
        TSchedulerConfigPtr config,
        const TOperationPtr& operation)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
        , OperationId_(operation->GetId())
        , RuntimeData_(operation->GetRuntimeData())
        , PreemptionMode_(operation->Spec()->PreemptionMode)
        , Logger(NLogging::TLogger(SchedulerLogger)
            .AddTag("OperationId: %v", OperationId_))
    { }


    virtual void AssignAgent(const TControllerAgentPtr& agent) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = Guard(SpinLock_);

        YT_VERIFY(!IncarnationId_);
        IncarnationId_ = agent->GetIncarnationId();
        Agent_ = agent;

        AgentProxy_ = std::make_unique<TControllerAgentServiceProxy>(agent->GetChannel());

        JobEventsOutbox_ = agent->GetJobEventsOutbox();
        OperationEventsOutbox_ = agent->GetOperationEventsOutbox();
        ScheduleJobRequestsOutbox_ = agent->GetScheduleJobRequestsOutbox();
    }

    virtual void RevokeAgent() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = Guard(SpinLock_);

        if (!IncarnationId_) {
            return;
        }

        PendingInitializeResult_ = TPromise<TOperationControllerInitializeResult>();
        PendingPrepareResult_ = TPromise<TOperationControllerPrepareResult>();
        PendingMaterializeResult_ = TPromise<TOperationControllerMaterializeResult>();
        PendingReviveResult_ = TPromise<TOperationControllerReviveResult>();
        PendingCommitResult_ = TPromise<TOperationControllerCommitResult>();

        IncarnationId_ = {};
        Agent_.Reset();
    }

    virtual TControllerAgentPtr FindAgent() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Agent_.Lock();
    }

    virtual TFuture<TOperationControllerInitializeResult> Initialize(const std::optional<TOperationTransactions>& transactions) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(IncarnationId_);

        auto agent = Agent_.Lock();
        if (!agent) {
            throw TFiberCanceledException();
        }

        YT_VERIFY(!PendingInitializeResult_);
        PendingInitializeResult_ = NewPromise<TOperationControllerInitializeResult>();

        auto req = AgentProxy_->InitializeOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        req->SetTimeout(Config_->ControllerAgentTracker->LightRpcTimeout);
        if (transactions) {
            req->set_clean(false);
            ToProto(req->mutable_transaction_ids(), *transactions);
        } else {
            req->set_clean(true);
        }
        InvokeAgent<TControllerAgentServiceProxy::TRspInitializeOperation>(req).Subscribe(
            BIND([
                this,
                this_ = MakeStrong(this)
            ] (const TErrorOr<TControllerAgentServiceProxy::TRspInitializeOperationPtr>& rspOrError) {
                if (!rspOrError.IsOK()) {
                    OnInitializationFinished(static_cast<TError>(rspOrError));
                    return;
                }

                auto rsp = rspOrError.Value();
                if (rsp->has_result()) {
                    TOperationControllerInitializeResult result;
                    FromProto(
                        &result,
                        rsp->result(),
                        OperationId_,
                        Bootstrap_,
                        Config_->OperationTransactionPingPeriod);

                    OnInitializationFinished(result);
                }
            })
            .Via(agent->GetCancelableInvoker()));

        return PendingInitializeResult_;
    }

    virtual TFuture<TOperationControllerPrepareResult> Prepare() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(IncarnationId_);

        auto agent = Agent_.Lock();
        if (!agent) {
            throw TFiberCanceledException();
        }

        YT_VERIFY(!PendingPrepareResult_);
        PendingPrepareResult_ = NewPromise<TOperationControllerPrepareResult>();

        auto req = AgentProxy_->PrepareOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        req->SetTimeout(Config_->ControllerAgentTracker->LightRpcTimeout);
        InvokeAgent<TControllerAgentServiceProxy::TRspPrepareOperation>(req).Subscribe(
            BIND([
                this,
                this_ = MakeStrong(this)
            ] (const TErrorOr<TControllerAgentServiceProxy::TRspPrepareOperationPtr>& rspOrError) {
                if (!rspOrError.IsOK()) {
                    OnPreparationFinished(static_cast<TError>(rspOrError));
                    return;
                }

                auto rsp = rspOrError.Value();
                if (rsp->has_result()) {
                    OnPreparationFinished(FromProto<TOperationControllerPrepareResult>(rsp->result()));
                }
            })
            .Via(agent->GetCancelableInvoker()));

        return PendingPrepareResult_;
    }

    virtual TFuture<TOperationControllerMaterializeResult> Materialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(IncarnationId_);

        auto agent = Agent_.Lock();
        if (!agent) {
            throw TFiberCanceledException();
        }

        YT_VERIFY(!PendingMaterializeResult_);
        PendingMaterializeResult_ = NewPromise<TOperationControllerMaterializeResult>();

        auto req = AgentProxy_->MaterializeOperation();
        req->SetTimeout(Config_->ControllerAgentTracker->LightRpcTimeout);
        ToProto(req->mutable_operation_id(), OperationId_);
        InvokeAgent<TControllerAgentServiceProxy::TRspMaterializeOperation>(req).Subscribe(
            BIND([
                this,
                this_ = MakeStrong(this)
            ] (const TErrorOr<TControllerAgentServiceProxy::TRspMaterializeOperationPtr>& rspOrError) {
                if (!rspOrError.IsOK()) {
                    OnMaterializationFinished(static_cast<TError>(rspOrError));
                    return;
                }

                auto rsp = rspOrError.Value();
                if (rsp->has_result()) {
                    OnMaterializationFinished(FromProto<TOperationControllerMaterializeResult>(rsp->result()));
                }
            })
            .Via(agent->GetCancelableInvoker()));

        return PendingMaterializeResult_;
    }

    virtual TFuture<TOperationControllerReviveResult> Revive() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(IncarnationId_);

        auto agent = Agent_.Lock();
        if (!agent) {
            throw TFiberCanceledException();
        }

        YT_VERIFY(!PendingReviveResult_);
        PendingReviveResult_ = NewPromise<TOperationControllerReviveResult>();

        auto req = AgentProxy_->ReviveOperation();
        req->SetTimeout(Config_->ControllerAgentTracker->LightRpcTimeout);
        ToProto(req->mutable_operation_id(), OperationId_);
        InvokeAgent<TControllerAgentServiceProxy::TRspReviveOperation>(req).Subscribe(
            BIND([
                this,
                this_ = MakeStrong(this),
                operationId = OperationId_,
                incarnationId = agent->GetIncarnationId(),
                preemptionMode = PreemptionMode_
            ] (const TErrorOr<TControllerAgentServiceProxy::TRspReviveOperationPtr>& rspOrError) {
                if (!rspOrError.IsOK()) {
                    OnRevivalFinished(static_cast<TError>(rspOrError));
                    return;
                }

                auto rsp = rspOrError.Value();
                if (rsp->has_result()) {
                    TOperationControllerReviveResult result;
                    FromProto(
                        &result,
                        rsp->result(),
                        operationId,
                        incarnationId,
                        preemptionMode);

                    OnRevivalFinished(result);
                }
            })
            .Via(agent->GetCancelableInvoker()));

        return PendingReviveResult_;
    }

    virtual TFuture<TOperationControllerCommitResult> Commit() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(IncarnationId_);

        auto agent = Agent_.Lock();
        if (!agent) {
            throw TFiberCanceledException();
        }

        YT_VERIFY(!PendingCommitResult_);
        PendingCommitResult_ = NewPromise<TOperationControllerCommitResult>();

        auto req = AgentProxy_->CommitOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        req->SetTimeout(Config_->ControllerAgentTracker->LightRpcTimeout);
        InvokeAgent<TControllerAgentServiceProxy::TRspCommitOperation>(req).Subscribe(
            BIND([
                this,
                this_ = MakeStrong(this)
            ] (const TErrorOr<TControllerAgentServiceProxy::TRspCommitOperationPtr>& rspOrError) {
                if (!rspOrError.IsOK()) {
                    OnCommitFinished(static_cast<TError>(rspOrError));
                    return;
                }

                auto rsp = rspOrError.Value();
                if (rsp->has_result()) {
                    OnCommitFinished(FromProto<TOperationControllerCommitResult>(rsp->result()));
                }
            })
            .Via(agent->GetCancelableInvoker()));

        return PendingCommitResult_;
    }

    virtual TFuture<void> Terminate(EOperationState finalState) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Terminating operation controller");

        if (!IncarnationId_) {
            YT_LOG_INFO("Operation has no agent assigned; terminate request ignored");
            return VoidFuture;
        }

        YT_VERIFY(finalState == EOperationState::Aborted || finalState == EOperationState::Failed);
        EControllerState controllerFinalState = finalState == EOperationState::Aborted
            ? EControllerState::Aborted
            : EControllerState::Failed;

        auto req = AgentProxy_->TerminateOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        req->set_controller_final_state(static_cast<int>(controllerFinalState));
        req->SetTimeout(Config_->ControllerAgentTracker->HeavyRpcTimeout);
        return InvokeAgent<TControllerAgentServiceProxy::TRspTerminateOperation>(req).As<void>();
    }

    virtual TFuture<void> Complete() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(IncarnationId_);

        auto req = AgentProxy_->CompleteOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        req->SetTimeout(Config_->ControllerAgentTracker->HeavyRpcTimeout);
        return InvokeAgent<TControllerAgentServiceProxy::TRspCompleteOperation>(req).As<void>();
    }

    virtual TFuture<TOperationControllerUnregisterResult> Unregister() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_INFO("Unregistering operation controller");

        if (!IncarnationId_) {
            YT_LOG_INFO("Operation has no agent assigned; unregister request ignored");
            return MakeFuture<TOperationControllerUnregisterResult>({});
        }

        auto req = AgentProxy_->UnregisterOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        req->SetTimeout(Config_->ControllerAgentTracker->HeavyRpcTimeout);
        return InvokeAgent<TControllerAgentServiceProxy::TRspUnregisterOperation>(req).Apply(
            BIND([] (const TControllerAgentServiceProxy::TRspUnregisterOperationPtr& rsp){
                return TOperationControllerUnregisterResult{FromProto<TOperationJobMetrics>(rsp->residual_job_metrics())};
            }));
    }

    virtual TFuture<void> UpdateRuntimeParameters(TOperationRuntimeParametersUpdatePtr update) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        if (!IncarnationId_) {
            return VoidFuture;
        }

        auto req = AgentProxy_->UpdateOperationRuntimeParameters();
        ToProto(req->mutable_operation_id(), OperationId_);
        ToProto(req->mutable_parameters(), ConvertToYsonString(update).GetData());
        req->SetTimeout(Config_->ControllerAgentTracker->HeavyRpcTimeout);
        return InvokeAgent<TControllerAgentServiceProxy::TRspUpdateOperationRuntimeParameters>(req).As<void>();
    }


    virtual void OnJobStarted(const TJobPtr& job) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto event = BuildEvent(ESchedulerToAgentJobEventType::Started, job, false, nullptr);
        JobEventsOutbox_->Enqueue(std::move(event));
        YT_LOG_DEBUG("Job start notification enqueued (JobId: %v)",
            job->GetId());
    }

    virtual void OnJobCompleted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool abandoned) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto event = BuildEvent(ESchedulerToAgentJobEventType::Completed, job, true, status);
        event.Abandoned = abandoned;
        event.InterruptReason = job->GetInterruptReason();
        auto result = EnqueueJobEvent(std::move(event));
        YT_LOG_DEBUG("Job completion notification %v (JobId: %v)",
            result ? "enqueued" : "dropped",
            job->GetId());
    }

    virtual void OnJobFailed(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto event = BuildEvent(ESchedulerToAgentJobEventType::Failed, job, true, status);
        auto result = EnqueueJobEvent(std::move(event));
        YT_LOG_DEBUG("Job failure notification %v (JobId: %v)",
            result ? "enqueued" : "dropped",
            job->GetId());
    }

    virtual void OnJobAborted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool byScheduler) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto event = BuildEvent(ESchedulerToAgentJobEventType::Aborted, job, true, status);
        event.AbortReason = job->GetAbortReason();
        event.AbortedByScheduler = byScheduler;
        event.PreemptedFor = job->GetPreemptedFor();

        auto result = EnqueueJobEvent(std::move(event));
        YT_LOG_DEBUG("Job abort notification %v (JobId: %v, ByScheduler: %v)",
            result ? "enqueued" : "dropped",
            job->GetId(),
            byScheduler);
    }

    virtual void OnNonscheduledJobAborted(
        TJobId jobId,
        EAbortReason abortReason) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto status = std::make_unique<NJobTrackerClient::NProto::TJobStatus>();
        ToProto(status->mutable_job_id(), jobId);
        ToProto(status->mutable_operation_id(), OperationId_);
        TSchedulerToAgentJobEvent event{
            ESchedulerToAgentJobEventType::Aborted,
            OperationId_,
            false,
            {},
            {},
            std::move(status),
            abortReason,
            {},
            {}
        };
        auto result = EnqueueJobEvent(std::move(event));
        YT_LOG_DEBUG("Nonscheduled job abort notification %v (JobId: %v)",
            result ? "enqueued" : "dropped",
            jobId);
    }

    virtual void OnJobRunning(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool shouldLogJob) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto event = BuildEvent(ESchedulerToAgentJobEventType::Running, job, true, status);
        auto result = EnqueueJobEvent(std::move(event));
        YT_LOG_DEBUG_IF(shouldLogJob,
            "Job run notification %v (JobId: %v)",
            result ? "enqueued" : "dropped",
            job->GetId());
    }

    virtual void OnInitializationFinished(const TErrorOr<TOperationControllerInitializeResult>& resultOrError) override
    {
        YT_VERIFY(PendingInitializeResult_);

        if (resultOrError.IsOK()) {
            YT_LOG_DEBUG("Successful initialization result received");
        } else {
            YT_LOG_DEBUG("Unsuccessful initialization result received (Error: %v)", resultOrError);
            ProcessControllerAgentError(resultOrError);
        }

        PendingInitializeResult_.TrySet(resultOrError);
    }

    virtual void OnPreparationFinished(const TErrorOr<TOperationControllerPrepareResult>& resultOrError) override
    {
        YT_VERIFY(PendingPrepareResult_);

        if (resultOrError.IsOK()) {
            YT_LOG_DEBUG("Successful preparation result received");
        } else {
            YT_LOG_DEBUG("Unsuccessful preparation result received (Error: %v)", resultOrError);
            ProcessControllerAgentError(resultOrError);
        }

        PendingPrepareResult_.TrySet(resultOrError);
    }

    virtual void OnMaterializationFinished(const TErrorOr<TOperationControllerMaterializeResult>& resultOrError) override
    {
        YT_VERIFY(PendingMaterializeResult_);

        if (resultOrError.IsOK()) {
            auto materializeResult = resultOrError.Value();
            YT_LOG_DEBUG("Successful materialization result received (Suspend: %v, InitialNeededResources: %v)",
                materializeResult.Suspend,
                FormatResources(materializeResult.InitialNeededResources));
        } else {
            YT_LOG_DEBUG("Unsuccessful materialization result received (Error: %v)", resultOrError);
            ProcessControllerAgentError(resultOrError);
        }

        PendingMaterializeResult_.TrySet(resultOrError);
    }

    virtual void OnRevivalFinished(const TErrorOr<TOperationControllerReviveResult>& resultOrError) override
    {
        YT_VERIFY(PendingReviveResult_);

        if (resultOrError.IsOK()) {
            auto result = resultOrError.Value();
            YT_LOG_DEBUG(
                "Successful revival result received "
                "(RevivedFromSnapshot: %v, RevivedJobCount: %v, RevivedBannedTreeIds: %v, NeededResources: %v)",
                result.RevivedFromSnapshot,
                result.RevivedJobs.size(),
                result.RevivedBannedTreeIds,
                FormatResources(result.NeededResources));
        } else {
            YT_LOG_DEBUG("Unsuccessful revival result received (Error: %v)", resultOrError);
            ProcessControllerAgentError(resultOrError);
        }

        PendingReviveResult_.TrySet(resultOrError);
    }

    virtual void OnCommitFinished(const TErrorOr<TOperationControllerCommitResult>& resultOrError) override
    {
        YT_VERIFY(PendingCommitResult_);

        if (resultOrError.IsOK()) {
            YT_LOG_DEBUG("Successful commit result received");
        } else {
            YT_LOG_DEBUG("Unsuccessful commit result received (Error: %v)", resultOrError);
            ProcessControllerAgentError(resultOrError);
        }

        PendingCommitResult_.TrySet(resultOrError);
    }

    virtual TFuture<TControllerScheduleJobResultPtr> ScheduleJob(
        const ISchedulingContextPtr& context,
        const TJobResourcesWithQuota& jobLimits,
        const TString& treeId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto nodeId = context->GetNodeDescriptor().Id;
        auto cellTag = Bootstrap_->GetMasterClient()->GetNativeConnection()->GetPrimaryMasterCellTag();
        auto jobId = GenerateJobId(cellTag, nodeId);

        auto request = std::make_unique<TScheduleJobRequest>();
        request->OperationId = OperationId_;
        request->JobId = jobId;
        request->JobResourceLimits = jobLimits.ToJobResources();
        request->TreeId = treeId;
        request->NodeId = nodeId;
        request->NodeResourceLimits = context->ResourceLimits();
        request->NodeDiskResources = context->DiskResources();

        TIncarnationId incarnationId;
        {
            auto guard = Guard(SpinLock_);
            if (!IncarnationId_) {
                guard.Release();

                YT_LOG_DEBUG("Job schedule request cannot be served since no agent is assigned (JobId: %v)",
                    jobId);

                auto result = New<TControllerScheduleJobResult>();
                result->RecordFail(EScheduleJobFailReason::NoAgentAssigned);

                return MakeFuture(result);
            }

            incarnationId = IncarnationId_;
            ScheduleJobRequestsOutbox_->Enqueue(std::move(request));
        }

        YT_LOG_TRACE("Job schedule request enqueued (JobId: %v)",
            jobId);

        const auto& scheduler = Bootstrap_->GetScheduler();
        auto shardId = scheduler->GetNodeShardId(nodeId);
        const auto& nodeShard = scheduler->GetNodeShards()[shardId];
        return nodeShard->BeginScheduleJob(incarnationId, OperationId_, jobId);
    }

    virtual TJobResources GetNeededResources() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RuntimeData_->GetNeededResources();
    }

    virtual void UpdateMinNeededJobResources() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        EnqueueOperationEvent({
            ESchedulerToAgentOperationEventType::UpdateMinNeededJobResources,
            OperationId_
        });
        YT_LOG_DEBUG("Min needed job resources update request enqueued");
    }

    virtual TJobResourcesWithQuotaList GetMinNeededJobResources() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RuntimeData_->GetMinNeededJobResources();
    }

    virtual int GetPendingJobCount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RuntimeData_->GetPendingJobCount();
    }

    virtual EPreemptionMode GetPreemptionMode() const override
    {
        return PreemptionMode_;
    }

private:
    TBootstrap* const Bootstrap_;
    TSchedulerConfigPtr Config_;
    const TOperationId OperationId_;
    const TOperationRuntimeDataPtr RuntimeData_;
    const EPreemptionMode PreemptionMode_;
    const NLogging::TLogger Logger;

    TSpinLock SpinLock_;

    TIncarnationId IncarnationId_;
    TWeakPtr<TControllerAgent> Agent_;
    std::unique_ptr<TControllerAgentServiceProxy> AgentProxy_;

    TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentJobEvent>> JobEventsOutbox_;
    TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>> OperationEventsOutbox_;
    TIntrusivePtr<TMessageQueueOutbox<TScheduleJobRequestPtr>> ScheduleJobRequestsOutbox_;

    TPromise<TOperationControllerInitializeResult> PendingInitializeResult_;
    TPromise<TOperationControllerPrepareResult> PendingPrepareResult_;
    TPromise<TOperationControllerMaterializeResult> PendingMaterializeResult_;
    TPromise<TOperationControllerReviveResult> PendingReviveResult_;
    TPromise<TOperationControllerCommitResult> PendingCommitResult_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    bool EnqueueJobEvent(TSchedulerToAgentJobEvent&& event)
    {
        auto guard = Guard(SpinLock_);
        if (IncarnationId_) {
            JobEventsOutbox_->Enqueue(std::move(event));
            return true;
        } else {
            // All job notifications must be dropped after agent disconnection.
            // Job revival machinery will reconsider this event further.
            return false;
        }
    }

    void EnqueueOperationEvent(TSchedulerToAgentOperationEvent&& event)
    {
        YT_VERIFY(IncarnationId_);
        OperationEventsOutbox_->Enqueue(std::move(event));
    }

    void EnqueueScheduleJobRequest(TScheduleJobRequestPtr&& event)
    {
        YT_VERIFY(IncarnationId_);
        ScheduleJobRequestsOutbox_->Enqueue(std::move(event));
    }


    TSchedulerToAgentJobEvent BuildEvent(
        ESchedulerToAgentJobEventType eventType,
        const TJobPtr& job,
        bool logAndProfile,
        NJobTrackerClient::NProto::TJobStatus* status)
    {
        auto statusHolder = std::make_unique<NJobTrackerClient::NProto::TJobStatus>();
        if (status) {
            statusHolder->CopyFrom(*status);
            auto truncatedError = FromProto<TError>(status->result().error()).Truncate();
            ToProto(statusHolder->mutable_result()->mutable_error(), truncatedError);
        }
        ToProto(statusHolder->mutable_job_id(), job->GetId());
        ToProto(statusHolder->mutable_operation_id(), job->GetOperationId());
        statusHolder->set_job_type(static_cast<int>(job->GetType()));
        statusHolder->set_state(static_cast<int>(job->GetState()));
        return TSchedulerToAgentJobEvent{
            eventType,
            OperationId_,
            logAndProfile,
            job->GetStartTime(),
            job->GetFinishTime(),
            std::move(statusHolder),
            {},
            {},
            {}
        };
    }

    template <class TResponse, class TRequest>
    TFuture<TIntrusivePtr<TResponse>> InvokeAgent(
        const TIntrusivePtr<TRequest>& request)
    {
        auto agent = Agent_.Lock();
        if (!agent) {
            throw TFiberCanceledException();
        }
        return NScheduler::InvokeAgent<TResponse, TRequest>(
            Bootstrap_,
            OperationId_,
            agent,
            request);
    }

    void ProcessControllerAgentError(const TError& error)
    {
        if (IsAgentDisconnectionError(error)) {
            auto agent = Agent_.Lock();
            if (!agent) {
                throw TFiberCanceledException();
            }
            const auto& agentTracker = Bootstrap_->GetControllerAgentTracker();
            agentTracker->HandleAgentFailure(agent, error);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentTracker::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TSchedulerConfigPtr config,
        TBootstrap* bootstrap)
        : SchedulerConfig_(std::move(config))
        , Config_(SchedulerConfig_->ControllerAgentTracker)
        , Bootstrap_(bootstrap)
    { }

    void Initialize()
    {
        auto* masterConnector = Bootstrap_->GetScheduler()->GetMasterConnector();
        masterConnector->SubscribeMasterConnected(BIND(
            &TImpl::OnMasterConnected,
            Unretained(this)));
        masterConnector->SubscribeMasterDisconnected(BIND(
            &TImpl::OnMasterDisconnected,
            Unretained(this)));
    }

    std::vector<TControllerAgentPtr> GetAgents()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TControllerAgentPtr> result;
        result.reserve(IdToAgent_.size());
        for (const auto& [agentId, agent] : IdToAgent_) {
            result.push_back(agent);
        }
        return result;
    }

    IOperationControllerPtr CreateController(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return New<TOperationController>(Bootstrap_, SchedulerConfig_, operation);
    }

    TControllerAgentPtr PickAgentForOperation(const TOperationPtr& /* operation */)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TControllerAgentPtr> registeredAgents;
        for (const auto& [agentId, agent] : IdToAgent_) {
            if (agent->GetState() != EControllerAgentState::Registered) {
                continue;
            }
            registeredAgents.push_back(agent);
        }

        if (registeredAgents.size() < Config_->MinAgentCount) {
            return nullptr;
        }

        switch (Config_->AgentPickStrategy) {
            case EControllerAgentPickStrategy::Random: {
                std::vector<TControllerAgentPtr> agents;
                for (const auto& agent : registeredAgents) {
                    auto memoryStatistics = agent->GetMemoryStatistics();
                    if (memoryStatistics) {
                        auto minAgentAvailableMemory = std::max(
                            Config_->MinAgentAvailableMemory,
                            static_cast<i64>(Config_->MinAgentAvailableMemoryFraction * memoryStatistics->Limit));
                        if (memoryStatistics->Usage + minAgentAvailableMemory >= memoryStatistics->Limit) {
                            continue;
                        }
                    }
                    agents.push_back(agent);
                }

                return agents.empty() ? nullptr : agents[RandomNumber(agents.size())];
            }
            case EControllerAgentPickStrategy::MemoryUsageBalanced: {
                TControllerAgentPtr pickedAgent;
                double scoreSum = 0.0;
                for (const auto& agent : registeredAgents) {
                    auto memoryStatistics = agent->GetMemoryStatistics();
                    if (!memoryStatistics) {
                        YT_LOG_WARNING("Controller agent skipped since it did not report memory information "
                            "and memory usage balanced pick strategy used (AgentId: %v)",
                            agent->GetId());
                        continue;
                    }

                    auto minAgentAvailableMemory = std::max(
                        Config_->MinAgentAvailableMemory,
                        static_cast<i64>(Config_->MinAgentAvailableMemoryFraction * memoryStatistics->Limit));
                    if (memoryStatistics->Usage + minAgentAvailableMemory >= memoryStatistics->Limit) {
                        continue;
                    }

                    i64 freeMemory = std::max(static_cast<i64>(0), memoryStatistics->Limit - memoryStatistics->Usage);
                    double rawScore = static_cast<double>(freeMemory) / memoryStatistics->Limit;
                    double score = std::pow(rawScore, Config_->MemoryBalancedPickStrategyScorePower);

                    scoreSum += score;
                    if (RandomNumber<float>() <= static_cast<float>(score) / scoreSum) {
                        pickedAgent = agent;
                    }
                }
                return pickedAgent;
            }
            default:
                YT_ABORT();
        }
    }

    void AssignOperationToAgent(
        const TOperationPtr& operation,
        const TControllerAgentPtr& agent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_VERIFY(agent->Operations().insert(operation).second);
        operation->SetAgent(agent.Get());

        YT_LOG_INFO("Operation assigned to agent (AgentId: %v, OperationId: %v)",
            agent->GetId(),
            operation->GetId());
    }

    TFuture<void> RegisterOperationAtAgent(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = operation->GetAgentOrCancelFiber();

        YT_LOG_DEBUG("Registering operation at agent (AgentId: %v, OperationId: %v)",
            agent->GetId(),
            operation->GetId());

        TControllerAgentServiceProxy proxy(agent->GetChannel());
        auto req = proxy.RegisterOperation();
        req->SetTimeout(Config_->HeavyRpcTimeout);

        auto* descriptor = req->mutable_operation_descriptor();
        ToProto(descriptor->mutable_operation_id(), operation->GetId());
        descriptor->set_operation_type(static_cast<int>(operation->GetType()));
        descriptor->set_spec(operation->GetSpecString().GetData());
        descriptor->set_start_time(ToProto<ui64>(operation->GetStartTime()));
        descriptor->set_authenticated_user(operation->GetAuthenticatedUser());
        if (operation->GetSecureVault()) {
            descriptor->set_secure_vault(ConvertToYsonString(operation->GetSecureVault()).GetData());
        }
        descriptor->set_acl(ConvertToYsonString(operation->GetRuntimeParameters()->Acl).GetData());
        ToProto(descriptor->mutable_pool_tree_controller_settings_map(), operation->PoolTreeControllerSettingsMap());
        ToProto(descriptor->mutable_user_transaction_id(), operation->GetUserTransactionId());

        return InvokeAgent<TControllerAgentServiceProxy::TRspRegisterOperation>(
            Bootstrap_,
            operation->GetId(),
            agent,
            req).As<void>();
    }


    void HandleAgentFailure(
        const TControllerAgentPtr& agent,
        const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_WARNING(error, "Agent failed; unregistering (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        Bootstrap_->GetControlInvoker(EControlQueue::AgentTracker)->Invoke(
            BIND(&TImpl::UnregisterAgent, MakeStrong(this), agent));
    }


    void UnregisterOperationFromAgent(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = operation->FindAgent();
        if (!agent) {
            return;
        }

        YT_VERIFY(agent->Operations().erase(operation) == 1);

        YT_LOG_DEBUG("Operation unregistered from agent (AgentId: %v, OperationId: %v)",
            agent->GetId(),
            operation->GetId());
    }

    void UpdateConfig(TSchedulerConfigPtr config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        SchedulerConfig_ = std::move(config);
        Config_ = SchedulerConfig_->ControllerAgentTracker;
    }

    TControllerAgentPtr FindAgent(const TAgentId& id)
    {
        auto it = IdToAgent_.find(id);
        return it == IdToAgent_.end() ? nullptr : it->second;
    }

    TControllerAgentPtr GetAgentOrThrow(const TAgentId& id)
    {
        auto agent = FindAgent(id);
        if (!agent) {
            THROW_ERROR_EXCEPTION("Agent %v is not registered",
                id);
        }
        return agent;
    }


    void ProcessAgentHandshake(const TCtxAgentHandshakePtr& context)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        auto* request = &context->Request();
        auto* response = &context->Response();

        const auto& agentId = request->agent_id();
        auto existingAgent = FindAgent(agentId);
        if (existingAgent) {
            auto state = existingAgent->GetState();
            if (state == EControllerAgentState::Registered || state == EControllerAgentState::WaitingForInitialHeartbeat) {
                YT_LOG_INFO("Kicking out agent due to id conflict (AgentId: %v, ExistingIncarnationId: %v)",
                    agentId,
                    existingAgent->GetIncarnationId());
                UnregisterAgent(existingAgent);
            }

            context->Reply(TError("Agent %Qv is in %Qlv state; please retry",
                agentId,
                state));
            return;
        }

        auto addresses =  FromProto<NNodeTrackerClient::TAddressMap>(request->agent_addresses());
        auto address = NNodeTrackerClient::GetAddressOrThrow(addresses, Bootstrap_->GetLocalNetworks());
        auto channel = Bootstrap_->GetMasterClient()->GetChannelFactory()->CreateChannel(address);

        auto agent = New<TControllerAgent>(
            agentId,
            addresses,
            std::move(channel),
            Bootstrap_->GetControlInvoker(EControlQueue::AgentTracker));
        agent->SetState(EControllerAgentState::Registering);
        RegisterAgent(agent);

        YT_LOG_INFO("Starting agent incarnation transaction (AgentId: %v)",
            agentId);

        NApi::TTransactionStartOptions options;
        options.Timeout = Config_->IncarnationTransactionTimeout;
        if (Config_->IncarnationTransactionPingPeriod) {
            options.PingPeriod = Config_->IncarnationTransactionPingPeriod;
        }
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Controller agent incarnation for %v", agentId));
        options.Attributes = std::move(attributes);
        const auto& lockTransaction = Bootstrap_->GetScheduler()->GetMasterConnector()->GetLockTransaction();
        lockTransaction->StartTransaction(NTransactionClient::ETransactionType::Master, options)
            .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<NApi::ITransactionPtr>& transactionOrError) {
                VERIFY_THREAD_AFFINITY(ControlThread);

                if (!transactionOrError.IsOK()) {
                    Bootstrap_->GetScheduler()->Disconnect(transactionOrError);
                    return;
                }

                if (agent->GetState() != EControllerAgentState::Registering) {
                    return;
                }

                const auto& transaction = transactionOrError.Value();
                agent->SetIncarnationTransaction(transaction);
                agent->SetState(EControllerAgentState::WaitingForInitialHeartbeat);

                agent->SetLease(TLeaseManager::CreateLease(
                    Config_->HeartbeatTimeout,
                    BIND(&TImpl::OnAgentHeartbeatTimeout, MakeWeak(this), MakeWeak(agent))
                        .Via(GetCancelableControlInvoker())));

                transaction->SubscribeAborted(
                    BIND(&TImpl::OnAgentIncarnationTransactionAborted, MakeWeak(this), MakeWeak(agent))
                        .Via(GetCancelableControlInvoker()));

                YT_LOG_INFO("Agent incarnation transaction started (AgentId: %v, IncarnationId: %v)",
                    agentId,
                    agent->GetIncarnationId());

                context->SetResponseInfo("IncarnationId: %v",
                    agent->GetIncarnationId());
                ToProto(response->mutable_incarnation_id(), agent->GetIncarnationId());
                response->set_config(ConvertToYsonString(SchedulerConfig_).GetData());
                context->Reply();
            })
            .Via(GetCancelableControlInvoker()));
    }

    void ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& scheduler = Bootstrap_->GetScheduler();
        scheduler->ValidateConnected();

        auto* request = &context->Request();
        auto* response = &context->Response();

        const auto& agentId = request->agent_id();
        auto incarnationId = FromProto<NControllerAgent::TIncarnationId>(request->incarnation_id());

        context->SetRequestInfo("AgentId: %v, IncarnationId: %v, OperationCount: %v",
            agentId,
            incarnationId,
            request->operations_size());

        auto agent = GetAgentOrThrow(agentId);
        if (agent->GetState() != EControllerAgentState::Registered && agent->GetState() != EControllerAgentState::WaitingForInitialHeartbeat) {
            context->Reply(TError("Agent %Qv is in %Qlv state",
                agentId,
                agent->GetState()));
            return;
        }
        if (incarnationId != agent->GetIncarnationId()) {
            context->Reply(TError("Wrong agent incarnation id: expected %v, got %v",
                agent->GetIncarnationId(),
                incarnationId));
            return;
        }
        if (agent->GetState() == EControllerAgentState::WaitingForInitialHeartbeat) {
            YT_LOG_INFO("Agent registration confirmed by heartbeat");
            agent->SetState(EControllerAgentState::Registered);
        }

        TLeaseManager::RenewLease(agent->GetLease(), Config_->HeartbeatTimeout);

        SwitchTo(agent->GetCancelableInvoker());

        TOperationIdToOperationJobMetrics operationIdToOperationJobMetrics;
        for (const auto& protoOperation : request->operations()) {
            auto operationId = FromProto<TOperationId>(protoOperation.operation_id());
            auto operation = scheduler->FindOperation(operationId);
            auto operationJobMetrics = FromProto<TOperationJobMetrics>(protoOperation.job_metrics());
            if (!operation) {
                // TODO(eshcherbin): This is used for flap diagnostics. Remove when TestPoolMetricsPorto is fixed (YT-12207).
                THashMap<TString, i64> treeIdToOperationTotalTimeDelta;
                for (const auto& [treeId, metrics] : operationJobMetrics) {
                    treeIdToOperationTotalTimeDelta.emplace(treeId, metrics.Values()[EJobMetricName::TotalTime]);
                }

                YT_LOG_DEBUG("Unknown operation is running at agent; unregister requested (AgentId: %v, OperationId: %v, TreeIdToOperationTotalTimeDelta: %v)",
                    agent->GetId(),
                    operationId,
                    treeIdToOperationTotalTimeDelta);
                ToProto(response->add_operation_ids_to_unregister(), operationId);
                continue;
            }

            if (protoOperation.has_alerts()) {
                for (const auto& protoAlert : protoOperation.alerts().alerts()) {
                    auto alertType = EOperationAlertType(protoAlert.type());
                    auto alert = FromProto<TError>(protoAlert.error());
                    if (alert.IsOK()) {
                        operation->ResetAlert(alertType);
                    } else {
                        operation->SetAlert(alertType, alert);
                    }
                }
            }

            YT_VERIFY(operationIdToOperationJobMetrics.emplace(operationId, operationJobMetrics).second);

            if (protoOperation.has_suspicious_jobs()) {
                operation->SetSuspiciousJobs(TYsonString(protoOperation.suspicious_jobs(), EYsonType::MapFragment));
            }

            auto runtimeData = operation->GetRuntimeData();
            runtimeData->SetPendingJobCount(protoOperation.pending_job_count());
            runtimeData->SetNeededResources(FromProto<TJobResources>(protoOperation.needed_resources()));
            runtimeData->SetMinNeededJobResources(FromProto<TJobResourcesWithQuotaList>(protoOperation.min_needed_job_resources()));
        }

        scheduler->GetStrategy()->ApplyJobMetricsDelta(operationIdToOperationJobMetrics);

        const auto& nodeShards = scheduler->GetNodeShards();
        int nodeShardCount = static_cast<int>(nodeShards.size());

        std::vector<std::vector<const NProto::TAgentToSchedulerJobEvent*>> groupedJobEvents(nodeShards.size());
        std::vector<std::vector<const NProto::TScheduleJobResponse*>> groupedScheduleJobResponses(nodeShards.size());

        RunInMessageOffloadThread([&] {
            agent->GetJobEventsInbox()->HandleIncoming(
                request->mutable_agent_to_scheduler_job_events(),
                [&] (auto* protoEvent) {
                    auto jobId = FromProto<TJobId>(protoEvent->job_id());
                    auto shardId = scheduler->GetNodeShardId(NodeIdFromJobId(jobId));
                    groupedJobEvents[shardId].push_back(protoEvent);
                });
            agent->GetJobEventsInbox()->ReportStatus(
                response->mutable_agent_to_scheduler_job_events());

            agent->GetScheduleJobResponsesInbox()->HandleIncoming(
                request->mutable_agent_to_scheduler_schedule_job_responses(),
                [&] (auto* protoEvent) {
                    auto jobId = FromProto<TJobId>(protoEvent->job_id());
                    auto shardId = scheduler->GetNodeShardId(NodeIdFromJobId(jobId));
                    groupedScheduleJobResponses[shardId].push_back(protoEvent);
                });
            agent->GetScheduleJobResponsesInbox()->ReportStatus(
                response->mutable_agent_to_scheduler_schedule_job_responses());

            agent->GetJobEventsOutbox()->HandleStatus(
                request->scheduler_to_agent_job_events());
            agent->GetJobEventsOutbox()->BuildOutcoming(
                response->mutable_scheduler_to_agent_job_events(),
                [] (auto* protoEvent, const auto& event) {
                    ToProto(protoEvent->mutable_operation_id(), event.OperationId);
                    protoEvent->set_event_type(static_cast<int>(event.EventType));
                    protoEvent->set_log_and_profile(event.LogAndProfile);
                    protoEvent->mutable_status()->CopyFrom(*event.Status);
                    protoEvent->set_start_time(ToProto<ui64>(event.StartTime));
                    if (event.FinishTime) {
                        protoEvent->set_finish_time(ToProto<ui64>(*event.FinishTime));
                    }
                    if (event.Abandoned) {
                        protoEvent->set_abandoned(*event.Abandoned);
                    }
                    if (event.AbortReason) {
                        protoEvent->set_abort_reason(static_cast<int>(*event.AbortReason));
                    }
                    if (event.InterruptReason) {
                        protoEvent->set_interrupt_reason(static_cast<int>(*event.InterruptReason));
                    }
                    if (event.AbortedByScheduler) {
                        protoEvent->set_aborted_by_scheduler(*event.AbortedByScheduler);
                    }
                    if (event.PreemptedFor) {
                        ToProto(protoEvent->mutable_preempted_for(), *event.PreemptedFor);
                    }
                });

            agent->GetOperationEventsOutbox()->HandleStatus(
                request->scheduler_to_agent_operation_events());
            agent->GetOperationEventsOutbox()->BuildOutcoming(
                response->mutable_scheduler_to_agent_operation_events(),
                [] (auto* protoEvent, const auto& event) {
                    protoEvent->set_event_type(static_cast<int>(event.EventType));
                    ToProto(protoEvent->mutable_operation_id(), event.OperationId);
                });

            agent->GetScheduleJobRequestsOutbox()->HandleStatus(
                request->scheduler_to_agent_schedule_job_requests());
            agent->GetScheduleJobRequestsOutbox()->BuildOutcoming(
                response->mutable_scheduler_to_agent_schedule_job_requests(),
                [] (auto* protoRequest, const auto& request) {
                    ToProto(protoRequest->mutable_operation_id(), request->OperationId);
                    ToProto(protoRequest->mutable_job_id(), request->JobId);
                    protoRequest->set_tree_id(request->TreeId);
                    ToProto(protoRequest->mutable_job_resource_limits(), request->JobResourceLimits);
                    ToProto(protoRequest->mutable_node_resource_limits(), request->NodeResourceLimits);
                    protoRequest->mutable_node_disk_resources()->CopyFrom(request->NodeDiskResources);
                });
        });

        agent->GetOperationEventsInbox()->HandleIncoming(
            request->mutable_agent_to_scheduler_operation_events(),
            [&] (auto* protoEvent) {
                auto eventType = static_cast<EAgentToSchedulerOperationEventType>(protoEvent->event_type());
                auto operationId = FromProto<TOperationId>(protoEvent->operation_id());
                auto error = FromProto<TError>(protoEvent->error());
                auto operation = scheduler->FindOperation(operationId);
                if (!operation) {
                    return;
                }
                switch (eventType) {
                    case EAgentToSchedulerOperationEventType::Completed:
                        scheduler->OnOperationCompleted(operation);
                        break;
                    case EAgentToSchedulerOperationEventType::Suspended:
                        scheduler->OnOperationSuspended(operation, error);
                        break;
                    case EAgentToSchedulerOperationEventType::Aborted:
                        scheduler->OnOperationAborted(operation, error);
                        break;
                    case EAgentToSchedulerOperationEventType::Failed:
                        scheduler->OnOperationFailed(operation, error);
                        break;
                    case EAgentToSchedulerOperationEventType::BannedInTentativeTree: {
                        auto treeId = protoEvent->tentative_tree_id();
                        auto jobIds = FromProto<std::vector<TJobId>>(protoEvent->tentative_tree_job_ids());
                        scheduler->OnOperationBannedInTentativeTree(operation, treeId, jobIds);
                        break;
                    }
                    case EAgentToSchedulerOperationEventType::InitializationFinished: {
                        TErrorOr<TOperationControllerInitializeResult> resultOrError;
                        if (error.IsOK()) {
                            YT_ASSERT(protoEvent->has_initialize_result());

                            TOperationControllerInitializeResult result;
                            FromProto(
                                &result,
                                protoEvent->initialize_result(),
                                operationId,
                                Bootstrap_,
                                SchedulerConfig_->OperationTransactionPingPeriod);

                            resultOrError = std::move(result);
                        } else {
                            resultOrError = std::move(error);
                        }

                        operation->GetController()->OnInitializationFinished(resultOrError);
                        break;
                    }
                    case EAgentToSchedulerOperationEventType::PreparationFinished: {
                        TErrorOr<TOperationControllerPrepareResult> resultOrError;
                        if (error.IsOK()) {
                            YT_ASSERT(protoEvent->has_prepare_result());
                            resultOrError = FromProto<TOperationControllerPrepareResult>(protoEvent->prepare_result());
                        } else {
                            resultOrError = std::move(error);
                        }

                        operation->GetController()->OnPreparationFinished(resultOrError);
                        break;
                    }
                    case EAgentToSchedulerOperationEventType::MaterializationFinished: {
                        TErrorOr<TOperationControllerMaterializeResult> resultOrError;
                        if (error.IsOK()) {
                            YT_ASSERT(protoEvent->has_materialize_result());
                            resultOrError = FromProto<TOperationControllerMaterializeResult>(protoEvent->materialize_result());
                        } else {
                            resultOrError = std::move(error);
                        }

                        operation->GetController()->OnMaterializationFinished(resultOrError);
                        break;
                    }
                    case EAgentToSchedulerOperationEventType::RevivalFinished: {
                        TErrorOr<TOperationControllerReviveResult> resultOrError;
                        if (error.IsOK()) {
                            YT_ASSERT(protoEvent->has_revive_result());

                            TOperationControllerReviveResult result;
                            FromProto(
                                &result,
                                protoEvent->revive_result(),
                                operationId,
                                incarnationId,
                                operation->GetController()->GetPreemptionMode());

                            resultOrError = std::move(result);
                        } else {
                            resultOrError = std::move(error);
                        }

                        operation->GetController()->OnRevivalFinished(resultOrError);
                        break;
                    }
                    case EAgentToSchedulerOperationEventType::CommitFinished: {
                        TErrorOr<TOperationControllerCommitResult> resultOrError;
                        if (error.IsOK()) {
                            YT_ASSERT(protoEvent->has_commit_result());
                            resultOrError = FromProto<TOperationControllerCommitResult>(protoEvent->commit_result());
                        } else {
                            resultOrError = std::move(error);
                        }

                        operation->GetController()->OnCommitFinished(resultOrError);
                        break;
                    }
                    default:
                        YT_ABORT();
                }
            });
        agent->GetOperationEventsInbox()->ReportStatus(
            response->mutable_agent_to_scheduler_operation_events());

        if (request->has_controller_memory_limit()) {
            agent->SetMemoryStatistics(TControllerAgentMemoryStatistics{request->controller_memory_limit(), request->controller_memory_usage()});
        }

        if (request->exec_nodes_requested()) {
            for (const auto& [nodeId, descriptor] : *scheduler->GetCachedExecNodeDescriptors()) {
                ToProto(response->mutable_exec_nodes()->add_exec_nodes(), descriptor);
            }
        }

        for (int shardId = 0; shardId < nodeShardCount; ++shardId) {
            scheduler->GetCancelableNodeShardInvoker(shardId)->Invoke(
                BIND([
                    context,
                    nodeShard = nodeShards[shardId],
                    protoEvents = std::move(groupedJobEvents[shardId]),
                    protoResponses = std::move(groupedScheduleJobResponses[shardId])
                ] {
                    for (const auto* protoEvent : protoEvents) {
                        auto eventType = static_cast<EAgentToSchedulerJobEventType>(protoEvent->event_type());
                        auto jobId = FromProto<TJobId>(protoEvent->job_id());
                        auto error = FromProto<TError>(protoEvent->error());
                        auto interruptReason = static_cast<EInterruptReason>(protoEvent->interrupt_reason());
                        switch (eventType) {
                            case EAgentToSchedulerJobEventType::Interrupted:
                                nodeShard->InterruptJob(jobId, interruptReason);
                                break;
                            case EAgentToSchedulerJobEventType::Aborted:
                                nodeShard->AbortJob(jobId, error);
                                break;
                            case EAgentToSchedulerJobEventType::Failed:
                                nodeShard->FailJob(jobId);
                                break;
                            case EAgentToSchedulerJobEventType::Released:
                                nodeShard->ReleaseJob(jobId, FromProto<TReleaseJobFlags>(protoEvent->release_job_flags()));
                                break;
                            default:
                                YT_ABORT();
                        }
                    }

                    for (const auto* protoResponse : protoResponses) {
                        nodeShard->EndScheduleJob(*protoResponse);
                    }
                }));
        }

        response->set_operation_archive_version(Bootstrap_->GetScheduler()->GetOperationArchiveVersion());
        response->set_enable_job_reporter(Bootstrap_->GetScheduler()->IsJobReporterEnabled());

        context->Reply();
    }

private:
    TSchedulerConfigPtr SchedulerConfig_;
    TControllerAgentTrackerConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    const TActionQueuePtr MessageOffloadQueue_ = New<TActionQueue>("MessageOffload");

    THashMap<TAgentId, TControllerAgentPtr> IdToAgent_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    template <class F>
    void RunInMessageOffloadThread(F func)
    {
        Y_UNUSED(WaitFor(BIND(func)
            .AsyncVia(MessageOffloadQueue_->GetInvoker())
            .Run()));
    }


    void RegisterAgent(const TControllerAgentPtr& agent)
    {
        YT_VERIFY(IdToAgent_.emplace(agent->GetId(), agent).second);
    }

    void UnregisterAgent(const TControllerAgentPtr& agent)
    {
        if (agent->GetState() == EControllerAgentState::Unregistering ||
            agent->GetState() == EControllerAgentState::Unregistered)
        {
            return;
        }

        YT_VERIFY(agent->GetState() == EControllerAgentState::Registered || agent->GetState() == EControllerAgentState::WaitingForInitialHeartbeat);

        const auto& scheduler = Bootstrap_->GetScheduler();
        for (const auto& operation : agent->Operations()) {
            scheduler->OnOperationAgentUnregistered(operation);
        }

        TerminateAgent(agent);

        YT_LOG_INFO("Aborting agent incarnation transaction (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        agent->SetState(EControllerAgentState::Unregistering);
        agent->GetIncarnationTransaction()->Abort()
            .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
                VERIFY_THREAD_AFFINITY(ControlThread);

                if (!error.IsOK()) {
                    Bootstrap_->GetScheduler()->Disconnect(error);
                    return;
                }

                if (agent->GetState() != EControllerAgentState::Unregistering) {
                    return;
                }

                YT_LOG_INFO("Agent unregistered (AgentId: %v, IncarnationId: %v)",
                    agent->GetId(),
                    agent->GetIncarnationId());

                agent->SetState(EControllerAgentState::Unregistered);
                YT_VERIFY(IdToAgent_.erase(agent->GetId()) == 1);
            })
            .Via(GetCancelableControlInvoker()));
    }

    void TerminateAgent(const TControllerAgentPtr& agent)
    {
        TLeaseManager::CloseLease(agent->GetLease());
        agent->SetLease(TLease());

        TError error("Agent disconnected");
        agent->GetChannel()->Terminate(error);
        agent->Cancel(error);
    }

    void OnAgentHeartbeatTimeout(const TWeakPtr<TControllerAgent>& weakAgent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = weakAgent.Lock();
        if (!agent) {
            return;
        }

        YT_LOG_WARNING("Agent heartbeat timeout; unregistering (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        UnregisterAgent(agent);
    }

    void OnAgentIncarnationTransactionAborted(const TWeakPtr<TControllerAgent>& weakAgent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = weakAgent.Lock();
        if (!agent) {
            return;
        }

        YT_LOG_WARNING("Agent incarnation transaction aborted; unregistering (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        UnregisterAgent(agent);
    }


    void DoCleanup()
    {
        for (const auto& [agentId, agent] : IdToAgent_) {
            TerminateAgent(agent);
            agent->SetState(EControllerAgentState::Unregistered);
        }
        IdToAgent_.clear();
    }

    void OnMasterConnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoCleanup();
    }

    void OnMasterDisconnected()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoCleanup();
    }

    const IInvokerPtr& GetCancelableControlInvoker()
    {
        return Bootstrap_
            ->GetScheduler()
            ->GetMasterConnector()
            ->GetCancelableControlInvoker(EControlQueue::AgentTracker);
    }
};

////////////////////////////////////////////////////////////////////////////////

TControllerAgentTracker::TControllerAgentTracker(
    TSchedulerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

TControllerAgentTracker::~TControllerAgentTracker() = default;

void TControllerAgentTracker::Initialize()
{
    Impl_->Initialize();
}

std::vector<TControllerAgentPtr> TControllerAgentTracker::GetAgents()
{
    return Impl_->GetAgents();
}

IOperationControllerPtr TControllerAgentTracker::CreateController(const TOperationPtr& operation)
{
    return Impl_->CreateController(operation);
}

TControllerAgentPtr TControllerAgentTracker::PickAgentForOperation(const TOperationPtr& operation)
{
    return Impl_->PickAgentForOperation(operation);
}

void TControllerAgentTracker::AssignOperationToAgent(
    const TOperationPtr& operation,
    const TControllerAgentPtr& agent)
{
    Impl_->AssignOperationToAgent(operation, agent);
}

TFuture<void> TControllerAgentTracker::RegisterOperationAtAgent(const TOperationPtr& operation)
{
    return Impl_->RegisterOperationAtAgent(operation);
}

void TControllerAgentTracker::HandleAgentFailure(
    const TControllerAgentPtr& agent,
    const TError& error)
{
    Impl_->HandleAgentFailure(agent, error);
}

void TControllerAgentTracker::UnregisterOperationFromAgent(const TOperationPtr& operation)
{
    Impl_->UnregisterOperationFromAgent(operation);
}

void TControllerAgentTracker::UpdateConfig(TSchedulerConfigPtr config)
{
    Impl_->UpdateConfig(std::move(config));
}

void TControllerAgentTracker::ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context)
{
    Impl_->ProcessAgentHeartbeat(context);
}

void TControllerAgentTracker::ProcessAgentHandshake(const TCtxAgentHandshakePtr& context)
{
    Impl_->ProcessAgentHandshake(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
