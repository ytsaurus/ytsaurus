#include "controller_agent_tracker.h"
#include "scheduler.h"
#include "job_metrics.h"
#include "scheduler_strategy.h"
#include "controller_agent.h"
#include "operation.h"
#include "node_shard.h"
#include "operation_controller.h"
#include "scheduling_context.h"
#include "helpers.h"
#include "controller_agent_tracker_service_proxy.h"
#include "master_connector.h"
#include "bootstrap.h"
#include "config.h"

#include <yt/server/controller_agent/operation.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/client/api/transaction.h>

#include <yt/ytlib/controller_agent/controller_agent_service_proxy.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/lease_manager.h>

#include <yt/core/yson/public.h>

#include <util/string/join.h>

namespace NYT {
namespace NScheduler {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NControllerAgent;

using std::placeholders::_1;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TResponse, class TRequest>
TFuture<TIntrusivePtr<TResponse>> InvokeAgent(
    TBootstrap* bootstrap,
    const TOperationId& operationId,
    const TControllerAgentPtr& agent,
    const TIntrusivePtr<TRequest>& request)
{
    LOG_DEBUG("Sending request to agent (AgentId: %v, OperationId: %v, Method: %v)",
        agent->GetId(),
        operationId,
        request->GetMethod());

    ToProto(request->mutable_incarnation_id(), agent->GetIncarnationId());

    return request->Invoke().Apply(BIND([=] (const TErrorOr<TIntrusivePtr<TResponse>>& rspOrError) {
        LOG_DEBUG(rspOrError, "Agent response received (AgentId: %v, OperationId: %v)",
            agent->GetId(),
            operationId);
        if (rspOrError.GetCode() == NControllerAgent::EErrorCode::AgentCallFailed) {
            YCHECK(rspOrError.InnerErrors().size() == 1);
            THROW_ERROR rspOrError.InnerErrors()[0];
        } else if (IsChannelFailureError(rspOrError)) {
            const auto& agentTracker = bootstrap->GetControllerAgentTracker();
            agentTracker->HandleAgentFailure(agent, rspOrError);
        }
        return rspOrError.ValueOrThrow();
    }));
}

} // namespace

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
        , OperationId_(operation->GetId())
        , RuntimeData_(operation->GetRuntimeData())
    { }


    virtual void AssignAgent(const TControllerAgentPtr& agent) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = Guard(SpinLock_);

        YCHECK(!IncarnationId_);
        IncarnationId_ = agent->GetIncarnationId();
        Agent_ = agent;

        AgentProxy_ = std::make_unique<TControllerAgentServiceProxy>(agent->GetChannel());

        JobEventsOutbox_ = agent->GetJobEventsOutbox();
        OperationEventsOutbox_ = agent->GetOperationEventsOutbox();
        ScheduleJobRequestsOutbox_ = agent->GetScheduleJobRequestsOutbox();

        if (!PostponedJobEvents_.empty()) {
            LOG_DEBUG("Postponed job events enqueued (OperationId: %v, EventCount: %v)",
                OperationId_,
                PostponedJobEvents_.size());
            JobEventsOutbox_->Enqueue(std::move(PostponedJobEvents_));
            PostponedJobEvents_.clear(); // just to be sure
        }
    }

    virtual void RevokeAgent() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = Guard(SpinLock_);

        if (!IncarnationId_) {
            return;
        }

        IncarnationId_ = {};
        Agent_.Reset();
        YCHECK(PostponedJobEvents_.empty());
    }

    virtual TControllerAgentPtr FindAgent() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Agent_.Lock();
    }


    virtual TFuture<TOperationControllerInitializeResult> Initialize(const TNullable<TOperationTransactions>& transactions) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IncarnationId_);

        auto req = AgentProxy_->InitializeOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        if (transactions) {
            req->set_clean(false);
            ToProto(req->mutable_transaction_ids(), *transactions);
        } else {
            req->set_clean(true);
        }
        return InvokeAgent<TControllerAgentServiceProxy::TRspInitializeOperation>(req).Apply(
            BIND([this, this_ = MakeStrong(this)] (const TControllerAgentServiceProxy::TRspInitializeOperationPtr& rsp) {
                TOperationTransactions transactions;
                try {
                    FromProto(&transactions, rsp->transaction_ids(), std::bind(&TBootstrap::GetRemoteMasterClient, Bootstrap_, _1));
                } catch (const std::exception& ex) {
                    LOG_INFO(ex, "Failed to attach operation transactions (OperationId: %v)",
                        OperationId_);
                }
                return TOperationControllerInitializeResult{
                    TOperationControllerInitializeAttributes{
                        TYsonString(rsp->mutable_attributes(), EYsonType::MapFragment),
                        TYsonString(rsp->brief_spec(), EYsonType::MapFragment),
                        TYsonString(rsp->full_spec(), EYsonType::Node),
                        TYsonString(rsp->unrecognized_spec(), EYsonType::Node)
                    },
                    transactions
                };
            }));
    }

    virtual TFuture<TOperationControllerPrepareResult> Prepare() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IncarnationId_);

        auto req = AgentProxy_->PrepareOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspPrepareOperation>(req).Apply(
            BIND([] (const TControllerAgentServiceProxy::TRspPrepareOperationPtr& rsp) {
                return TOperationControllerPrepareResult{
                    rsp->has_attributes() ? TYsonString(rsp->attributes(), EYsonType::MapFragment) : TYsonString()
                };
            }));
    }

    virtual TFuture<TOperationControllerMaterializeResult> Materialize() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IncarnationId_);

        auto req = AgentProxy_->MaterializeOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspMaterializeOperation>(req).Apply(
            BIND([] (const TControllerAgentServiceProxy::TRspMaterializeOperationPtr& rsp) {
                return TOperationControllerMaterializeResult{rsp->suspend()};
            }));
    }

    virtual TFuture<TOperationControllerReviveResult> Revive() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IncarnationId_);

        auto agent = Agent_.Lock();
        if (!agent) {
            throw TFiberCanceledException();
        }

        auto req = AgentProxy_->ReviveOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspReviveOperation>(req).Apply(
            BIND([
                operationId = OperationId_,
                incarnationId = agent->GetIncarnationId()
            ] (const TControllerAgentServiceProxy::TRspReviveOperationPtr& rsp)
            {
                TOperationControllerReviveResult result;
                result.Attributes = TYsonString(rsp->attributes(), EYsonType::MapFragment);
                result.RevivedFromSnapshot = rsp->revived_from_snapshot();
                for (const auto& protoJob : rsp->revived_jobs()) {
                    auto job = New<TJob>(
                        FromProto<TJobId>(protoJob.job_id()),
                        static_cast<EJobType>(protoJob.job_type()),
                        operationId,
                        incarnationId,
                        nullptr /* execNode */,
                        FromProto<TInstant>(protoJob.start_time()),
                        FromProto<TJobResources>(protoJob.resource_limits()),
                        protoJob.interruptible(),
                        protoJob.tree_id(),
                        protoJob.node_id(),
                        protoJob.node_address());
                    job->SetState(EJobState::Running);
                    result.RevivedJobs.push_back(job);
                }
                return result;
            }));
    }

    virtual TFuture<void> Commit() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IncarnationId_);

        auto req = AgentProxy_->CommitOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspCommitOperation>(req).As<void>();
    }

    virtual TFuture<void> Abort() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!IncarnationId_) {
            LOG_WARNING("Operation has no agent assigned; control abort request ignored (OperationId: %v)",
                OperationId_);
            return VoidFuture;
        }

        auto req = AgentProxy_->AbortOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspAbortOperation>(req).As<void>();
    }

    virtual TFuture<void> Complete() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YCHECK(IncarnationId_);

        auto req = AgentProxy_->CompleteOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspCompleteOperation>(req).As<void>();
    }

    virtual TFuture<void> Unregister() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!IncarnationId_) {
            return VoidFuture;
        }

        auto req = AgentProxy_->UnregisterOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspUnregisterOperation>(req).As<void>();
    }


    virtual void OnJobStarted(const TJobPtr& job) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto event = BuildEvent(ESchedulerToAgentJobEventType::Started, job, false, nullptr);
        JobEventsOutbox_->Enqueue(std::move(event));
        LOG_DEBUG("Job start notification enqueued (OperationId: %v, JobId: %v)",
            OperationId_,
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
        LOG_DEBUG("Job completion notification %v (OperationId: %v, JobId: %v)",
            result ? "enqueued" : "buffered",
            OperationId_,
            job->GetId());
    }

    virtual void OnJobFailed(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto event = BuildEvent(ESchedulerToAgentJobEventType::Failed, job, true, status);
        auto result = EnqueueJobEvent(std::move(event));
        LOG_DEBUG("Job failure notification %v (OperationId: %v, JobId: %v)",
            result ? "enqueued" : "buffered",
            OperationId_,
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
        auto result = EnqueueJobEvent(std::move(event));
        LOG_DEBUG("Job abort notification %v (OperationId: %v, JobId: %v, ByScheduler: %v)",
            result ? "enqueued" : "buffered",
            OperationId_,
            job->GetId(),
            byScheduler);
    }

    virtual void OnNonscheduledJobAborted(
        const TJobId& jobId,
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
        LOG_DEBUG("Nonscheduled job abort notification %v (OperationId: %v, JobId: %v)",
            result ? "enqueued" : "buffered",
            OperationId_,
            jobId);
    }

    virtual void OnJobRunning(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool shouldLogJob) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto event = BuildEvent(ESchedulerToAgentJobEventType::Running, job, true, status);
        auto result = EnqueueJobEvent(std::move(event), false);
        LOG_DEBUG_IF(shouldLogJob,
            "Job run notification %v (OperationId: %v, JobId: %v)",
            result ? "enqueued" : "dropped",
            OperationId_,
            job->GetId());
    }


    virtual TFuture<TScheduleJobResultPtr> ScheduleJob(
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
        request->NodeDiskInfo = context->DiskInfo();

        TIncarnationId incarnationId;
        {
            auto guard = Guard(SpinLock_);
            if (!IncarnationId_) {
                guard.Release();

                LOG_DEBUG("Job schedule request cannot be served since no agent is assigned (OperationId: %v, JobId: %v)",
                    OperationId_,
                    jobId);

                auto result = New<TScheduleJobResult>();
                result->RecordFail(EScheduleJobFailReason::NoAgentAssigned);

                return MakeFuture(result);
            }

            incarnationId = IncarnationId_;
            ScheduleJobRequestsOutbox_->Enqueue(std::move(request));
        }

        LOG_TRACE("Job schedule request enqueued (OperationId: %v, JobId: %v)",
            OperationId_,
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
        LOG_DEBUG("Min needed job resources update request enqueued (OperationId: %v)",
            OperationId_);
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

private:
    TBootstrap* const Bootstrap_;
    const TOperationId OperationId_;
    const TOperationRuntimeDataPtr RuntimeData_;

    TSpinLock SpinLock_;

    TIncarnationId IncarnationId_;
    TWeakPtr<TControllerAgent> Agent_;
    std::unique_ptr<TControllerAgentServiceProxy> AgentProxy_;

    std::vector<TSchedulerToAgentJobEvent> PostponedJobEvents_;
    TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentJobEvent>> JobEventsOutbox_;
    TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>> OperationEventsOutbox_;
    TIntrusivePtr<TMessageQueueOutbox<TScheduleJobRequestPtr>> ScheduleJobRequestsOutbox_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    bool EnqueueJobEvent(TSchedulerToAgentJobEvent&& event, bool postponeIfNoAgent = true)
    {
        auto guard = Guard(SpinLock_);
        if (IncarnationId_) {
            JobEventsOutbox_->Enqueue(std::move(event));
            return true;
        } else {
            if (postponeIfNoAgent) {
                PostponedJobEvents_.emplace_back(std::move(event));
            }
            return false;
        }
    }

    void EnqueueOperationEvent(TSchedulerToAgentOperationEvent&& event)
    {
        YCHECK(IncarnationId_);
        OperationEventsOutbox_->Enqueue(std::move(event));
    }

    void EnqueueScheduleJobRequest(TScheduleJobRequestPtr&& event)
    {
        YCHECK(IncarnationId_);
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
        for (const auto& pair : IdToAgent_) {
            result.push_back(pair.second);
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
        for (const auto& pair : IdToAgent_) {
            const auto& agent = pair.second;
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
                    if (memoryStatistics && memoryStatistics->Usage + Config_->MinAgentAvailableMemory >= memoryStatistics->Limit) {
                        continue;
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
                        LOG_WARNING("Controller agent skipped since it did not report memory information "
                            "and memory usage balanced pick strategy used (AgentId: %v)",
                            agent->GetId());
                        continue;
                    }
                    if (memoryStatistics->Usage + Config_->MinAgentAvailableMemory >= memoryStatistics->Limit) {
                        continue;
                    }

                    i64 freeMemory = std::max(static_cast<i64>(0), memoryStatistics->Limit - memoryStatistics->Usage);
                    double score = static_cast<double>(freeMemory) / memoryStatistics->Limit;

                    scoreSum += score;
                    if (RandomNumber<float>() <= static_cast<float>(score) / scoreSum) {
                        pickedAgent = agent;
                    }
                }
                return pickedAgent;
            }
            default:
                Y_UNREACHABLE();
        }
    }

    void AssignOperationToAgent(
        const TOperationPtr& operation,
        const TControllerAgentPtr& agent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(agent->Operations().insert(operation).second);
        operation->SetAgent(agent.Get());

        LOG_INFO("Operation assigned to agent (AgentId: %v, OperationId: %v)",
            agent->GetId(),
            operation->GetId());
    }

    TFuture<void> RegisterOperationAtAgent(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = operation->GetAgentOrCancelFiber();

        LOG_DEBUG("Registering operation at agent (AgentId: %v, OperationId: %v)",
            agent->GetId(),
            operation->GetId());

        TControllerAgentServiceProxy proxy(agent->GetChannel());
        auto req = proxy.RegisterOperation();
        req->SetTimeout(Config_->HeavyRpcTimeout);

        auto* descriptor = req->mutable_operation_descriptor();
        ToProto(descriptor->mutable_operation_id(), operation->GetId());
        descriptor->set_operation_type(static_cast<int>(operation->GetType()));
        descriptor->set_spec(ConvertToYsonString(operation->GetSpec()).GetData());
        descriptor->set_start_time(ToProto<ui64>(operation->GetStartTime()));
        descriptor->set_authenticated_user(operation->GetAuthenticatedUser());
        if (operation->GetSecureVault()) {
            descriptor->set_secure_vault(ConvertToYsonString(operation->GetSecureVault()).GetData());
        }
        ToProto(descriptor->mutable_owners(), operation->GetOwners());
        ToProto(descriptor->mutable_user_transaction_id(), operation->GetUserTransactionId());
        ToProto(descriptor->mutable_pool_tree_scheduling_tag_filters(), operation->PoolTreeToSchedulingTagFilter());

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

        LOG_WARNING(error, "Agent failed; unregistering (AgentId: %v, IncarnationId: %v)",
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

        YCHECK(agent->Operations().erase(operation) == 1);

        LOG_DEBUG("Operation unregistered from agent (AgentId: %v, OperationId: %v)",
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
                LOG_INFO("Kicking out agent due to id conflict (AgentId: %v, ExistingIncarnationId: %v)",
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

        LOG_INFO("Starting agent incarnation transaction (AgentId: %v)",
            agentId);

        NApi::TTransactionStartOptions options;
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

                LOG_INFO("Agent incarnation transaction started (AgentId: %v, IncarnationId: %v)",
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
            LOG_INFO("Agent registration confirmed by heartbeat");
            agent->SetState(EControllerAgentState::Registered);
        }

        TLeaseManager::RenewLease(agent->GetLease(), Config_->HeartbeatTimeout);

        SwitchTo(agent->GetCancelableInvoker());

        TOperationIdToOperationJobMetrics operationIdToOperationJobMetrics;
        for (const auto& protoOperation : request->operations()) {
            auto operationId = FromProto<TOperationId>(protoOperation.operation_id());
            auto operation = scheduler->FindOperation(operationId);
            if (!operation) {
                LOG_DEBUG("Unknown operation is running at agent; unregister requested (AgentId: %v, OperationId: %v)",
                    agent->GetId(),
                    operationId);
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

            auto operationJobMetrics = FromProto<TOperationJobMetrics>(protoOperation.job_metrics());
            YCHECK(operationIdToOperationJobMetrics.emplace(operationId, operationJobMetrics).second);

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
                    protoRequest->mutable_node_disk_info()->CopyFrom(request->NodeDiskInfo);
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
                    default:
                        Y_UNREACHABLE();
                }
            });
        agent->GetOperationEventsInbox()->ReportStatus(
            response->mutable_agent_to_scheduler_operation_events());

        if (request->has_controller_memory_limit()) {
            agent->SetMemoryStatistics(TControllerAgentMemoryStatistics{request->controller_memory_limit(), request->controller_memory_usage()});
        }

        if (request->exec_nodes_requested()) {
            for (const auto& pair : *scheduler->GetCachedExecNodeDescriptors()) {
                ToProto(response->mutable_exec_nodes()->add_exec_nodes(), pair.second);
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
                        auto archiveJobSpec = protoEvent->archive_job_spec();
                        auto archiveStderr = protoEvent->archive_stderr();
                        auto archiveFailContext = protoEvent->archive_fail_context();
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
                                nodeShard->ReleaseJob(jobId, archiveJobSpec, archiveStderr, archiveFailContext);
                                break;
                            default:
                                Y_UNREACHABLE();
                        }
                    }

                    for (const auto* protoResponse : protoResponses) {
                        nodeShard->EndScheduleJob(*protoResponse);
                    }
                }));
        }

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
        YCHECK(IdToAgent_.emplace(agent->GetId(), agent).second);
    }

    void UnregisterAgent(const TControllerAgentPtr& agent)
    {
        if (agent->GetState() == EControllerAgentState::Unregistering ||
            agent->GetState() == EControllerAgentState::Unregistered)
        {
            return;
        }

        YCHECK(agent->GetState() == EControllerAgentState::Registered || agent->GetState() == EControllerAgentState::WaitingForInitialHeartbeat);

        const auto& scheduler = Bootstrap_->GetScheduler();
        for (const auto& operation : agent->Operations()) {
            scheduler->OnOperationAgentUnregistered(operation);
        }

        TerminateAgent(agent);

        LOG_INFO("Aborting agent incarnation transaction (AgentId: %v, IncarnationId: %v)",
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

                LOG_INFO("Agent unregistered (AgentId: %v, IncarnationId: %v)",
                    agent->GetId(),
                    agent->GetIncarnationId());

                agent->SetState(EControllerAgentState::Unregistered);
                YCHECK(IdToAgent_.erase(agent->GetId()) == 1);
            })
            .Via(GetCancelableControlInvoker()));
    }

    void TerminateAgent(const TControllerAgentPtr& agent)
    {
        TLeaseManager::CloseLease(agent->GetLease());
        agent->SetLease(TLease());
        agent->GetChannel()->Terminate(TError("Agent disconnected"));
        agent->Cancel();
    }

    void OnAgentHeartbeatTimeout(const TWeakPtr<TControllerAgent>& weakAgent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto agent = weakAgent.Lock();
        if (!agent) {
            return;
        }

        LOG_WARNING("Agent heartbeat timeout; unregistering (AgentId: %v, IncarnationId: %v)",
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

        LOG_WARNING("Agent incarnation transaction aborted; unregistering (AgentId: %v, IncarnationId: %v)",
            agent->GetId(),
            agent->GetIncarnationId());

        UnregisterAgent(agent);
    }


    void DoCleanup()
    {
        for (const auto& pair : IdToAgent_) {
            const auto& agent = pair.second;
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

} // namespace NScheduler
} // namespace NYT
