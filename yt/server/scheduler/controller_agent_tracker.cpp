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

#include <yt/server/cell_scheduler/bootstrap.h>
#include <yt/server/cell_scheduler/config.h>

// XXX(babenko): remove
#include <yt/server/controller_agent/controller_agent.h>
#include <yt/server/controller_agent/master_connector.h>
#include <yt/server/controller_agent/controller_agent_service_proxy.h>

#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/yson/public.h>

#include <util/string/join.h>

namespace NYT {
namespace NScheduler {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYson;
using namespace NYTree;
using namespace NControllerAgent;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TResponse, class TRequest>
TFuture<TIntrusivePtr<TResponse>> InvokeAgent(
    const TOperationId& operationId,
    const TControllerAgentPtr& agent,
    const TIntrusivePtr<TRequest>& request)
{
    LOG_DEBUG("Sending request to agent (AgentAddress: %v, OperationId: %v)",
        agent->GetDefaultAddress(),
        operationId);

    ToProto(request->mutable_incarnation_id(), agent->GetIncarnationId());

    return request->Invoke().Apply(BIND([=] (const TErrorOr<TIntrusivePtr<TResponse>>& rspOrError) {
        // XXX(babenko): handle agent errors
        LOG_DEBUG(rspOrError, "Agent response received (AgentAddress: %v, OperationId: %v)",
            agent->GetDefaultAddress(),
            operationId);
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
        NCellScheduler::TBootstrap* bootstrap,
        TSchedulerConfigPtr config,
        const TControllerAgentPtr& agent,
        TOperation* operation)
        : Bootstrap_(bootstrap)
        , Agent_(agent)
        , JobEventsOutbox_(agent->GetJobEventsOutbox())
        , OperationEventsOutbox_(agent->GetOperationEventsOutbox())
        , ScheduleJobRequestsOutbox_(agent->GetScheduleJobRequestsOutbox())
        , OperationId_(operation->GetId())
        , RuntimeData_(operation->GetRuntimeData())
        , AgentProxy_(Bootstrap_->GetLocalRpcChannel())
    {
        AgentProxy_.SetDefaultTimeout(config->ControllerAgentHeavyRpcTimeout);
    }

    virtual TFuture<TOperationControllerInitializationResult> Initialize(const TNullable<TOperationRevivalDescriptor>& descriptor) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto req = AgentProxy_.InitializeOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        if (descriptor) {
            req->set_clean(false);
            auto getId = [] (const NApi::ITransactionPtr& transaction) {
                return transaction ? transaction->GetId() : NTransactionClient::TTransactionId();
            };
            ToProto(req->mutable_async_transaction_id(), getId(descriptor->AsyncTransaction));
            ToProto(req->mutable_input_transaction_id(), getId(descriptor->InputTransaction));
            ToProto(req->mutable_output_transaction_id(), getId(descriptor->OutputTransaction));
            ToProto(req->mutable_debug_transaction_id(), getId(descriptor->DebugTransaction));
            ToProto(req->mutable_output_completion_transaction_id(), getId(descriptor->OutputCompletionTransaction));
            ToProto(req->mutable_debug_completion_transaction_id(), getId(descriptor->DebugCompletionTransaction));
        } else {
            req->set_clean(true);
        }
        return InvokeAgent<TControllerAgentServiceProxy::TRspInitializeOperation>(req).Apply(
            BIND([] (const TControllerAgentServiceProxy::TRspInitializeOperationPtr& rsp) {
                return TOperationControllerInitializationResult{
                    TOperationControllerInitializationAttributes{
                        TYsonString(rsp->immutable_attributes(), EYsonType::MapFragment),
                        TYsonString(rsp->mutable_attributes(), EYsonType::MapFragment),
                        TYsonString(rsp->brief_spec(), EYsonType::MapFragment),
                        TYsonString(rsp->unrecognized_spec(), EYsonType::Node)
                    }
                };
            }));
    }

    virtual TFuture<TOperationControllerPrepareResult> Prepare() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto req = AgentProxy_.PrepareOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspPrepareOperation>(req).Apply(
            BIND([] (const TControllerAgentServiceProxy::TRspPrepareOperationPtr& rsp) {
                return TOperationControllerPrepareResult{
                    rsp->has_attributes() ? TYsonString(rsp->attributes(), EYsonType::MapFragment) : TYsonString()
                };
            }));
    }

    virtual TFuture<void> Materialize() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto req = AgentProxy_.MaterializeOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspMaterializeOperation>(req).As<void>();
    }

    virtual TFuture<TOperationControllerReviveResult> Revive() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto req = AgentProxy_.ReviveOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspReviveOperation>(req).Apply(
            BIND([operationId = OperationId_] (const TControllerAgentServiceProxy::TRspReviveOperationPtr& rsp) {
                TOperationControllerReviveResult result;
                result.Attributes = TYsonString(rsp->attributes(), EYsonType::MapFragment);
                result.RevivedFromSnapshot = rsp->revived_from_snapshot();
                for (const auto& protoJob : rsp->revived_jobs()) {
                    auto job = New<TJob>(
                        FromProto<TJobId>(protoJob.job_id()),
                        static_cast<EJobType>(protoJob.job_type()),
                        operationId,
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
        VERIFY_THREAD_AFFINITY_ANY();

        auto req = AgentProxy_.CommitOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspCommitOperation>(req).As<void>();
    }

    virtual TFuture<void> Abort() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto req = AgentProxy_.AbortOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspAbortOperation>(req).As<void>();
    }

    virtual TFuture<void> Complete() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto req = AgentProxy_.CompleteOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspCompleteOperation>(req).As<void>();
    }

    virtual TFuture<void> Dispose() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto req = AgentProxy_.DisposeOperation();
        ToProto(req->mutable_operation_id(), OperationId_);
        return InvokeAgent<TControllerAgentServiceProxy::TRspDisposeOperation>(req).As<void>();
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
        JobEventsOutbox_->Enqueue(std::move(event));
        LOG_DEBUG("Job completion notification enqueued (OperationId: %v, JobId: %v)",
            OperationId_,
            job->GetId());
    }

    virtual void OnJobFailed(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto event = BuildEvent(ESchedulerToAgentJobEventType::Failed, job, true, status);
        JobEventsOutbox_->Enqueue(std::move(event));
        LOG_DEBUG("Job failure notification enqueued (OperationId: %v, JobId: %v)",
            OperationId_,
            job->GetId());
    }

    virtual void OnJobAborted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto event = BuildEvent(ESchedulerToAgentJobEventType::Aborted, job, true, status);
        event.AbortReason = job->GetAbortReason();
        JobEventsOutbox_->Enqueue(std::move(event));
        LOG_DEBUG("Job abort notification enqueued (OperationId: %v, JobId: %v)",
            OperationId_,
            job->GetId());
    }

    virtual void OnNonscheduledJobAborted(
        const TJobId& jobId,
        EAbortReason abortReason) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto status = std::make_unique<NJobTrackerClient::NProto::TJobStatus>();
        ToProto(status->mutable_job_id(), jobId);
        ToProto(status->mutable_operation_id(), OperationId_);
        JobEventsOutbox_->Enqueue(TSchedulerToAgentJobEvent{
            ESchedulerToAgentJobEventType::Aborted,
            OperationId_,
            false,
            {},
            {},
            std::move(status),
            abortReason,
            {},
            {}
        });
        LOG_DEBUG("Nonscheduled job abort notification enqueued (OperationId: %v, JobId: %v)",
            OperationId_,
            jobId);
    }

    virtual void OnJobRunning(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        JobEventsOutbox_->Enqueue(BuildEvent(ESchedulerToAgentJobEventType::Running, job, true, status));
        LOG_DEBUG("Job run notification enqueued (OperationId: %v, JobId: %v)",
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
        request->JobResourceLimits = jobLimits;
        request->TreeId = treeId;
        request->NodeId = nodeId;
        request->NodeResourceLimits = context->ResourceLimits();
        request->NodeDiskInfo = context->DiskInfo();
        ScheduleJobRequestsOutbox_->Enqueue(std::move(request));
        LOG_DEBUG("Job schedule request enqueued (OperationId: %v, JobId: %v)",
            OperationId_,
            jobId);

        const auto& scheduler = Bootstrap_->GetScheduler();
        auto shardId = scheduler->GetNodeShardId(nodeId);
        const auto& nodeShard = scheduler->GetNodeShards()[shardId];
        return nodeShard->BeginScheduleJob(jobId);
    }

    virtual TJobResources GetNeededResources() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RuntimeData_->GetNeededResources();
    }

    virtual void UpdateMinNeededJobResources() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        OperationEventsOutbox_->Enqueue({
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
    NCellScheduler::TBootstrap* const Bootstrap_;
    const TWeakPtr<TControllerAgent> Agent_;
    const TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentJobEvent>> JobEventsOutbox_;
    const TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>> OperationEventsOutbox_;
    const TIntrusivePtr<TMessageQueueOutbox<TScheduleJobRequestPtr>> ScheduleJobRequestsOutbox_;
    const TOperationId OperationId_;
    const TOperationRuntimeDataPtr RuntimeData_;

    TControllerAgentServiceProxy AgentProxy_;


    TSchedulerToAgentJobEvent BuildEvent(
        ESchedulerToAgentJobEventType eventType,
        const TJobPtr& job,
        bool logAndProfile,
        NJobTrackerClient::NProto::TJobStatus* status)
    {
        auto statusHolder = std::make_unique<NJobTrackerClient::NProto::TJobStatus>();
        if (status) {
            statusHolder->CopyFrom(*status);
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
        return NScheduler::InvokeAgent<TResponse, TRequest>(OperationId_, agent, request);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentTracker::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap)
        : Config_(std::move(config))
        , Bootstrap_(bootstrap)
    { }

    void OnAgentConnected()
    {
        Agent_ = New<TControllerAgent>(Bootstrap_->GetControllerAgent()->GetMasterConnector()->GetIncarnationId());
    }

    void OnAgentDisconected()
    {
        Agent_.Reset();
    }

    std::vector<TControllerAgentPtr> GetAgents()
    {
        return Agent_ ? std::vector<TControllerAgentPtr>{Agent_} : std::vector<TControllerAgentPtr>{};
    }

    IOperationControllerPtr CreateController(
        TControllerAgent* agent,
        TOperation* operation)
    {
        return New<TOperationController>(Bootstrap_, Config_, agent, operation);
    }

    TControllerAgentPtr PickAgentForOperation(const TOperationPtr& /*operation*/)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Agent_;
    }

    void AssignOperationToAgent(
        const TOperationPtr& operation,
        const TControllerAgentPtr& agent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(agent->Operations().insert(operation).second);
        operation->SetAgent(agent.Get());

        // TODO(babenko): agent id
        LOG_DEBUG("Operation assigned to agent (OperationId: %v)",
            operation->GetId());
    }

    TFuture<void> RegisterOperationAtAgent(const TOperationPtr& operation)
    {
        // TODO(babenko): agent id
        LOG_DEBUG("Registering operation at agent (OperationId: %v)",
            operation->GetId());

        TControllerAgentServiceProxy proxy(Bootstrap_->GetLocalRpcChannel());
        auto req = proxy.RegisterOperation();
        req->SetTimeout(Config_->ControllerAgentHeavyRpcTimeout);

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
        ToProto(descriptor->mutable_pool_tree_scheduling_tag_filters(), operation->PoolTreeSchedulingTagFilters());

        auto agent = operation->GetAgentOrCancelFiber();
        return InvokeAgent<TControllerAgentServiceProxy::TRspRegisterOperation>(operation->GetId(), agent, req).As<void>();
    }

    void UnregisterOperationFromAgent(const TOperationPtr& operation)
    {
        auto agent = operation->FindAgent();
        if (!agent) {
            return;
        }

        YCHECK(agent->Operations().erase(operation) == 1);

        // TODO(babenko): agent id
        LOG_DEBUG("Operation unregistered from agent (OperationId: %v)",
            operation->GetId());
    }

    void ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& scheduler = Bootstrap_->GetScheduler();
        if (!scheduler->IsConnected()) {
            context->Reply(TError(
                NRpc::EErrorCode::Unavailable,
                "Scheduler is not able to accept agent heartbeats"));
            return;
        }

        // TODO(babenko): multiagent
        auto agent = Agent_;

        auto* request = &context->Request();
        auto* response = &context->Response();

        auto agentIncarnationId = FromProto<NControllerAgent::TIncarnationId>(request->agent_incarnation_id());
        if (agentIncarnationId != agent->GetIncarnationId()) {
            context->Reply(TError(
                NRpc::EErrorCode::Unavailable,
                "Wrong agent incarnation id: expected %v, got %v",
                agent->GetIncarnationId(),
                agentIncarnationId));
            return;
        }

        context->SetRequestInfo("AgentIncarnationId: %v, OperationCount: %v",
            agentIncarnationId,
            request->operations_size());

        TOperationIdToOperationJobMetrics operationIdToOperationJobMetrics;
        std::vector<TString> suspiciousJobsYsons;
        for (const auto& protoOperation : request->operations()) {
            auto operationId = FromProto<TOperationId>(protoOperation.operation_id());

            const auto& scheduler = Bootstrap_->GetScheduler();
            auto operation = scheduler->FindOperation(operationId);
            if (!operation) {
                // TODO(babenko): agentid?
                LOG_DEBUG("Unknown operation is running at agent; unregister requested (OperationId: %v)",
                    operationId);
                ToProto(response->add_operation_ids_to_unregister(), operationId);
                continue;
            }

            if (protoOperation.has_alerts()) {
                for (const auto& protoAlert : protoOperation.alerts().alerts()) {
                    auto alertType = EOperationAlertType(protoAlert.type());
                    auto alert = FromProto<TError>(protoAlert.error());
                    if (operation->Alerts()[alertType] != alert) {
                        operation->MutableAlerts()[alertType] = alert;
                    }
                }
            }

            auto operationJobMetrics = FromProto<TOperationJobMetrics>(protoOperation.job_metrics());
            YCHECK(operationIdToOperationJobMetrics.emplace(operationId, operationJobMetrics).second);

            if (protoOperation.has_suspicious_jobs()) {
                suspiciousJobsYsons.push_back(protoOperation.suspicious_jobs());
            }

            auto runtimeData = operation->GetRuntimeData();
            runtimeData->SetPendingJobCount(protoOperation.pending_job_count());
            runtimeData->SetNeededResources(FromProto<TJobResources>(protoOperation.needed_resources()));
            runtimeData->SetMinNeededJobResources(FromProto<TJobResourcesWithQuotaList>(protoOperation.min_needed_job_resources()));
        }

        scheduler->GetStrategy()->ApplyJobMetricsDelta(operationIdToOperationJobMetrics);

        Agent_->SetSuspiciousJobsYson(TYsonString(JoinSeq("", suspiciousJobsYsons), EYsonType::MapFragment));

        const auto& nodeShards = scheduler->GetNodeShards();

        // We must wait for all these results before replying since these activities
        // rely on RPC request to remain alive.
        std::vector<TFuture<void>> asyncResults;

        std::vector<std::vector<const NProto::TAgentToSchedulerJobEvent*>> groupedJobEvents(nodeShards.size());
        agent->JobEventsInbox().HandleIncoming(
            request->mutable_agent_to_scheduler_job_events(),
            [&] (auto* protoEvent) {
                auto jobId = FromProto<TJobId>(protoEvent->job_id());
                auto shardId = scheduler->GetNodeShardId(NodeIdFromJobId(jobId));
                groupedJobEvents[shardId].push_back(protoEvent);
            });

        for (size_t shardId = 0; shardId < nodeShards.size(); ++shardId) {
            const auto& nodeShard = nodeShards[shardId];
            asyncResults.push_back(
                BIND([context, nodeShard, this_ = MakeStrong(this), protoEvents = std::move(groupedJobEvents[shardId])] {
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
                                nodeShard->ReleaseJob(jobId);
                                break;
                            default:
                                Y_UNREACHABLE();
                        }
                    }
                })
                .AsyncVia(nodeShard->GetInvoker())
                .Run());
        }

        agent->OperationEventsInbox().HandleIncoming(
            request->mutable_agent_to_scheduler_operation_events(),
            [&] (auto* protoEvent) {
                auto eventType = static_cast<EAgentToSchedulerOperationEventType>(protoEvent->event_type());
                auto operationId = FromProto<TOperationId>(protoEvent->operation_id());
                auto error = FromProto<TError>(protoEvent->error());
                switch (eventType) {
                    case EAgentToSchedulerOperationEventType::Completed:
                        scheduler->OnOperationCompleted(operationId);
                        break;
                    case EAgentToSchedulerOperationEventType::Suspended:
                        scheduler->OnOperationSuspended(operationId, error);
                        break;
                    case EAgentToSchedulerOperationEventType::Aborted:
                        scheduler->OnOperationAborted(operationId, error);
                        break;
                    case EAgentToSchedulerOperationEventType::Failed:
                        scheduler->OnOperationFailed(operationId, error);
                        break;
                    default:
                        Y_UNREACHABLE();
                }
            });

        std::vector<std::vector<const NProto::TScheduleJobResponse*>> groupedScheduleJobResponses(nodeShards.size());
        agent->ScheduleJobResponsesInbox().HandleIncoming(
            request->mutable_agent_to_scheduler_schedule_job_responses(),
            [&] (auto* protoEvent) {
                auto jobId = FromProto<TJobId>(protoEvent->job_id());
                auto shardId = scheduler->GetNodeShardId(NodeIdFromJobId(jobId));
                groupedScheduleJobResponses[shardId].push_back(protoEvent);
            });

        for (size_t shardId = 0; shardId < nodeShards.size(); ++shardId) {
            const auto& nodeShard = nodeShards[shardId];
            asyncResults.push_back(
                BIND([context, nodeShard, protoResponses = std::move(groupedScheduleJobResponses[shardId])] {
                    for (const auto* protoResponse : protoResponses) {
                        nodeShard->EndScheduleJob(*protoResponse);
                    }
                })
                .AsyncVia(nodeShard->GetInvoker())
                .Run());
        }

        agent->GetJobEventsOutbox()->HandleStatus(request->scheduler_to_agent_job_events());
        agent->GetOperationEventsOutbox()->HandleStatus(request->scheduler_to_agent_operation_events());
        agent->GetScheduleJobRequestsOutbox()->HandleStatus(request->scheduler_to_agent_schedule_job_requests());

        if (request->exec_nodes_requested()) {
            for (const auto& pair : *scheduler->GetCachedExecNodeDescriptors()) {
                ToProto(response->mutable_exec_nodes()->add_exec_nodes(), pair.second);
            }
        }

        if (request->config_requested()) {
            response->set_config(ConvertToYsonString(scheduler->GetConfig()).GetData());
        }

        auto error = WaitFor(Combine(asyncResults));
        if (!error.IsOK()) {
            scheduler->Disconnect(error);
            context->Reply(error);
            return;
        }

        agent->OperationEventsInbox().ReportStatus(response->mutable_agent_to_scheduler_operation_events());
        agent->JobEventsInbox().ReportStatus(response->mutable_agent_to_scheduler_job_events());
        agent->ScheduleJobResponsesInbox().ReportStatus(response->mutable_agent_to_scheduler_schedule_job_responses());

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
            });

        agent->GetOperationEventsOutbox()->BuildOutcoming(
            response->mutable_scheduler_to_agent_operation_events(),
            [] (auto* protoEvent, const auto& event) {
                protoEvent->set_event_type(static_cast<int>(event.EventType));
                ToProto(protoEvent->mutable_operation_id(), event.OperationId);
            });

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

        context->Reply();
    }

private:
    const TSchedulerConfigPtr Config_;
    NCellScheduler::TBootstrap* const Bootstrap_;

    // TODO(babenko): multiagent support
    TControllerAgentPtr Agent_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

TControllerAgentTracker::TControllerAgentTracker(
    TSchedulerConfigPtr config,
    NCellScheduler::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(std::move(config), bootstrap))
{ }

TControllerAgentTracker::~TControllerAgentTracker() = default;

void TControllerAgentTracker::OnAgentConnected()
{
    Impl_->OnAgentConnected();
}

void TControllerAgentTracker::OnAgentDisconnected()
{
    Impl_->OnAgentDisconected();
}

std::vector<TControllerAgentPtr> TControllerAgentTracker::GetAgents()
{
    return Impl_->GetAgents();
}

IOperationControllerPtr TControllerAgentTracker::CreateController(
    TControllerAgent* agent,
    TOperation* operation)
{
    return Impl_->CreateController(agent, operation);
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

void TControllerAgentTracker::UnregisterOperationFromAgent(const TOperationPtr& operation)
{
    Impl_->UnregisterOperationFromAgent(operation);
}

void TControllerAgentTracker::ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context)
{
    Impl_->ProcessAgentHeartbeat(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
