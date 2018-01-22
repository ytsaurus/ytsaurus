#include "controller_agent_tracker.h"
#include "scheduler.h"
#include "job_metrics.h"
#include "scheduler_strategy.h"
#include "controller_agent.h"
#include "operation.h"
#include "node_shard.h"
#include "operation_controller.h"
#include "helpers.h"

#include <yt/server/cell_scheduler/bootstrap.h>

#include <yt/server/controller_agent/operation_controller.h>
#include <yt/server/controller_agent/controller_agent.h>
#include <yt/server/controller_agent/master_connector.h>

#include <yt/ytlib/scheduler/proto/controller_agent_tracker_service.pb.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/yson/public.h>

#include <util/string/join.h>

namespace NYT {
namespace NScheduler {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYson;

using NControllerAgent::TOperationAlertMap;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

class TOperationController
    : public IOperationController
{
public:
    TOperationController(
        TControllerAgent* agent,
        TOperation* operation)
        : JobEventsOutbox_(agent->GetJobEventsOutbox())
        , OperationEventsOutbox_(agent->GetOperationEventsOutbox())
        , OperationId_(operation->GetId())
        , RuntimeData_(operation->GetRuntimeData())
    { }

    virtual void OnJobStarted(const TJobPtr& job) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto event = BuildEvent(ESchedulerToAgentJobEventType::Started, job, false, nullptr);
        auto itemId = JobEventsOutbox_->Enqueue(std::move(event));
        LOG_DEBUG("Job start notification enqueued (ItemId: %v, OperationId: %v, JobId: %v)",
            itemId,
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
        auto itemId = JobEventsOutbox_->Enqueue(std::move(event));
        LOG_DEBUG("Job completion notification enqueued (ItemId: %v, OperationId: %v, JobId: %v)",
            itemId,
            OperationId_,
            job->GetId());
    }

    virtual void OnJobFailed(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto event = BuildEvent(ESchedulerToAgentJobEventType::Failed, job, true, status);
        auto itemId = JobEventsOutbox_->Enqueue(std::move(event));
        LOG_DEBUG("Job failure notification enqueued (ItemId: %v, OperationId: %v, JobId: %v)",
            itemId,
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
        auto itemId = JobEventsOutbox_->Enqueue(std::move(event));
        LOG_DEBUG("Job abort notification enqueued (ItemId: %v, OperationId: %v, JobId: %v)",
            itemId,
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
        auto itemId = JobEventsOutbox_->Enqueue(TSchedulerToAgentJobEvent{
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
        LOG_DEBUG("Nonscheduled job abort notification enqueued (ItemId: %v, OperationId: %v, JobId: %v)",
            itemId,
            OperationId_,
            jobId);
    }

    virtual void OnJobRunning(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto itemId = JobEventsOutbox_->Enqueue(BuildEvent(ESchedulerToAgentJobEventType::Running, job, true, status));
        LOG_DEBUG("Job run notification enqueued (ItemId: %v, OperationId: %v, JobId: %v)",
            itemId,
            OperationId_,
            job->GetId());
    }


    virtual TScheduleJobResultPtr ScheduleJob(
        ISchedulingContextPtr context,
        const TJobResourcesWithQuota& jobLimits,
        const TString& treeId) override
    {
        // TODO(babenko)
        return AgentController_->ScheduleJob(
            std::move(context),
            jobLimits,
            treeId);
    }

    virtual IInvokerPtr GetCancelableInvoker() const override
    {
        // TODO(babenko)
        return AgentController_->GetCancelableInvoker();
    }

    virtual TJobResources GetNeededResources() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RuntimeData_->GetNeededResources();
    }

    virtual void UpdateMinNeededJobResources() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto itemId = OperationEventsOutbox_->Enqueue({
            ESchedulerToAgentOperationEventType::UpdateMinNeededJobResources,
            OperationId_
        });
        LOG_DEBUG("Min needed job resources update request enqueued (ItemId: %v, OperationId: %v)",
            itemId,
            OperationId_);
    }

    virtual std::vector<TJobResourcesWithQuota> GetMinNeededJobResources() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RuntimeData_->GetMinNeededJobResources();
    }

    virtual int GetPendingJobCount() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RuntimeData_->GetPendingJobCount();
    }

    virtual NControllerAgent::IOperationControllerPtr GetAgentController() const override
    {
        YCHECK(AgentController_);
        return AgentController_;
    }

    virtual void SetAgentController(const NControllerAgent::IOperationControllerPtr& controller) override
    {
        AgentController_ = controller;
    }
    
private:
    const TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentJobEvent>> JobEventsOutbox_;
    const TIntrusivePtr<TMessageQueueOutbox<TSchedulerToAgentOperationEvent>> OperationEventsOutbox_;
    const TOperationId OperationId_;
    const NControllerAgent::IOperationControllerPtr AgentController_;
    const TOperationRuntimeDataPtr RuntimeData_;

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

    TControllerAgentPtr GetAgent()
    {
        return Agent_;
    }

    IOperationControllerPtr CreateController(
        TControllerAgent* agent,
        TOperation* operation)
    {
        return New<TOperationController>(agent, operation);
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
                LOG_DEBUG("Unknown operation is running at agent (OperationId: %v)",
                    operationId);

                auto itemId = agent->GetOperationEventsOutbox()->Enqueue({
                    ESchedulerToAgentOperationEventType::Abandon,
                    operationId
                });
                LOG_DEBUG("Operation abanbon request enqueued (ItemId: %v, OperationId: %v)",
                    itemId,
                    operationId);
                continue;
            }

            TOperationAlertMap alerts;
            for (const auto& protoAlert : protoOperation.alerts()) {
                auto alertType = EOperationAlertType(protoAlert.type());
                auto alert = FromProto<TError>(protoAlert.error());
                if (operation->Alerts()[alertType] != alert) {
                    operation->MutableAlerts()[alertType] = alert;
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
            runtimeData->SetMinNeededJobResources(FromProto<std::vector<TJobResourcesWithQuota>>(protoOperation.min_needed_job_resources()));
        }

        scheduler->GetStrategy()->ApplyJobMetricsDelta(operationIdToOperationJobMetrics);

        Agent_->SetSuspiciousJobsYson(TYsonString(JoinSeq("", suspiciousJobsYsons), EYsonType::MapFragment));

        const auto& nodeShards = scheduler->GetNodeShards();
        std::vector<std::vector<const NProto::TAgentToSchedulerJobEvent*>> groupedJobEvents(nodeShards.size());
        agent->JobEventsInbox().HandleIncoming(
            request->mutable_agent_to_scheduler_job_events(),
            [&] (auto* protoEvent) {
                auto jobId = FromProto<TJobId>(protoEvent->job_id());
                auto shardId = scheduler->GetNodeShardId(NodeIdFromJobId(jobId));
                groupedJobEvents[shardId].push_back(protoEvent);
            });

        std::vector<TFuture<void>> asyncResults;
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

        agent->GetJobEventsOutbox()->HandleStatus(request->scheduler_to_agent_job_events());

        if (request->exec_nodes_requested()) {
            for (const auto& execNode : scheduler->GetCachedExecNodeDescriptors()->Descriptors) {
                ToProto(response->mutable_exec_nodes()->add_exec_nodes(), execNode);
            }
        }

        auto error = WaitFor(Combine(asyncResults));
        if (!error.IsOK()) {
            scheduler->Disconnect();
            context->Reply(error);
            return;
        }

        agent->OperationEventsInbox().ReportStatus(response->mutable_agent_to_scheduler_operation_events());
        agent->JobEventsInbox().ReportStatus(response->mutable_agent_to_scheduler_job_events());

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

TControllerAgentPtr TControllerAgentTracker::GetAgent()
{
    return Impl_->GetAgent();
}

void TControllerAgentTracker::OnAgentConnected()
{
    Impl_->OnAgentConnected();
}

void TControllerAgentTracker::OnAgentDisconnected()
{
    Impl_->OnAgentDisconected();
}

IOperationControllerPtr TControllerAgentTracker::CreateController(
    TControllerAgent* agent,
    TOperation* operation)
{
    return Impl_->CreateController(agent, operation);
}

void TControllerAgentTracker::ProcessAgentHeartbeat(const TCtxAgentHeartbeatPtr& context)
{
    Impl_->ProcessAgentHeartbeat(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
