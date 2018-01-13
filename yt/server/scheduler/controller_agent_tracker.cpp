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

namespace NYT {
namespace NScheduler {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYson;

using NControllerAgent::TOperationAlertMap;

////////////////////////////////////////////////////////////////////////////////

class TOperationController
    : public IOperationController
{
public:
    TOperationController(
        TControllerAgent* agent,
        TOperation* operation)
        : JobEventsOutbox_(agent->GetJobEventsOutbox())
        , OperationId_(operation->GetId())
    { }

    virtual void OnJobStarted(const TJobPtr& job) override
    {
        auto event = BuildEvent(ESchedulerToAgentJobEventType::Started, job, false, nullptr);
        JobEventsOutbox_->Enqueue(std::move(event));
    }

    virtual void OnJobCompleted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status,
        bool abandoned) override
    {
        auto event = BuildEvent(ESchedulerToAgentJobEventType::Completed, job, true, status);
        event.Abandoned = abandoned;
        event.InterruptReason = job->GetInterruptReason();
        JobEventsOutbox_->Enqueue(std::move(event));
    }

    virtual void OnJobFailed(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) override
    {
        auto event = BuildEvent(ESchedulerToAgentJobEventType::Failed, job, true, status);
        JobEventsOutbox_->Enqueue(std::move(event));
    }

    virtual void OnJobAborted(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) override
    {
        auto event = BuildEvent(ESchedulerToAgentJobEventType::Aborted, job, true, status);
        event.AbortReason = GetAbortReason(status->result());
        JobEventsOutbox_->Enqueue(std::move(event));
    }

    virtual void OnJobAborted(
        const TJobPtr& job,
        EAbortReason abortReason) override
    {
        auto event = BuildEvent(ESchedulerToAgentJobEventType::Aborted, job, false, nullptr);
        event.AbortReason = abortReason;
        JobEventsOutbox_->Enqueue(std::move(event));
    }

    virtual void OnJobRunning(
        const TJobPtr& job,
        NJobTrackerClient::NProto::TJobStatus* status) override
    {
        JobEventsOutbox_->Enqueue(BuildEvent(ESchedulerToAgentJobEventType::Running, job, true, status));
    }

private:
    const TIntrusivePtr<TMessageQueueOutbox<TJobEvent>> JobEventsOutbox_;
    const TOperationId OperationId_;

    TJobEvent BuildEvent(
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
        return TJobEvent{
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

        context->SetRequestInfo("AgentIncarnationId: %v", agentIncarnationId);

        for (const auto& jobMetricsProto : request->job_metrics()) {
            auto jobMetrics = FromProto<TOperationJobMetrics>(jobMetricsProto);
            scheduler->GetStrategy()->ApplyJobMetricsDelta(jobMetrics);
        }

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

        for (const auto& protoOperationAlerts : request->operation_alerts()) {
            auto operationId = FromProto<TOperationId>(protoOperationAlerts.operation_id());
            TOperationAlertMap alerts;
            for (const auto& protoAlert : protoOperationAlerts.alerts()) {
                auto alertType = EOperationAlertType(protoAlert.type());
                auto alert = FromProto<TError>(protoAlert.error());
                YCHECK(alerts.emplace(alertType, std::move(alert)).second);
            }
            DoSetOperationAlerts(operationId, alerts);
        }

        if (request->exec_nodes_requested()) {
            for (const auto& execNode : scheduler->GetCachedExecNodeDescriptors()->Descriptors) {
                ToProto(response->mutable_exec_nodes()->add_exec_nodes(), execNode);
            }
        }

        Agent_->SetSuspiciousJobsYson(TYsonString(request->suspicious_jobs(), EYsonType::MapFragment));

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

        context->Reply();
    }

private:
    const TSchedulerConfigPtr Config_;
    NCellScheduler::TBootstrap* const Bootstrap_;

    // TODO(babenko): multiagent support
    TControllerAgentPtr Agent_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void DoSetOperationAlerts(
        const TOperationId& operationId,
        const TOperationAlertMap& operationAlerts)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& scheduler = Bootstrap_->GetScheduler();
        auto operation = scheduler->FindOperation(operationId);
        if (!operation) {
            return;
        }

        for (const auto& pair : operationAlerts) {
            const auto& alertType = pair.first;
            const auto& alert = pair.second;
            if (operation->Alerts()[alertType] != alert) {
                operation->MutableAlerts()[alertType] = alert;
            }
        }
    }
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
