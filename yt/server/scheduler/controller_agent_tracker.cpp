#include "controller_agent_tracker.h"
#include "scheduler.h"
#include "job_metrics.h"
#include "scheduler_strategy.h"
#include "controller_agent.h"
#include "operation.h"
#include "node_shard.h"
#include "operation_controller.h"

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
    explicit TOperationController(TOperation* operation)
        : Controller_(operation->GetController())
    { }

    virtual void OnJobStarted(
        const TJobId& jobId,
        TInstant startTime) override
    {
        Controller_->GetCancelableInvoker()->Invoke(BIND(
            &NControllerAgent::IOperationControllerSchedulerHost::OnJobStarted,
            Controller_,
            jobId,
            startTime));
    }

    virtual void OnJobCompleted(
        const TJobPtr& job,
        const NJobTrackerClient::NProto::TJobStatus& status,
        bool abandoned) override
    {
        Controller_->GetCancelableInvoker()->Invoke(BIND(
            &NControllerAgent::IOperationControllerSchedulerHost::OnJobCompleted,
            Controller_,
            Passed(std::make_unique<TCompletedJobSummary>(job, status, abandoned))));
    }

    virtual void OnJobFailed(
        const TJobPtr& job,
        const NJobTrackerClient::NProto::TJobStatus& status) override
    {
        Controller_->GetCancelableInvoker()->Invoke(BIND(
            &NControllerAgent::IOperationControllerSchedulerHost::OnJobFailed,
            Controller_,
            Passed(std::make_unique<TFailedJobSummary>(job, &status))));
    }

    virtual void OnJobAborted(
        const TJobPtr& job,
        const NJobTrackerClient::NProto::TJobStatus& status) override
    {
        Controller_->GetCancelableInvoker()->Invoke(BIND(
            &NControllerAgent::IOperationControllerSchedulerHost::OnJobAborted,
            Controller_,
            Passed(std::make_unique<TAbortedJobSummary>(job, status))));
    }

    virtual void OnJobAborted(
        const TJobPtr& job,
        EAbortReason abortReason) override
    {
        Controller_->GetCancelableInvoker()->Invoke(BIND(
            &NControllerAgent::IOperationControllerSchedulerHost::OnJobAborted,
            Controller_,
            Passed(std::make_unique<TAbortedJobSummary>(job->GetId(), abortReason))));
    }

    virtual void OnJobRunning(
        const TJobPtr& job,
        const NJobTrackerClient::NProto::TJobStatus& status) override
    {
        Controller_->GetCancelableInvoker()->Invoke(BIND(
            &NControllerAgent::IOperationControllerSchedulerHost::OnJobRunning,
            Controller_,
            Passed(std::make_unique<TRunningJobSummary>(job, status))));
    }

private:
    const NControllerAgent::IOperationControllerPtr Controller_;
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
        const TControllerAgentPtr& /*agent*/,
        TOperation* operation)
    {
        return New<TOperationController>(operation);
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

        const auto* request = &context->Request();
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
        agent->JobEventsQueue().HandleIncoming(
            request->agent_to_scheduler_job_events_queue(),
            [&] (const auto& protoEvent) {
                auto jobId = FromProto<TJobId>(protoEvent.job_id());
                auto shardId = scheduler->GetNodeShardId(NodeIdFromJobId(jobId));
                groupedJobEvents[shardId].push_back(&protoEvent);
            });

        std::vector<TFuture<void>> asyncResults;
        for (size_t shardId = 0; shardId < nodeShards.size(); ++shardId) {
            const auto& nodeShard = nodeShards[shardId];
            asyncResults.push_back(
                BIND([context, nodeShard, this_ = MakeStrong(this), events = std::move(groupedJobEvents[shardId])] {
                    for (const auto* event : events) {
                        auto eventType = static_cast<EAgentToSchedulerJobEventType>(event->event_type());
                        auto jobId = FromProto<TJobId>(event->job_id());
                        auto error = FromProto<TError>(event->error());
                        auto interruptReason = static_cast<EInterruptReason>(event->interrupt_reason());
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

        agent->OperationEventsQueue().HandleIncoming(
            request->agent_to_scheduler_operation_events_queue(),
            [&] (const auto& protoEvent) {
                auto eventType = static_cast<EAgentToSchedulerOperationEventType>(protoEvent.event_type());
                auto operationId = FromProto<TOperationId>(protoEvent.operation_id());
                auto error = FromProto<TError>(protoEvent.error());
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

        agent->OperationEventsQueue().ReportStatus(response->mutable_agent_to_scheduler_operation_events_queue());
        agent->JobEventsQueue().ReportStatus(response->mutable_agent_to_scheduler_job_events_queue());

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
    const TControllerAgentPtr& agent,
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
