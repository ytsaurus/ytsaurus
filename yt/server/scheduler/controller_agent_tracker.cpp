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

        std::vector<TFuture<void>> asyncResults;

        ProcessNodeShardRequests(
            context,
            &asyncResults,
            request->jobs_to_interrupt(),
            [] (const auto& nodeShard, const auto& subrequest) {
                auto jobId = FromProto<TJobId>(subrequest.job_id());
                auto reason = EInterruptReason(subrequest.reason());
                nodeShard->InterruptJob(jobId, reason);
            });

        ProcessNodeShardRequests(
            context,
            &asyncResults,
            request->jobs_to_abort(),
            [] (const auto& nodeShard, const auto& subrequest) {
                auto jobId = FromProto<TJobId>(subrequest.job_id());
                auto error = FromProto<TError>(subrequest.error());
                nodeShard->AbortJob(jobId, error);
            });

        ProcessNodeShardRequests(
            context,
            &asyncResults,
            request->jobs_to_fail(),
            [] (const auto& nodeShard, const auto& subrequest) {
                auto jobId = FromProto<TJobId>(subrequest.job_id());
                nodeShard->FailJob(jobId);
            });

        ProcessNodeShardRequests(
            context,
            &asyncResults,
            request->jobs_to_release(),
            [] (const auto& nodeShard, const auto& subrequest) {
                auto jobId = FromProto<TJobId>(subrequest.job_id());
                nodeShard->ReleaseJob(jobId);
            });

        agent->OperationEventsQueue().HandleResponse(
            request->operation_events_queue(),
            [&] (const auto& protoEvent) {
                auto eventType = static_cast<EOperationEventType>(protoEvent.event_type());
                auto operationId = FromProto<TOperationId>(protoEvent.operation_id());
                auto error = FromProto<TError>(protoEvent.error());
                switch (eventType) {
                    case EOperationEventType::Completed:
                        scheduler->OnOperationCompleted(operationId);
                        break;
                    case EOperationEventType::Suspended:
                        scheduler->OnOperationSuspended(operationId, error);
                        break;
                    case EOperationEventType::Aborted:
                        scheduler->OnOperationAborted(operationId, error);
                        break;
                    case EOperationEventType::Failed:
                        scheduler->OnOperationFailed(operationId, error);
                        break;
                    default:
                        Y_UNREACHABLE();
                }
            });

        agent->OperationEventsQueue().BuildRequest(response->mutable_operation_events_queue());

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
            // Heartbeat must succeed and not throw.
            scheduler->Disconnect();
        }

        context->Reply(error);
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

    template <class TContext, class TSubrequest, class F>
    void ProcessNodeShardRequests(
        const TContext& context,
        std::vector<TFuture<void>>* asyncResults,
        const google::protobuf::RepeatedPtrField<TSubrequest>& subrequests,
        F handler)
    {
        const auto& scheduler = Bootstrap_->GetScheduler();
        const auto& nodeShards = scheduler->GetNodeShards();
        std::vector<std::vector<const TSubrequest*>> groupedSubrequests(nodeShards.size());
        for (const auto& subrequest : subrequests) {
            auto jobId = FromProto<TJobId>(subrequest.job_id());
            auto shardId = scheduler->GetNodeShardId(NodeIdFromJobId(jobId));
            groupedSubrequests[shardId].push_back(&subrequest);
        }

        for (size_t shardId = 0; shardId < nodeShards.size(); ++shardId) {
            const auto& nodeShard = nodeShards[shardId];
            asyncResults->push_back(
                BIND([context, handler, nodeShard, this_ = MakeStrong(this), subrequests = std::move(groupedSubrequests[shardId])] {
                    for (const auto* subrequest : subrequests) {
                        handler(nodeShard, *subrequest);
                    }
                })
                .AsyncVia(nodeShard->GetInvoker())
                .Run());
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
