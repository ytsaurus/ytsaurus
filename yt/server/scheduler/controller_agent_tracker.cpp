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

    virtual void OnJobStarted(const TJobId& jobId, TInstant startTime) override
    {
        Controller_->GetCancelableInvoker()->Invoke(BIND(
            &NControllerAgent::IOperationControllerSchedulerHost::OnJobStarted,
            Controller_,
            jobId,
            startTime));
    }

    virtual void OnJobCompleted(std::unique_ptr<TCompletedJobSummary> jobSummary) override
    {
        Controller_->GetCancelableInvoker()->Invoke(BIND(
            &NControllerAgent::IOperationControllerSchedulerHost::OnJobCompleted,
            Controller_,
            Passed(std::move(jobSummary))));
    }

    virtual void OnJobFailed(std::unique_ptr<TFailedJobSummary> jobSummary) override
    {
        Controller_->GetCancelableInvoker()->Invoke(BIND(
            &NControllerAgent::IOperationControllerSchedulerHost::OnJobFailed,
            Controller_,
            Passed(std::move(jobSummary))));
    }

    virtual void OnJobAborted(std::unique_ptr<TAbortedJobSummary> jobSummary) override
    {
        Controller_->GetCancelableInvoker()->Invoke(BIND(
            &NControllerAgent::IOperationControllerSchedulerHost::OnJobAborted,
            Controller_,
            Passed(std::move(jobSummary))));
    }

    virtual void OnJobRunning(std::unique_ptr<TRunningJobSummary> jobSummary) override
    {
        Controller_->GetCancelableInvoker()->Invoke(BIND(
            &NControllerAgent::IOperationControllerSchedulerHost::OnJobRunning,
            Controller_,
            Passed(std::move(jobSummary))));
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

        // TODO(babenko): multiagent
        auto agent = Agent_;

        const auto& scheduler = Bootstrap_->GetScheduler();
        if (!scheduler->IsConnected()) {
            context->Reply(TError(
                NRpc::EErrorCode::Unavailable,
                "Scheduler is not able to accept agent heartbeats"));
            return;
        }

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

        // TODO(ignat): add controller agent id to distinguish different controller agents in future.
        auto mutationId = context->GetMutationId();
        if (mutationId == agent->GetLastSeenHeartbeatMutationId()) {
            context->Reply();
            return;
        }

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

        for (const auto& protoOperationCompletion : request->completed_operations()) {
            auto operationId = FromProto<TOperationId>(protoOperationCompletion.operation_id());
            scheduler->OnOperationCompleted(operationId);
        }

        for (const auto& protoOperationSuspension : request->suspended_operations()) {
            auto operationId = FromProto<TOperationId>(protoOperationSuspension.operation_id());
            auto error = FromProto<TError>(protoOperationSuspension.error());
            scheduler->OnOperationSuspended(operationId, error);
        }

        for (const auto& protoOperationAbort : request->aborted_operations()) {
            auto operationId = FromProto<TOperationId>(protoOperationAbort.operation_id());
            auto error = FromProto<TError>(protoOperationAbort.error());
            scheduler->OnOperationAborted(operationId, error);
        }

        for (const auto& protoOperationFailure : request->failed_operations()) {
            auto operationId = FromProto<TOperationId>(protoOperationFailure.operation_id());
            auto error = FromProto<TError>(protoOperationFailure.error());
            scheduler->OnOperationFailed(operationId, error);
        }

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
        if (error.IsOK()) {
            agent->SetLastSeenHeartbeatMutationId(mutationId);
        } else {
            // Heartbeat must succeed and not throw.
            scheduler->Disconnect();

            // TODO(babenko): move to agent disconnect handler
            agent->SetLastSeenHeartbeatMutationId({});
        }

        context->Reply(error);
    }

private:
    const TSchedulerConfigPtr Config_;
    NCellScheduler::TBootstrap* const Bootstrap_;

    // TODO(babenko): multiagent support
    const TControllerAgentPtr Agent_ = New<TControllerAgent>();

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
