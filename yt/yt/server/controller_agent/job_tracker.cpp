#include "job_tracker.h"

#include "bootstrap.h"
#include "config.h"
#include "controller_agent.h"
#include "operation.h"
#include "private.h"

#include <yt/yt/server/lib/controller_agent/proto/job_tracker_service.pb.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NControllerAgent {

static const auto& Logger = ControllerAgentLogger;

////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////

TJobTracker::TJobTracker(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , HeartbeatStatisticsBytes_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/statistics_bytes"))
    , HeartbeatDataStatisticsBytes_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/data_statistics_bytes"))
    , HeartbeatJobResultBytes_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/job_result_bytes"))
    , HeartbeatProtoMessageBytes_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/proto_message_bytes"))
    , HeartbeatEnqueuedControllerEvents_(ControllerAgentProfiler.WithHot().GaugeSummary("/node_heartbeat/enqueued_controller_events"))
    , HeartbeatCount_(ControllerAgentProfiler.WithHot().Counter("/node_heartbeat/count"))
{ }

void TJobTracker::ProcessHeartbeat(const TJobTracker::TCtxHeartbeatPtr& context)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto* request = &context->Request();

    auto incarnationId = FromProto<NScheduler::TIncarnationId>(request->controller_agent_incarnation_id());

    ProfileHeartbeatRequest(request);
    THashMap<TOperationId, std::vector<std::unique_ptr<TJobSummary>>> groupedJobSummaries;
    for (auto& job : *request->mutable_jobs()) {
        auto operationId = FromProto<TOperationId>(job.operation_id());
        groupedJobSummaries[operationId].push_back(ParseJobSummary(&job, Logger));
    }

    SwitchTo(Bootstrap_->GetControlInvoker());

    const auto& controllerAgent = Bootstrap_->GetControllerAgent();
    if (incarnationId != controllerAgent->GetIncarnationId()) {
        context->Reply(TError{EErrorCode::IncarnationMismatch, "Controller agent incarnation mismatch"});
        return;
    }
    controllerAgent->ValidateConnected();
    context->Reply();

    for (auto& [operationId, jobSummaries] : groupedJobSummaries) {
        auto operation = controllerAgent->FindOperation(operationId);
        if (!operation) {
            continue;
        }
        auto controller = operation->GetController();
        AccountEnqueuedControllerEvent(+1);
        // Raw pointer is OK since the job tracker service never dies.
        auto discountGuard = Finally(std::bind(&TJobTracker::AccountEnqueuedControllerEvent, this, -1));
        controller->GetCancelableInvoker(controllerAgent->GetConfig()->JobEventsControllerQueue)->Invoke(
            BIND(
                [&Logger{Logger}, controller, jobSummaries{std::move(jobSummaries)}, discountGuard = std::move(discountGuard)] () mutable {
                    for (auto& jobSummary : jobSummaries) {
                        auto jobState = jobSummary->State;
                        auto jobId = jobSummary->Id;

                        try {
                            controller->OnJobInfoReceivedFromNode(std::move(jobSummary));
                        } catch (const std::exception& ex) {
                            YT_LOG_WARNING(
                                ex,
                                "Failed to process job info from node (JobId: %v, JobState: %v)",
                                jobId,
                                jobState);
                        }
                    }
                }));
    }
}


void TJobTracker::ProfileHeartbeatRequest(const NProto::TReqHeartbeat* request)
{
    i64 totalJobStatisticsSize = 0;
    i64 totalJobDataStatisticsSize = 0;
    i64 totalJobResultSize = 0;
    for (auto& job : request->jobs()) {
        if (job.has_statistics()) {
            totalJobStatisticsSize += std::size(job.statistics());
        }
        if (job.has_result()) {
            totalJobResultSize += job.result().ByteSizeLong();
        }
        for (const auto& dataStatistics : job.output_data_statistics()) {
            totalJobDataStatisticsSize += dataStatistics.ByteSizeLong();
        }
        totalJobStatisticsSize += job.total_input_data_statistics().ByteSizeLong();
    }

    HeartbeatProtoMessageBytes_.Increment(request->ByteSizeLong());
    HeartbeatStatisticsBytes_.Increment(totalJobStatisticsSize);
    HeartbeatDataStatisticsBytes_.Increment(totalJobDataStatisticsSize);
    HeartbeatJobResultBytes_.Increment(totalJobResultSize);
    HeartbeatCount_.Increment();
}

void TJobTracker::AccountEnqueuedControllerEvent(int delta)
{
    auto newValue = EnqueuedControllerEventCount_.fetch_add(delta) + delta;
    HeartbeatEnqueuedControllerEvents_.Update(newValue);
}

////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
