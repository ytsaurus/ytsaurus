#include "job_heartbeat_processor.h"

#include <yt/yt/server/node/data_node/private.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

auto& Logger = DataNodeLogger;

using TJobController = NJobAgent::TJobController;
using EJobState = NJobTrackerClient::EJobState;

using namespace NObjectClient;
using namespace NJobTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

void TMasterJobHeartbeatProcessor::ProcessResponse(
    const TJobController::TRspHeartbeatPtr& response)
{
    ProcessHeartbeatCommonResponsePart(response);

    YT_VERIFY(std::ssize(response->Attachments()) == response->jobs_to_start_size());
    int attachmentIndex = 0;
    for (const auto& startInfo : response->jobs_to_start()) {
        auto operationId = FromProto<TOperationId>(startInfo.operation_id());
        auto jobId = FromProto<TJobId>(startInfo.job_id());
        YT_LOG_DEBUG("Job spec is passed via attachments (OperationId: %v, JobId: %v)",
            operationId,
            jobId);

        const auto& attachment = response->Attachments()[attachmentIndex];

        TJobSpec spec;
        DeserializeProtoWithEnvelope(&spec, attachment);

        const auto& resourceLimits = startInfo.resource_limits();

        CreateJob(jobId, operationId, resourceLimits, std::move(spec));

        ++attachmentIndex;
    }
}

void TMasterJobHeartbeatProcessor::PrepareRequest(
    TCellTag cellTag,
    const TJobController::TReqHeartbeatPtr& request)
{
    PrepareHeartbeatCommonRequestPart(request);

    for (const auto& job : JobController_->GetJobs()) {
        auto jobId = job->GetId();

        if (CellTagFromId(jobId) != cellTag || TypeFromId(jobId) != EObjectType::MasterJob) {
            continue;
        }

        auto* jobStatus = request->add_jobs();
        FillJobStatus(jobStatus, job);
        switch (job->GetState()) {
            case EJobState::Running:
                *jobStatus->mutable_resource_usage() = job->GetResourceUsage();
                break;

            case EJobState::Completed:
            case EJobState::Aborted:
            case EJobState::Failed:
                *jobStatus->mutable_result() = job->GetResult();
                if (auto statistics = job->GetStatistics()) {
                    auto statisticsString = statistics.ToString();
                    job->ResetStatisticsLastSendTime();
                    jobStatus->set_statistics(statisticsString);
                }
                break;

            default:
                break;
        }
    }

    request->set_confirmed_job_count(0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
