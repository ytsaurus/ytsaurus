#include "job.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

void FillJobStatus(NJobTrackerClient::NProto::TJobStatus* jobStatus, IJobPtr job)
{
    auto jobType = EJobType(job->GetSpec().type());
    ToProto(jobStatus->mutable_job_id(), job->GetId());
    ToProto(jobStatus->mutable_operation_id(), job->GetOperationId());
    jobStatus->set_job_type(static_cast<int>(jobType));
    jobStatus->set_state(static_cast<int>(job->GetState()));
    jobStatus->set_phase(static_cast<int>(job->GetPhase()));
    jobStatus->set_progress(job->GetProgress());
    ToProto(jobStatus->mutable_time_statistics(), job->GetTimeStatistics());
    auto stderrSize = job->GetStderrSize();
    if (stderrSize > 0) {
        jobStatus->set_stderr_size(stderrSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent

