#include "job.h"

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NJobAgent {

////////////////////////////////////////////////////////////////////////////////

void FillJobStatus(NJobTrackerClient::NProto::TJobStatus* jobStatus, IJobPtr job)
{
    auto jobType = EJobType(job->GetSpec().type());
    ToProto(jobStatus->mutable_job_id(), job->GetId());
    jobStatus->set_job_type(static_cast<int>(jobType));
    jobStatus->set_state(static_cast<int>(job->GetState()));
    jobStatus->set_phase(static_cast<int>(job->GetPhase()));
    jobStatus->set_progress(job->GetProgress());
    auto prepareDuration = job->GetPrepareDuration();
    if (prepareDuration) {
        jobStatus->set_prepare_duration(ToProto<i64>(*prepareDuration));
    }
    auto downloadDuration = job->GetDownloadDuration();
    if (downloadDuration) {
        jobStatus->set_download_duration(ToProto<i64>(*downloadDuration));
    }
    auto execDuration = job->GetExecDuration();
    if (execDuration) {
        jobStatus->set_exec_duration(ToProto<i64>(*execDuration));
    }
    auto stderrSize = job->GetStderrSize();
    if (stderrSize > 0) {
        jobStatus->set_stderr_size(stderrSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobAgent
} // namespace NYT

