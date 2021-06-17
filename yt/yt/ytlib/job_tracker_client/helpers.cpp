#include "helpers.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NProto::TJobToAbort* protoJobToAbort, const NJobTrackerClient::TJobToAbort& jobToAbort)
{
    ToProto(protoJobToAbort->mutable_job_id(), jobToAbort.JobId);
    if (jobToAbort.AbortReason) {
        protoJobToAbort->set_abort_reason(NYT::ToProto<int>(*jobToAbort.AbortReason));
    }
    if (jobToAbort.PreemptionReason) {
        protoJobToAbort->set_preemption_reason(*jobToAbort.PreemptionReason);
    }
}

void FromProto(NJobTrackerClient::TJobToAbort* jobToAbort, const NProto::TJobToAbort& protoJobToAbort)
{
    FromProto(&jobToAbort->JobId, protoJobToAbort.job_id());
    if (protoJobToAbort.has_abort_reason()) {
        jobToAbort->AbortReason = NYT::FromProto<NScheduler::EAbortReason>(protoJobToAbort.abort_reason());
    }
    if (protoJobToAbort.has_preemption_reason()) {
        jobToAbort->PreemptionReason = protoJobToAbort.preemption_reason();
    }
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

void AddJobToAbort(NProto::TRspHeartbeat* response, const TJobToAbort& jobToAbort)
{
    ToProto(response->add_old_jobs_to_abort(), jobToAbort.JobId);
    ToProto(response->add_jobs_to_abort(), jobToAbort);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient
