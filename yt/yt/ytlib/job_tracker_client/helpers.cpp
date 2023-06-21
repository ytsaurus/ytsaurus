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
}

void FromProto(NJobTrackerClient::TJobToAbort* jobToAbort, const NProto::TJobToAbort& protoJobToAbort)
{
    FromProto(&jobToAbort->JobId, protoJobToAbort.job_id());
    if (protoJobToAbort.has_abort_reason()) {
        jobToAbort->AbortReason = NYT::FromProto<NScheduler::EAbortReason>(protoJobToAbort.abort_reason());
    }
}

void ToProto(NProto::TJobToRemove* protoJobToRemove, const NJobTrackerClient::TJobToRemove& jobToRemove)
{
    ToProto(protoJobToRemove->mutable_job_id(), jobToRemove.JobId);
}

void FromProto(NJobTrackerClient::TJobToRemove* jobToRemove, const NProto::TJobToRemove& protoJobToRemove)
{
    jobToRemove->JobId = NYT::FromProto<TJobId>(protoJobToRemove.job_id());
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

void AddJobToAbort(NProto::TRspHeartbeat* response, const TJobToAbort& jobToAbort)
{
    ToProto(response->add_jobs_to_abort(), jobToAbort);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient
