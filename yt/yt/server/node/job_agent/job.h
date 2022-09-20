#pragma once

#include "public.h"

#include <yt/yt/ytlib/job_tracker_client/proto/job.pb.h>


namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

template <typename TJob>
void FillJobStatus(NJobTrackerClient::NProto::TJobStatus* jobStatus, const TJob& job)
{
    using NYT::ToProto;

    ToProto(jobStatus->mutable_job_id(), job->GetId());
    jobStatus->set_job_type(static_cast<int>(job->GetType()));
    jobStatus->set_state(static_cast<int>(job->GetState()));

    jobStatus->set_status_timestamp(ToProto<ui64>(TInstant::Now()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
