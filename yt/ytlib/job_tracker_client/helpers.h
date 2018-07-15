#pragma once

#include "public.h"

#include <yt/ytlib/job_tracker_client/proto/job_tracker_service.pb.h>

#include <yt/ytlib/scheduler/job.h>

namespace NYT {
namespace NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

TString JobTypeAsKey(EJobType jobType);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(
    TJobToRemove* protoJobToRemove,
    const NScheduler::TJobToRelease& jobToRelease);

void FromProto(
    NScheduler::TJobToRelease* jobToRelease,
    const TJobToRemove& protoJobToRemove);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobTrackerClient
} // namespace NYT
