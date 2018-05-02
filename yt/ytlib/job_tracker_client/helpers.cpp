#include "helpers.h"

#include <yt/core/misc/format.h>

namespace NYT {
namespace NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

TString JobTypeAsKey(EJobType jobType)
{
    return Format("%lv", jobType);
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(TJobToRemove* protoJobToRemove, const TJobToRelease& jobToRelease)
{
    ToProto(protoJobToRemove->mutable_job_id(), jobToRelease.JobId);
    protoJobToRemove->set_archive_job_spec(jobToRelease.ArchiveJobSpec);
}

void FromProto(TJobToRelease* jobToRelease, const TJobToRemove& protoJobToRemove)
{
    FromProto(&jobToRelease->JobId, protoJobToRemove.job_id());
    jobToRelease->ArchiveJobSpec = protoJobToRemove.archive_job_spec();
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobTrackerClient
} // namespace NYT
