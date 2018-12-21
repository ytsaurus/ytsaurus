#include "helpers.h"

#include <yt/core/misc/format.h>

namespace NYT::NJobTrackerClient {

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
    protoJobToRemove->set_archive_stderr(jobToRelease.ArchiveStderr);
    protoJobToRemove->set_archive_fail_context(jobToRelease.ArchiveFailContext);
    protoJobToRemove->set_archive_profile(jobToRelease.ArchiveProfile);
}

void FromProto(TJobToRelease* jobToRelease, const TJobToRemove& protoJobToRemove)
{
    FromProto(&jobToRelease->JobId, protoJobToRemove.job_id());
    jobToRelease->ArchiveJobSpec = protoJobToRemove.archive_job_spec();
    jobToRelease->ArchiveStderr = protoJobToRemove.archive_stderr();
    jobToRelease->ArchiveFailContext = protoJobToRemove.archive_fail_context();
    jobToRelease->ArchiveProfile = protoJobToRemove.archive_profile();
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient
