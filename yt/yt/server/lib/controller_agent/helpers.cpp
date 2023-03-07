#include "helpers.h"

#include <yt/server/lib/controller_agent/serialize.h>

#include <yt/core/misc/format.h>
#include <yt/core/misc/phoenix.h>

#include <util/generic/cast.h>

namespace NYT::NJobTrackerClient {

////////////////////////////////////////////////////////////////////////////////

TString JobTypeAsKey(EJobType jobType)
{
    return Format("%lv", jobType);
}

////////////////////////////////////////////////////////////////////////////////

bool TReleaseJobFlags::IsNonTrivial() const
{
    return ArchiveJobSpec || ArchiveStderr || ArchiveFailContext || ArchiveProfile;
}

void TReleaseJobFlags::Persist(const TStreamPersistenceContext& context)
{
    using namespace NYT::NControllerAgent;
    using NYT::Persist;

    Persist(context, ArchiveStderr);
    Persist(context, ArchiveJobSpec);
    Persist(context, ArchiveFailContext);
    Persist(context, ArchiveProfile);
}

TString ToString(const TReleaseJobFlags& releaseFlags)
{
    return Format(
        "ArchiveStderr: %v, ArchiveJobSpec: %v, ArchiveFailContext: %v, ArchiveProfile: %v",
        releaseFlags.ArchiveStderr,
        releaseFlags.ArchiveJobSpec,
        releaseFlags.ArchiveFailContext,
        releaseFlags.ArchiveProfile);
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NProto::TReleaseJobFlags* protoReleaseJobFlags, const NJobTrackerClient::TReleaseJobFlags& releaseJobFlags)
{
    protoReleaseJobFlags->set_archive_job_spec(releaseJobFlags.ArchiveJobSpec);
    protoReleaseJobFlags->set_archive_stderr(releaseJobFlags.ArchiveStderr);
    protoReleaseJobFlags->set_archive_fail_context(releaseJobFlags.ArchiveFailContext);
    protoReleaseJobFlags->set_archive_profile(releaseJobFlags.ArchiveProfile);
}

void FromProto(NJobTrackerClient::TReleaseJobFlags* releaseJobFlags, const NProto::TReleaseJobFlags& protoReleaseJobFlags)
{
    releaseJobFlags->ArchiveJobSpec = protoReleaseJobFlags.archive_job_spec();
    releaseJobFlags->ArchiveStderr = protoReleaseJobFlags.archive_stderr();
    releaseJobFlags->ArchiveFailContext = protoReleaseJobFlags.archive_fail_context();
    releaseJobFlags->ArchiveProfile = protoReleaseJobFlags.archive_profile();
}

void ToProto(NProto::TJobToRemove* protoJobToRemove, const NJobTrackerClient::TJobToRelease& jobToRelease)
{
    ToProto(protoJobToRemove->mutable_job_id(), jobToRelease.JobId);
    ToProto(protoJobToRemove->mutable_release_job_flags(), jobToRelease.ReleaseFlags);

    // COMPAT
    const auto& releaseJobFlags = jobToRelease.ReleaseFlags;
    protoJobToRemove->set_archive_job_spec(releaseJobFlags.ArchiveJobSpec);
    protoJobToRemove->set_archive_stderr(releaseJobFlags.ArchiveStderr);
    protoJobToRemove->set_archive_fail_context(releaseJobFlags.ArchiveFailContext);
    protoJobToRemove->set_archive_profile(releaseJobFlags.ArchiveProfile);
}

void FromProto(NJobTrackerClient::TJobToRelease* jobToRelease, const NProto::TJobToRemove& protoJobToRemove)
{
    FromProto(&jobToRelease->JobId, protoJobToRemove.job_id());
    if (protoJobToRemove.has_release_job_flags()) {
        FromProto(&jobToRelease->ReleaseFlags, protoJobToRemove.release_job_flags());
    } else {
        // COMPAT
        jobToRelease->ReleaseFlags.ArchiveStderr = protoJobToRemove.archive_stderr();
        jobToRelease->ReleaseFlags.ArchiveJobSpec = protoJobToRemove.archive_job_spec();
        jobToRelease->ReleaseFlags.ArchiveFailContext = protoJobToRemove.archive_fail_context();
        jobToRelease->ReleaseFlags.ArchiveProfile = protoJobToRemove.archive_profile();
    }
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient
