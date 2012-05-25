#include "stdafx.h"

#include "config.h"

#include "user_job_io.h"
#include "map_job_io.h"

#include <ytlib/election/leader_channel.h>

namespace NYT {
namespace NJobProxy {

using namespace NElection;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<IUserJobIO> CreateUserJobIO(
    TJobIOConfigPtr config,
    NElection::TLeaderLookup::TConfigPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec)
{
    YCHECK(jobSpec.HasExtension(TUserJobSpecExt::user_job_spec_ext));

    auto masterChannel = CreateLeaderChannel(mastersConfig);

    return new TMapJobIO(
        config, 
        masterChannel, 
        jobSpec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

