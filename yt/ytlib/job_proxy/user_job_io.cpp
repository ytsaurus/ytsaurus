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

TAutoPtr<IUserJobIo> CreateUserJobIo(
    const TJobIoConfigPtr config,
    const NElection::TLeaderLookup::TConfig::TPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec)
{
    YASSERT(jobSpec.HasExtension(TUserJobSpec::user_job_spec));

    auto masterChannel = CreateLeaderChannel(mastersConfig);

    YASSERT(jobSpec.HasExtension(TMapJobSpec::map_job_spec));
    return new TMapJobIo(
        config, 
        ~masterChannel, 
        jobSpec.GetExtension(TMapJobSpec::map_job_spec));

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

