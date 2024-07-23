#pragma once

#include "public.h"
#include "job.h"

#include <yt/yt/ytlib/controller_agent/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateUserJob(
    IJobHostPtr host,
    const NControllerAgent::NProto::TUserJobSpec& userJobSpec,
    NJobTrackerClient::TJobId jobId,
    NJobTrackerClient::EJobType jobType,
    const std::vector<int>& ports,
    std::unique_ptr<TUserJobWriteController> userJobWriteController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
