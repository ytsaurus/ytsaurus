#pragma once

#include "public.h"
#include "job.h"

#include <yt/ytlib/scheduler/proto/job.pb.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateUserJob(IJobHostPtr host,
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    NJobTrackerClient::TJobId jobId,
    const std::vector<int>& ports,
    std::unique_ptr<TUserJobWriteController> userJobWriteController);

const TString& GetCGroupUserJobBase();
const TString& GetCGroupUserJobPrefix();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
