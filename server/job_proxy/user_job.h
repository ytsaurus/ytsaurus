#pragma once

#include "public.h"
#include "job.h"

#include <yt/server/job_agent/public.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateUserJob(IJobHostPtr host,
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    const NJobAgent::TJobId& jobId,
    const std::vector<int>& ports,
    std::unique_ptr<TUserJobWriteController> userJobWriteController);

const TString& GetCGroupUserJobBase();
const TString& GetCGroupUserJobPrefix();

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
