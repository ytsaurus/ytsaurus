#pragma once

#include "public.h"
#include "job.h"

#include <server/job_agent/public.h>

#include <ytlib/scheduler/job.pb.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateUserJob(
    IJobHost* host,
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    std::unique_ptr<IUserJobIO> userJobIO,
    const NJobAgent::TJobId& jobId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
