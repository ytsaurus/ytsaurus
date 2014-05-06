#pragma once

#include "public.h"
#include "job.h"

#include <server/job_agent/public.h>

#include <ytlib/scheduler/job.pb.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

TJobPtr CreateUserJob(
    IJobHost* host,
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    std::unique_ptr<TUserJobIO> userJobIO,
    NJobAgent::TJobId jobId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
