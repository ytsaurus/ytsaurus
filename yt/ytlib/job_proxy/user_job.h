#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/scheduler/job.pb.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<IJob> CreateUserJob(
    IJobHost* host,
    const NScheduler::NProto::TUserJobSpec& userJobSpec,
    TAutoPtr<TUserJobIO> userJobIO);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
