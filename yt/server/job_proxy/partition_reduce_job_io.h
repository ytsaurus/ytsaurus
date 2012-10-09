#pragma once

#include "public.h"
#include "user_job_io.h"

#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

TAutoPtr<TUserJobIO> CreatePartitionReduceJobIO(
    NScheduler::TJobIOConfigPtr config,
    IJobHost* host,
    NMetaState::TMasterDiscoveryConfigPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec);

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
