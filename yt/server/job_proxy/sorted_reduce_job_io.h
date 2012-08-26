#pragma once

#include "public.h"
#include "user_job_io.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

TAutoPtr<TUserJobIO> CreateSortedReduceJobIO(
    TJobIOConfigPtr ioConfig,
    NMetaState::TMasterDiscoveryConfigPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec);

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
