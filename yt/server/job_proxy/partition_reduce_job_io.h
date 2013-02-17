#pragma once

#include "private.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

TAutoPtr<TUserJobIO> CreatePartitionReduceJobIO(
    NScheduler::TJobIOConfigPtr config,
    IJobHost* host);

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
