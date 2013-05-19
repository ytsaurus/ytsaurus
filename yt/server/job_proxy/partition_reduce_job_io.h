#pragma once

#include "private.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

std::unique_ptr<TUserJobIO> CreatePartitionReduceJobIO(
    NScheduler::TJobIOConfigPtr config,
    IJobHost* host);

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
