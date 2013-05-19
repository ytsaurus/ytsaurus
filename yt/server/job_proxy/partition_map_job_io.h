#pragma once

#include "private.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

std::unique_ptr<TUserJobIO> CreatePartitionMapJobIO(
    NScheduler::TJobIOConfigPtr ioConfig,
    IJobHost* host);

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
