#pragma once

#include "private.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

TAutoPtr<TUserJobIO> CreatePartitionMapJobIO(
    NScheduler::TJobIOConfigPtr ioConfig,
    IJobHost* host);

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
