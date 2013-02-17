#pragma once

#include "private.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

TAutoPtr<TUserJobIO> CreateSortedReduceJobIO(
    NScheduler::TJobIOConfigPtr ioConfig,
    IJobHost* host);

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
