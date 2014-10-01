#pragma once

#include "public.h"

#include <core/profiling/profiler.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

IResponseKeeperPtr CreateTransientResponseKeeper(
    TResponseKeeperConfigPtr config,
    const NProfiling::TProfiler& profiler = NProfiling::TProfiler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
