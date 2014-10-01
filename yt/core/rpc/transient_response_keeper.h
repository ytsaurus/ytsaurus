#pragma once

#include "public.h"

#include <core/profiling/profiler.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Creates a keeper instance that (transiently) remembers every finished request.
IResponseKeeperPtr CreateTransientResponseKeeper(
    TResponseKeeperConfigPtr config,
    const NProfiling::TProfiler& profiler = NProfiling::TProfiler());

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
