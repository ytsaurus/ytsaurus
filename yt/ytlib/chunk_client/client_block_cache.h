#pragma once

#include "public.h"

#include <core/misc/public.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Creates a simple client-side block cache.
IBlockCachePtr CreateClientBlockCache(
    TSlruCacheConfigPtr config,
    const NProfiling::TProfiler& profiler = NProfiling::TProfiler());

//! Returns an always-empty block cache.
IBlockCachePtr GetNullBlockCache();

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
