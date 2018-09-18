#pragma once

#include "public.h"

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Creates a simple client-side block cache.
IBlockCachePtr CreateClientBlockCache(
    TBlockCacheConfigPtr config,
    EBlockType supportedBlockTypes,
    const NProfiling::TProfiler& profiler = NProfiling::TProfiler());

//! Returns an always-empty block cache.
IBlockCachePtr GetNullBlockCache();

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
