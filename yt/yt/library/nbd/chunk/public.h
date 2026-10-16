#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NNbd::NChunk {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TChunkBlockDeviceConfig)
DECLARE_REFCOUNTED_STRUCT(TPageCacheConfig)

////////////////////////////////////////////////////////////////////////////////

//! Default number of TCP connections used for NBD RPC requests (multiplexing parallelism).
constexpr int DefaultNbdMultiplexingParallelism = 3;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NChunk
