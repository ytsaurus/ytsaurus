#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NNbd::NChunk {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IChunkHandler)
DECLARE_REFCOUNTED_STRUCT(TChunkBlockDeviceConfig)
DECLARE_REFCOUNTED_STRUCT(TPageCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NChunk
