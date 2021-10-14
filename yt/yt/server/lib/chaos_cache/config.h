#pragma once

#include "public.h"

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NChaosCache {

////////////////////////////////////////////////////////////////////////////////

class TChaosCacheConfig
    : public TSlruCacheConfig
{ };

DEFINE_REFCOUNTED_TYPE(TChaosCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache
