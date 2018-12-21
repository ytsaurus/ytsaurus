#pragma once

#include "public.h"

#include <yt/core/misc/config.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

class TTableMountCacheConfig
    : public TAsyncExpiringCacheConfig
{  };

DEFINE_REFCOUNTED_TYPE(TTableMountCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
