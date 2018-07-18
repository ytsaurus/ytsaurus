#pragma once

#include "public.h"

#include <yt/core/misc/config.h>

namespace NYT {
namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

class TTableMountCacheConfig
    : public TAsyncExpiringCacheConfig
{  };

DEFINE_REFCOUNTED_TYPE(TTableMountCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT
