#pragma once

#include "public.h"

#include <yt/yt/client/tablet_client/table_mount_cache.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct IStickyMountCache
    : public NTabletClient::ITableMountCache
{
    virtual TDuration GetWaitTime() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IStickyMountCache)

////////////////////////////////////////////////////////////////////////////////

IStickyMountCachePtr CreateStickyCache(
    NTabletClient::ITableMountCachePtr mountCache);;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
