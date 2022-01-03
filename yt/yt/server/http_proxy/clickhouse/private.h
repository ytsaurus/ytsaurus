#pragma once

#include "public.h"

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

inline const TString ClickHouseUserName("yt-clickhouse");

DECLARE_REFCOUNTED_CLASS(TCachedDiscovery)
DECLARE_REFCOUNTED_CLASS(TDiscoveryCache)

DECLARE_REFCOUNTED_CLASS(TDiscoveryCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
