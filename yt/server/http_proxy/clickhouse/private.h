#pragma once

#include "public.h"

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

extern const TString ClickHouseUserName;

DECLARE_REFCOUNTED_CLASS(TCachedDiscovery)
DECLARE_REFCOUNTED_CLASS(TCliqueCache)

DECLARE_REFCOUNTED_CLASS(TCliqueCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
