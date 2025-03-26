#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDiscovery)
DECLARE_REFCOUNTED_CLASS(TDiscovery)

DECLARE_REFCOUNTED_STRUCT(TDiscoveryBaseConfig)
DECLARE_REFCOUNTED_STRUCT(TDiscoveryV1Config)
DECLARE_REFCOUNTED_STRUCT(TDiscoveryV2Config)
DECLARE_REFCOUNTED_STRUCT(TDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
