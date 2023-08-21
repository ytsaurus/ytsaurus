#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDiscovery)
DECLARE_REFCOUNTED_CLASS(TDiscovery)

DECLARE_REFCOUNTED_CLASS(TDiscoveryBaseConfig)
DECLARE_REFCOUNTED_CLASS(TDiscoveryV1Config)
DECLARE_REFCOUNTED_CLASS(TDiscoveryV2Config)
DECLARE_REFCOUNTED_CLASS(TDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
