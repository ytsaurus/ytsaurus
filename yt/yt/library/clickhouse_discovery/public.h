#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/library/discovery_client/discovery.h>
#include <yt/yt/library/discovery_client/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

using IDiscovery = NDiscoveryClient::IDiscovery;
using IDiscoveryPtr = NDiscoveryClient::IDiscoveryPtr;
using TDiscoveryConfig = NDiscoveryClient::TDiscoveryConfig;
using TDiscoveryConfigPtr = NDiscoveryClient::TDiscoveryConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
