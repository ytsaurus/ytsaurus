#pragma once

#include "public.h"

#include <yt/yt/core/service_discovery/service_discovery.h>

namespace NYT::NServiceDiscovery::NYP {

////////////////////////////////////////////////////////////////////////////////

//! https://wiki.yandex-team.ru/yp/discovery/usage/
/*!
 * Default caching policy:
 * - Hold erroneous result for several seconds not to create pressure on the provider.
 * - Evict results which are inaccessed for a long period of time (days).
 * - Update successful results in the background with a period of several seconds.
 *
 * NB! Stale successful discovery result is always preferred to the most actual erroneous one.
 */
IServiceDiscoveryPtr CreateServiceDiscovery(TServiceDiscoveryConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery::NYP
