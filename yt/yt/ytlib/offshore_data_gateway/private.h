#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

// OffshoreDataGatewayClient is somewhat misleading:
// as offshore data gateway implements DataNodeService, there is no client specific to it.
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, OffshoreDataGatewayClientLogger, "OffshoreDataGatewayClient");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
