#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TOffshoreDataGatewayChannelTestingConfig)
DECLARE_REFCOUNTED_STRUCT(TOffshoreDataGatewayChannelConfig)

DECLARE_REFCOUNTED_STRUCT(IOffshoreDataGatewayChannelManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
