#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TOffshoreDataGatewayChannelTestingConfig)
DECLARE_REFCOUNTED_CLASS(TOffshoreDataGatewayChannelConfig)

DECLARE_REFCOUNTED_STRUCT(IOffshoreDataGatewayChannelManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
