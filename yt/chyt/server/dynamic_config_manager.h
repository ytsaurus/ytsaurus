#pragma once

#include "private.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TClickHouseServerDynamicConfig>
{
public:
    TDynamicConfigManager(
        const TClickHouseServerBootstrapConfigPtr& staticConfig,
        NApi::IClientPtr client,
        IInvokerPtr invoker);
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
