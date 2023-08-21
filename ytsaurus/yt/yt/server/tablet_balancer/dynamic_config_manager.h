#pragma once

#include "public.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TTabletBalancerDynamicConfig>
{
public:
    TDynamicConfigManager(
        const TTabletBalancerServerConfigPtr& config,
        const IBootstrap* bootstrap);
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
