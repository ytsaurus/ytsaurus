#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/library/dynamic_config/dynamic_config_manager.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfigManager
    : public NDynamicConfig::TDynamicConfigManagerBase<TBundleControllerDynamicConfig>
{
public:
    TDynamicConfigManager(
        const TCellBalancerBootstrapConfigPtr& config,
        const IBootstrap* bootstrap);
};

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
