#include "bootstrap.h"
#include "config.h"
#include "dynamic_config_manager.h"

#include <yt/yt/ytlib/api/native/client.h>

namespace NYT::NTabletBalancer {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(
    const TTabletBalancerServerConfigPtr& config,
    const IBootstrap* bootstrap)
    : TDynamicConfigManagerBase<TTabletBalancerDynamicConfig>(
        TDynamicConfigManagerOptions{
            .ConfigPath = config->DynamicConfigPath,
            .Name = "TabletBalancer",
        },
        config->DynamicConfigManager,
        bootstrap->GetClient(),
        bootstrap->GetControlInvoker())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
