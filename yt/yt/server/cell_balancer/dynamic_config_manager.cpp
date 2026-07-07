#include "bootstrap.h"
#include "config.h"
#include "dynamic_config_manager.h"

#include <yt/yt/ytlib/api/native/client.h>

namespace NYT::NCellBalancer {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(
    const TCellBalancerBootstrapConfigPtr& config,
    const IBootstrap* bootstrap)
    : TDynamicConfigManagerBase<TBundleControllerDynamicConfig>(
        TDynamicConfigManagerOptions{
            .ConfigPath = config->DynamicConfigPath,
            .Name = "BundleController",
        },
        config->DynamicConfigManager,
        bootstrap->GetClient(),
        bootstrap->GetControlInvoker())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
