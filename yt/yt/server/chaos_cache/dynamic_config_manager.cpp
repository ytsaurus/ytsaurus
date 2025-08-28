#include "dynamic_config_manager.h"

#include "bootstrap.h"

namespace NYT::NChaosCache {

////////////////////////////////////////////////////////////////////////////////

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(IBootstrap* bootstrap)
    : TDynamicConfigManagerBase(
        TDynamicConfigManagerOptions{
            .ConfigPath = "//sys/chaos_caches/@config",
            .Name = "ChaosCache",
        },
        bootstrap->GetConfig()->DynamicConfigManager,
        bootstrap->GetRootClient(),
        bootstrap->GetControlInvoker())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache
