#include "dynamic_config_manager.h"

#include "bootstrap.h"

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(IBootstrap* bootstrap)
    : TDynamicConfigManagerBase(
        TDynamicConfigManagerOptions{
            .ConfigPath = "//sys/master_caches/@config",
            .Name = "MasterCache",
        },
        bootstrap->GetConfig()->DynamicConfigManager,
        bootstrap->GetRootClient(),
        bootstrap->GetControlInvoker())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
