#include "dynamic_config_manager.h"

#include "config.h"

namespace NYT::NQueryTracker {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////


TDynamicConfigManager::TDynamicConfigManager(
    const TQueryTrackerServerConfigPtr& queryTrackerConfig,
    NApi::IClientPtr client,
    IInvokerPtr invoker)
    : TDynamicConfigManagerBase<TQueryTrackerServerDynamicConfig>(
        TDynamicConfigManagerOptions{
            .ConfigPath = queryTrackerConfig->DynamicConfigPath,
            .Name = "QueryTracker",
        },
        queryTrackerConfig->DynamicConfigManager,
        std::move(client),
        std::move(invoker))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
