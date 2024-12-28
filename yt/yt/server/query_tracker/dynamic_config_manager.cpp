#include "dynamic_config_manager.h"

#include "config.h"

namespace NYT::NQueryTracker {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////


TDynamicConfigManager::TDynamicConfigManager(
    const TQueryTrackerBootstrapConfigPtr& queryTrackerConfig,
    NApi::IClientPtr client,
    IInvokerPtr invoker)
    : TDynamicConfigManagerBase<TQueryTrackerComponentDynamicConfig>(
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
