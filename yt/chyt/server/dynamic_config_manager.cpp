#include "dynamic_config_manager.h"
#include "config.h"

namespace NYT::NClickHouseServer {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(
    const TClickHouseServerBootstrapConfigPtr& staticConfig,
    NApi::IClientPtr client,
    IInvokerPtr invoker)
    : TDynamicConfigManagerBase<TClickHouseServerDynamicConfig>(
        TDynamicConfigManagerOptions{
            .ConfigPath = *staticConfig->DynamicConfigPath,
            .Name = "Chyt",
        },
        staticConfig->DynamicConfigManager,
        std::move(client),
        std::move(invoker))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
