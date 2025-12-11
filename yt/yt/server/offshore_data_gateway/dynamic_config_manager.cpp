#include "dynamic_config_manager.h"

#include "config.h"

namespace NYT::NOffshoreDataGateway {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(
    const TOffshoreDataGatewayBootstrapConfigPtr& OffshoreDataGatewayConfig,
    NApi::IClientPtr client,
    IInvokerPtr invoker)
    : TDynamicConfigManagerBase<TOffshoreDataGatewayDynamicConfig>(
        TDynamicConfigManagerOptions{
            .ConfigPath = OffshoreDataGatewayConfig->DynamicConfigPath,
            .Name = "OffshoreDataGateway",
        },
        OffshoreDataGatewayConfig->DynamicConfigManager,
        std::move(client),
        std::move(invoker))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
