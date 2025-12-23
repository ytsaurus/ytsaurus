#include "dynamic_config_manager.h"

#include "config.h"

namespace NYT::NOffshoreDataGateway {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(
    const TOffshoreDataGatewayBootstrapConfigPtr& offshoreDataGatewayConfig,
    NApi::IClientPtr client,
    IInvokerPtr invoker)
    : TDynamicConfigManagerBase<TOffshoreDataGatewayDynamicConfig>(
        TDynamicConfigManagerOptions{
            .ConfigPath = offshoreDataGatewayConfig->DynamicConfigPath,
            .Name = "OffshoreDataGateway",
        },
        offshoreDataGatewayConfig->DynamicConfigManager,
        std::move(client),
        std::move(invoker))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
