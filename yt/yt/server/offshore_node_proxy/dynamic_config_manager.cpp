#include "dynamic_config_manager.h"

#include "config.h"

namespace NYT::NOffshoreNodeProxy {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(
    const TOffshoreNodeProxyBootstrapConfigPtr& OffshoreNodeProxyConfig,
    NApi::IClientPtr client,
    IInvokerPtr invoker)
    : TDynamicConfigManagerBase<TOffshoreNodeProxyDynamicConfig>(
        TDynamicConfigManagerOptions{
            .ConfigPath = OffshoreNodeProxyConfig->DynamicConfigPath,
            .Name = "OffshoreNodeProxy",
        },
        OffshoreNodeProxyConfig->DynamicConfigManager,
        std::move(client),
        std::move(invoker))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
