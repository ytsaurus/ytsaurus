#include "dynamic_config_manager.h"

#include "config.h"

namespace NYT::NYqlAgent {

using namespace NDynamicConfig;

///////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(
    const TYqlAgentServerConfigPtr& yqlAgentConfig,
    NApi::IClientPtr client,
    IInvokerPtr invoker)
    : TDynamicConfigManagerBase<TYqlAgentServerDynamicConfig>(
        TDynamicConfigManagerOptions{
            .ConfigPath = yqlAgentConfig->DynamicConfigPath,
            .Name = "YqlAgent",
        },
        yqlAgentConfig->DynamicConfigManager,
        std::move(client),
        std::move(invoker))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
