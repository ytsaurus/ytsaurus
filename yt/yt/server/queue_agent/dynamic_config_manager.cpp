#include "dynamic_config_manager.h"

#include "config.h"

namespace NYT::NQueueAgent {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(
    const TQueueAgentBootstrapConfigPtr& queueAgentConfig,
    NApi::IClientPtr client,
    IInvokerPtr invoker)
    : TDynamicConfigManagerBase<TQueueAgentComponentDynamicConfig>(
        TDynamicConfigManagerOptions{
            .ConfigPath = queueAgentConfig->DynamicConfigPath,
            .Name = "QueueAgent",
        },
        queueAgentConfig->DynamicConfigManager,
        std::move(client),
        std::move(invoker))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
