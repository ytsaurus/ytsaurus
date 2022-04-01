#include "dynamic_config_manager.h"

#include "config.h"

namespace NYT::NQueueAgent {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////


TDynamicConfigManager::TDynamicConfigManager(
    const TQueueAgentServerConfigPtr& queueAgentConfig,
    NApi::IClientPtr masterClient,
    IInvokerPtr invoker)
    : TDynamicConfigManagerBase<TQueueAgentServerDynamicConfig>(
        TDynamicConfigManagerOptions{
            .ConfigPath = queueAgentConfig->DynamicConfigPath,
            .Name = "QueueAgent",
        },
        queueAgentConfig->DynamicConfigManager,
        std::move(masterClient),
        std::move(invoker))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
