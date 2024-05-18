#include "dynamic_config_manager.h"

#include "bootstrap.h"

namespace NYT::NKafkaProxy {

using namespace NDynamicConfig;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(IBootstrap* bootstrap)
    : TDynamicConfigManagerBase(
        TDynamicConfigManagerOptions{
            .ConfigPath = TYPath(KafkaProxiesRootPath) + "/@config",
            .Name = "KafkaProxy",
        },
        bootstrap->GetConfig()->DynamicConfigManager,
        bootstrap->GetRootClient(),
        bootstrap->GetControlInvoker())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
