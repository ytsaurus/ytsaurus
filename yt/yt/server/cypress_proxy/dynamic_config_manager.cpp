#include "dynamic_config_manager.h"

#include "bootstrap.h"

namespace NYT::NCypressProxy {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

static const NYPath::TYPath CypressProxyConfigPath = "//sys/cypress_proxies/@config";

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(IBootstrap* bootstrap)
    : TDynamicConfigManagerBase(
        TDynamicConfigManagerOptions{
            .ConfigPath = CypressProxyConfigPath,
            .Name = "CypressProxy",
        },
        bootstrap->GetConfig()->DynamicConfigManager,
        bootstrap->GetRootClient(),
        bootstrap->GetControlInvoker())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
