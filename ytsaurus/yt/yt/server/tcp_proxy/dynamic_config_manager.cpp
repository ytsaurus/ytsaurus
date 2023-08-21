#include "dynamic_config_manager.h"

#include "bootstrap.h"

namespace NYT::NTcpProxy {

using namespace NDynamicConfig;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(IBootstrap* bootstrap)
    : TDynamicConfigManagerBase(
        TDynamicConfigManagerOptions{
            .ConfigPath = TYPath(TcpProxiesRootPath) + "/@config",
            .Name = "TcpProxy",
        },
        bootstrap->GetConfig()->DynamicConfigManager,
        bootstrap->GetRootClient(),
        bootstrap->GetControlInvoker())
    , InstanceTags_({bootstrap->GetConfig()->Role})
{ }

std::vector<TString> TDynamicConfigManager::GetInstanceTags() const
{
    return InstanceTags_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
