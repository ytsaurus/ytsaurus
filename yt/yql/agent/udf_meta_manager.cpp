#include "udf_meta_manager.h"

#include <yt/yt/library/dynamic_config/config.h>

namespace NYT::NYqlAgent {

using namespace NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

TUdfMetaManager::TUdfMetaManager(
    NYPath::TYPath udfMetaPath,
    NApi::IClientPtr client,
    IInvokerPtr invoker)
    : TDynamicConfigManagerBase<NYqlPlugin::TUdfMeta>(
        TDynamicConfigManagerOptions{
            .ConfigPath = std::move(udfMetaPath),
            .Name = "UdfMeta",
        },
        [] {
            auto managerConfig = New<TDynamicConfigManagerConfig>();
            managerConfig->IgnoreConfigAbsence = true;
            return managerConfig;
        }(),
        std::move(client),
        std::move(invoker))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
