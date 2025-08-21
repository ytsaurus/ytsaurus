#include "config.h"

#include <yt/yql/plugin/config.h>

namespace NYT::NYqlPlugin {
namespace NProcess {

void TYqlProcessPluginOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("singletons_config", &TThis::SingletonsConfig)
        .Default();
    registrar.Parameter("gateway_config", &TThis::GatewayConfig)
        .Default();
    registrar.Parameter("dq_gateway_config", &TThis::DqGatewayConfig)
        .Default();
    registrar.Parameter("dq_manager_config", &TThis::DqManagerConfig)
        .Default();
    registrar.Parameter("file_storage_config", &TThis::FileStorageConfig)
        .Default();
    registrar.Parameter("operation_attributes", &TThis::OperationAttributes)
        .Default();
    registrar.Parameter("libraries", &TThis::Libraries)
        .Default();

    registrar.Parameter("yt_token_path", &TThis::YTTokenPath);

    registrar.Parameter("yql_plugin_shared_library", &TThis::YqlPluginSharedLibrary);
}

void TYqlPluginProcessInternalConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("slot_index", &TThis::SlotIndex);
    registrar.Parameter("plugin_options", &TThis::PluginConfig)
        .DefaultNew();

    registrar.Parameter("max_supported_yql_version", &TThis::MaxSupportedYqlVersion);

    registrar.Parameter("start_dq_manager", &TThis::StartDqManager);
}

} // namespace NProcess
} // namespace NYT::NYqlPlugin
