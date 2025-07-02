#include "config.h"

namespace NYT::NYqlPlugin {
namespace NProcess {

void TYqlProcessPluginConfig::Register(TRegistrar registrar) {
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);

    registrar.Parameter("slots_count", &TThis::SlotsCount)
        .Default(32);

    registrar.Parameter("slots_root_path", &TThis::SlotsRootPath)
        .Default("/yt/plugin_slots");

    registrar.Parameter("check_process_active_delay", &TThis::CheckProcessActiveDelay)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("log_manager_template", &TThis::LogManagerTemplate)
        .DefaultNew();
}

void TYqlProcessPluginOptions::Register(TRegistrar registrar) {
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

void TYqlPluginProcessInternalConfig::Register(TRegistrar registrar) {
    registrar.Parameter("slot_index", &TThis::SlotIndex);
    registrar.Parameter("plugin_options", &TThis::PluginOptions)
        .DefaultNew();
}

} // namespace NProcess
} // namespace NYT::NYqlPlugin
