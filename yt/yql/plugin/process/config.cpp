#include "config.h"

#include <yt/yql/plugin/config.h>

namespace NYT::NYqlPlugin::NProcess {

void TProcessYqlPluginInternalConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("slot_index", &TThis::SlotIndex);
    registrar.Parameter("plugin_options", &TThis::PluginConfig)
        .DefaultNew();

    registrar.Parameter("max_supported_yql_version", &TThis::MaxSupportedYqlVersion);

    registrar.Parameter("singletons_config", &TThis::SingletonsConfig);

    registrar.Parameter("dynamic_gateways_config", &TThis::DynamicGatewaysConfig)
        .Default();
}

} // namespace NYT::NYqlPlugin::NProcess
