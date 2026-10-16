#include "config.h"

#include <yt/yql/plugin/config.h>

namespace NYT::NYqlPlugin::NProcess {

////////////////////////////////////////////////////////////////////////////////

void TProcessYqlPluginInternalConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("slot_index", &TThis::SlotIndex)
        .Default(0);
    registrar.Parameter("plugin_options", &TThis::PluginConfig)
        .DefaultNew();

    registrar.Parameter("singletons_config", &TThis::SingletonsConfig);

    registrar.Parameter("plugin_dynamic_config", &TThis::PluginDynamicConfig)
        .DefaultNew();

    registrar.Parameter("use_token_resolver", &TThis::UseTokenResolver)
        .Default(false);
    registrar.Parameter("token_service_socket_path", &TThis::TokenServiceSocketPath)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin::NProcess
