#include "config.h"

#include <yt/yql/plugin/config.h>

namespace NYT::NYqlPlugin {
namespace NProcess {

void TProcessYqlPluginInternalConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("slot_index", &TThis::SlotIndex);
    registrar.Parameter("plugin_options", &TThis::PluginConfig)
        .DefaultNew();

    registrar.Parameter("max_supported_yql_version", &TThis::MaxSupportedYqlVersion);

    registrar.Parameter("singletons_config", &TThis::SingletonsConfig);
}

} // namespace NProcess
} // namespace NYT::NYqlPlugin
