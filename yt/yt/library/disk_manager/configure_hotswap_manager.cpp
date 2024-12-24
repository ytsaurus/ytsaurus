#include "hotswap_manager.h"
#include "config.h"
#include "private.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NDiskManager {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = DiskManagerLogger;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<THotswapManagerConfigPtr>& parameter)
{
    parameter.Optional();
}

void SetupSingletonConfigParameter(TYsonStructParameter<THotswapManagerDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const THotswapManagerConfigPtr& config)
{
    if (config) {
        THotswapManager::Configure(config);
    } else {
        YT_LOG_INFO("Hotswap manager is not configured");
    }
}

void ReconfigureSingleton(
    const THotswapManagerConfigPtr& /*config*/,
    const THotswapManagerDynamicConfigPtr& dynamicConfig)
{
    THotswapManager::Reconfigure(dynamicConfig);
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "hotswap_manager",
    THotswapManagerConfig,
    THotswapManagerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiskManager
