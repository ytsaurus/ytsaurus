#include "native_authentication_manager.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NAuth {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TNativeAuthenticationManagerConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TNativeAuthenticationManagerDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TNativeAuthenticationManagerConfigPtr& config)
{
    TNativeAuthenticationManager::Get()->Configure(config);
}

void ReconfigureSingleton(
    const TNativeAuthenticationManagerConfigPtr& /*config*/,
    const TNativeAuthenticationManagerDynamicConfigPtr& dynamicConfig)
{
    TNativeAuthenticationManager::Get()->Reconfigure(dynamicConfig);
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "native_authentication_manager",
    TNativeAuthenticationManagerConfig,
    TNativeAuthenticationManagerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
