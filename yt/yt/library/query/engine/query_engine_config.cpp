#include "cg_cache.h"

#include <yt/yt/library/query/engine_api/query_engine_config.h>

#include <yt/yt/library/query/base/private.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NQueryClient {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TQueryEngineConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TQueryEngineDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TQueryEngineConfigPtr& config)
{
#ifdef YT_VERBOSE_CHANGING_QUERY_ENGINE_CONFIG
    YT_LOG_INFO("Configure QueryEngine (Config: %v)", ConvertToYsonString(config, EYsonFormat::Text));
#else
    Y_UNUSED(config);
#endif
}

void ReconfigureSingleton(
    const TQueryEngineConfigPtr& /*config*/,
    const TQueryEngineDynamicConfigPtr& dynamicConfig)
{
    if (dynamicConfig->CodegenCache) {
        TCodegenCacheSingleton::Reconfigure(dynamicConfig->CodegenCache);
    }

#ifdef YT_VERBOSE_CHANGING_QUERY_ENGINE_CONFIG
    YT_LOG_INFO("Reconfigure QueryEngine (Config: %v)", ConvertToYsonString(dynamicConfig, EYsonFormat::Text));
#endif
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "query_engine_config",
    TQueryEngineConfig,
    TQueryEngineDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
