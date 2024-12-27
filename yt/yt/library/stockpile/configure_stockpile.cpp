#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <library/cpp/yt/stockpile/stockpile.h>

namespace NYT {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TStockpileConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TStockpileDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TStockpileConfigPtr& config)
{
    TStockpileManager::Reconfigure(*config);
}

void ReconfigureSingleton(
    const TStockpileConfigPtr& config,
    const TStockpileDynamicConfigPtr& dynamicConfig)
{
    TStockpileManager::Reconfigure(*config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "stockpile",
    TStockpileConfig,
    TStockpileDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
