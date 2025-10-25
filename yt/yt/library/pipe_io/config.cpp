#include "config.h"

namespace NYT::NPipeIO {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TPipeIODispatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_polling_period", &TThis::ThreadPoolPollingPeriod)
        .Default(TDuration::MilliSeconds(10));
}

TPipeIODispatcherConfigPtr TPipeIODispatcherConfig::ApplyDynamic(
    const TPipeIODispatcherDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = CloneYsonStruct(MakeStrong(this));
    UpdateYsonStructField(mergedConfig->ThreadPoolPollingPeriod, dynamicConfig->ThreadPoolPollingPeriod);
    mergedConfig->Postprocess();
    return mergedConfig;
}

////////////////////////////////////////////////////////////////////////////////

void TPipeIODispatcherDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("thread_pool_polling_period", &TThis::ThreadPoolPollingPeriod)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipeIO
