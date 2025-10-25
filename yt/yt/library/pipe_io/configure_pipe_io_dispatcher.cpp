#include "pipe_io_dispatcher.h"
#include "config.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NPipeIO {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void SetupSingletonConfigParameter(TYsonStructParameter<TPipeIODispatcherConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TPipeIODispatcherDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TPipeIODispatcherConfigPtr& config)
{
    TPipeIODispatcher::Get()->Configure(config);
}

void ReconfigureSingleton(
    const TPipeIODispatcherConfigPtr& config,
    const TPipeIODispatcherDynamicConfigPtr& dynamicConfig)
{
    TPipeIODispatcher::Get()->Configure(config->ApplyDynamic(dynamicConfig));
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "pipe_io_dispatcher",
    TPipeIODispatcherConfig,
    TPipeIODispatcherDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipeIO
