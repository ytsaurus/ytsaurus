#pragma once

#include <yt/yt/core/rpc/public.h>

#include <yt/yql/plugin/plugin.h>

namespace NYT::NYqlPlugin::NProcess {

NRpc::IServicePtr CreateYqlPluginService(
    IInvokerPtr controlInvoker,
    std::unique_ptr<IYqlPlugin> yqlPlugin);

} // namespace NYT::NYqlPlugin::NProcess
