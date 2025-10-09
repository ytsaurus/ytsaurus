#pragma once

#include <yt/yql/agent/bootstrap.h>

#include <yt/yql/plugin/config.h>
#include <yt/yql/plugin/plugin.h>

namespace NYT::NYqlPlugin::NProcess {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateProcessYqlPlugin(
    TYqlPluginConfigPtr pluginConfig,
    TSingletonsConfigPtr singletonsConfig,
    NApi::NNative::TConnectionCompoundConfigPtr clusterConnectionConfig,
    TString maxSupportedYqlVersion,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin::NProcess
