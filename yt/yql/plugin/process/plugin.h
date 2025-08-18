#pragma once

#include <yt/yql/agent/bootstrap.h>

#include <yt/yql/plugin/config.h>
#include <yt/yql/plugin/plugin.h>

namespace NYT::NYqlPlugin::NProcess {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateProcessYqlPlugin(
    NYqlAgent::TBootstrap* bootstrap,
    TYqlPluginConfigPtr config,
    const NProfiling::TProfiler& profiler,
    TString maxSupportedYqlVersion);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin::NProcess
