#pragma once

#include "config.h"

#include <yt/yql/agent/bootstrap.h>
#include <yt/yql/agent/config.h>

#include <yt/yql/plugin/plugin.h>

namespace NYT::NYqlPlugin {
namespace NProcess {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateProcessYqlPlugin(
    NYqlAgent::TBootstrap* bootstrap,
    TYqlPluginOptions options,
    NYqlPlugin::NProcess::TYqlProcessPluginConfigPtr config,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NProcess
} // namespace NYT::NYqlPlugin
