#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NYqlPlugin {
namespace NProcess {

DECLARE_REFCOUNTED_STRUCT(TYqlProcessPluginOptions)
DECLARE_REFCOUNTED_STRUCT(TYqlPluginProcessInternalConfig)

YT_DEFINE_GLOBAL(const NLogging::TLogger, YqlProcessPluginLogger, "YqlProcessPlugin");

} // namespace NProcess
} // namespace NYT::NYqlPlugin
