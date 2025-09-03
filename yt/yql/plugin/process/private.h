#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NYqlPlugin {
namespace NProcess {

DECLARE_REFCOUNTED_STRUCT(TProcessYqlPluginInternalConfig)

YT_DEFINE_GLOBAL(const NLogging::TLogger, ProcessYqlPluginLogger, "ProcessYqlPlugin");
YT_DEFINE_GLOBAL(const NLogging::TLogger, YqlExecutorProcessLogger, "YqlExecutorProcess");

} // namespace NProcess
} // namespace NYT::NYqlPlugin
