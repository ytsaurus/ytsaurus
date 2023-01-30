#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

IIOEnginePtr CreateIOEngineUring(
    EIOEngineType engineType,
    NYTree::INodePtr ioConfig,
    TString locationId,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger);

bool IsUringIOEngineSupported();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
