#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger JaegerLogger{"Jaeger"};
inline const NProfiling::TProfiler TracingProfiler{"/tracing"};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
