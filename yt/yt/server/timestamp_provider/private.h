#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTimestampProviderConfig)

struct IBootstrap;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger TimestampProviderLogger;

extern const NProfiling::TProfiler TimestampProviderProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
