#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTimestampProviderBootstrapConfig)
DECLARE_REFCOUNTED_CLASS(TTimestampProviderProgramConfig)

DECLARE_REFCOUNTED_STRUCT(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, TimestampProviderLogger, "TimestampProvider");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, TimestampProviderProfiler, "/timestamp_provider");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
