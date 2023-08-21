#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTimestampProviderConfig)

struct IBootstrap;

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger TimestampProviderLogger("TimestampProvider");
inline const NProfiling::TProfiler TimestampProviderProfiler("/timestamp_provider");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
