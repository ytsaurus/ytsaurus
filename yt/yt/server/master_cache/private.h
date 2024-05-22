#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TChaosCache)
DECLARE_REFCOUNTED_CLASS(TMasterCacheConfig)
DECLARE_REFCOUNTED_CLASS(TMasterCacheDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

struct IBootstrap;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, MasterCacheLogger, "MasterCache");
inline const NProfiling::TProfiler MasterCacheProfiler("/master_cache");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
