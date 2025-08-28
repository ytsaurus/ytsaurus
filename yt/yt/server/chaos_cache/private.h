#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChaosCache {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TChaosCacheBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosCacheProgramConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosCacheDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

DECLARE_REFCOUNTED_STRUCT(IBootstrapBase)
DECLARE_REFCOUNTED_STRUCT(IBootstrap)
DECLARE_REFCOUNTED_STRUCT(IPartBootstrap)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ChaosCacheLogger, "ChaosCache");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, ChaosCacheProfiler, "/chaos_cache");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache
