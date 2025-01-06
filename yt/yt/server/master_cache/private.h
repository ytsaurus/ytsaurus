#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NMasterCache {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMasterCacheBootstrapConfig)
DECLARE_REFCOUNTED_CLASS(TMasterCacheProgramtConfig)
DECLARE_REFCOUNTED_CLASS(TMasterCacheDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TChaosCache)
DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

DECLARE_REFCOUNTED_STRUCT(IBootstrapBase)
DECLARE_REFCOUNTED_STRUCT(IBootstrap)
DECLARE_REFCOUNTED_STRUCT(IPartBootstrap)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, MasterCacheLogger, "MasterCache");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, MasterCacheProfiler, "/master_cache");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMasterCache
