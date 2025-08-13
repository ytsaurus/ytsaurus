#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

IChangelogStoreFactoryPtr CreateLocalChangelogStoreFactory(
    TFileChangelogStoreConfigPtr config,
    std::string threadName,
    const NProfiling::TProfiler& profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
