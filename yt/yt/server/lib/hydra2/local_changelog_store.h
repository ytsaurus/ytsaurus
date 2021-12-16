#pragma once

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

NHydra::IChangelogStoreFactoryPtr CreateLocalChangelogStoreFactory(
    NHydra::TFileChangelogStoreConfigPtr config,
    const TString& threadName,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
