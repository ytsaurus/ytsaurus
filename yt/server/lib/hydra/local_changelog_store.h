#pragma once

#include "public.h"

#include <yt/core/profiling/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

IChangelogStoreFactoryPtr CreateLocalChangelogStoreFactory(
    TFileChangelogStoreConfigPtr config,
    const TString& threadName,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
