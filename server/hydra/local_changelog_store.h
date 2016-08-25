#pragma once

#include "public.h"

#include <yt/core/profiling/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

IChangelogStoreFactoryPtr CreateLocalChangelogStoreFactory(
    TFileChangelogStoreConfigPtr config,
    const Stroka& threadName,
    const NProfiling::TProfiler& profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
