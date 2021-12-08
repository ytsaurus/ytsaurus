#pragma once

#include <yt/yt/server/lib/hydra_common/file_changelog_dispatcher.h>

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

NHydra::IFileChangelogDispatcherPtr CreateFileChangelogDispatcher(
    NIO::IIOEnginePtr ioEngine,
    NHydra::TFileChangelogDispatcherConfigPtr config,
    TString threadName,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
