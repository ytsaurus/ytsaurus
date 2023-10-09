#pragma once

#include "public.h"

#include <yt/yt/server/lib/io/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Provides a factory for creating new and opening existing file changelogs.
//! Manages a background thread that keeps track of unflushed changelogs and
//! issues flush requests periodically.
struct IFileChangelogDispatcher
    : public TRefCounted
{
    //! Updates the configuration.
    virtual void Reconfigure(TFileChangelogDispatcherConfigPtr config) = 0;

    //! Returns the invoker managed by the dispatcher.
    virtual IInvokerPtr GetInvoker() = 0;

    //! Asynchronously creates a new changelog.
    virtual TFuture<IFileChangelogPtr> CreateChangelog(
        int id,
        const TString& path,
        const NProto::TChangelogMeta& meta,
        const TFileChangelogConfigPtr& config) = 0;

    //! Synchronously opens an existing changelog.
    virtual TFuture<IFileChangelogPtr> OpenChangelog(
        int id,
        const TString& path,
        const TFileChangelogConfigPtr& config) = 0;

    //! Flushes all active changelogs owned by this dispatcher.
    virtual TFuture<void> FlushChangelogs() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileChangelogDispatcher)

////////////////////////////////////////////////////////////////////////////////

IFileChangelogDispatcherPtr CreateFileChangelogDispatcher(
    NIO::IIOEnginePtr ioEngine,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    TFileChangelogDispatcherConfigPtr config,
    TString threadName,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
