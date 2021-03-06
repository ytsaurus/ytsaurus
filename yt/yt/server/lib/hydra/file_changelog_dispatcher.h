#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Provides a factory for creating new and opening existing file changelogs.
//! Manages a background thread that keeps track of unflushed changelogs and
//! issues flush requests periodically.
struct IFileChangelogDispatcher
    : public TRefCounted
{
    //! Returns the invoker managed by the dispatcher.
    virtual IInvokerPtr GetInvoker() = 0;

    //! Asynchronously creates a new changelog.
    virtual TFuture<IChangelogPtr> CreateChangelog(
        const TString& path,
        const TFileChangelogConfigPtr& config) = 0;

    //! Synchronously opens an existing changelog.
    virtual TFuture<IChangelogPtr> OpenChangelog(
        const TString& path,
        const TFileChangelogConfigPtr& config) = 0;

    //! Flushes all active changelogs owned by this dispatcher.
    virtual TFuture<void> FlushChangelogs() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileChangelogDispatcher)

IFileChangelogDispatcherPtr CreateFileChangelogDispatcher(
    NChunkClient::IIOEnginePtr ioEngine,
    IFileChangelogDispatcherConfigPtr config,
    TString threadName,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
