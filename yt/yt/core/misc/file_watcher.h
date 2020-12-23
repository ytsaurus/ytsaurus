#pragma once

#include "file_watcher_api.h"

#include <yt/core/actions/callback.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct IFileWatcher
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual TFileWatchPtr CreateWatch(
        TString path,
        EFileWatchMode mode,
        TClosure callback) = 0;
    virtual void DropWatch(const TFileWatchPtr& watch) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileWatcher)

IFileWatcherPtr CreateFileWatcher(
    IInvokerPtr invoker,
    TDuration checkPeriod);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
