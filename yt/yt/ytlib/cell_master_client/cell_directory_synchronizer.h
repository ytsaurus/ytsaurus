#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/ytlib/api/public.h>

namespace NYT::NCellMasterClient {

////////////////////////////////////////////////////////////////////////////////

struct ICellDirectorySynchronizer
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;

    //! Returns a future that will be set after the next sync.
    virtual TFuture<void> NextSync(bool force = false) = 0;

    //! Returns a future that was set by the most recent sync.
    virtual TFuture<void> RecentSync() = 0;

    virtual void Reconfigure(const TCellDirectorySynchronizerConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

ICellDirectorySynchronizerPtr CreateCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    ICellDirectoryPtr directory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
