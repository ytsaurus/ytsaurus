#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

struct ICellDirectorySynchronizer
    : public TRefCounted
{
    //! Starts periodic syncs.
    virtual void Start() = 0;

    //! Stops periodic syncs.
    virtual void Stop() = 0;

    //! Returns a future that gets set with the next sync.
    //! Starts the synchronizer if not started yet.
    virtual TFuture<void> Sync() = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

ICellDirectorySynchronizerPtr CreateCellDirectorySynchronizer(
    TCellDirectorySynchronizerConfigPtr config,
    ICellDirectoryPtr cellDirectory,
    NObjectClient::TCellIdList sourceOrTruthCellIds,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
