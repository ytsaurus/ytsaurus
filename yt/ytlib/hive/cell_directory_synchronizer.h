#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/logging/public.h>

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer
    : public TRefCounted
{
public:
    TCellDirectorySynchronizer(
        TCellDirectorySynchronizerConfigPtr config,
        TCellDirectoryPtr cellDirectory,
        TCellId primaryCellId,
        const NLogging::TLogger& logger);
    ~TCellDirectorySynchronizer();

    //! Starts periodic syncs.
    void Start();

    //! Stops periodic syncs.
    void Stop();

    //! Returns a future that gets set with the next sync.
    //! Starts the synchronizer if not started yet.
    TFuture<void> Sync();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
