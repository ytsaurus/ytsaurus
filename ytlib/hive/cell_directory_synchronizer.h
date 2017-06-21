#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/logging/public.h>

namespace NYT {
namespace NHiveClient {

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer
    : public TRefCounted
{
public:
    TCellDirectorySynchronizer(
        TCellDirectorySynchronizerConfigPtr config,
        TCellDirectoryPtr cellDirectory,
        const TCellId& primaryCellId,
        const NLogging::TLogger& logger);
    ~TCellDirectorySynchronizer();

    //! Returns a future that gets set with the next sync.
    TFuture<void> Sync();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
