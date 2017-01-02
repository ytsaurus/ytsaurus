#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

namespace NYT {
namespace NHiveClient {

////////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer
    : public TRefCounted
{
public:
    TCellDirectorySynchronizer(
        TCellDirectorySynchronizerConfigPtr config,
        NHiveClient::TCellDirectoryPtr cellDirectory,
        const TCellId& primaryCellId);
    ~TCellDirectorySynchronizer();

    void Start();
    void Stop();

    //! Forces an out-of-order synchronization.
    TFuture<void> Sync();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
