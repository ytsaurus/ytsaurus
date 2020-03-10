#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/ref_counted.h>

#include <yt/client/api/public.h>

#include <yt/ytlib/api/public.h>

namespace NYT::NCellMasterClient {

///////////////////////////////////////////////////////////////////////////////

class TCellDirectorySynchronizer
    : public TRefCounted
{
public:
    TCellDirectorySynchronizer(
        TCellDirectorySynchronizerConfigPtr config,
        TCellDirectoryPtr directory);

    ~TCellDirectorySynchronizer();

    void Start();
    void Stop();

    //! Returns a future that will be set after the next sync.
    TFuture<void> NextSync(bool force = false);

    //! Returns a future that was set by the most recent sync.
    TFuture<void> RecentSync();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMasterClient
