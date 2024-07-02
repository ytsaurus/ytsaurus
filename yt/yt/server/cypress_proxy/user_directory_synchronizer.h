#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/rpc/per_user_request_queue_provider.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/signal.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct IUserDirectorySynchronizer
    : public virtual TRefCounted
{
    //! Starts periodic syncs.
    virtual void Start() = 0;

    //! Stops periodic syncs.
    virtual void Stop() = 0;

    //! Returns a future that will be set after the next sync.
    //! Starts the synchronizer if not started yet.
    //! If the force flag is set, synchronization will start immediately
    virtual TFuture<void> NextSync(bool synchronizeImmediately = false) = 0;

    //! Returns a future that was set by the most recent sync.
    //! Starts the synchronizer if not started yet.
    virtual TFuture<void> RecentSync() = 0;

    //! Raised with each synchronization (either successful or not).
    DECLARE_INTERFACE_SIGNAL(void(const TError&), Synchronized);
};

DEFINE_REFCOUNTED_TYPE(IUserDirectorySynchronizer)

IUserDirectorySynchronizerPtr CreateUserDirectorySynchronizer(
    TUserDirectorySynchronizerConfigPtr config,
    NApi::IClientPtr client,
    TUserDirectoryPtr userDirectory,
    IInvokerPtr invoker,
    TCallbackList<void(const TString&)>* reconfigurationCallback);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
