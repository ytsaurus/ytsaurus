#ifndef DISK_LOCATION_INL_H_
#error "Direct inclusion of this file is not allowed, include disk_location.h"
// For the sake of sane code completion.
#include "disk_location.h"
#endif
#undef DISK_LOCATION_INL_H_

#include <yt/yt/server/lib/misc/private.h>

#include <yt/yt/core/concurrency/async_rw_lock.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/atomic_ptr.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NDataNode {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

template <class T>
TFuture<T> TDiskLocation::RegisterAction(TCallback<TFuture<T>()> action)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto readerAcquired = StateChangingLock_.TryAcquireReader();

    if (!readerAcquired) {
        return MakeFuture(TError("Location state is changing"));
    }

    auto finally = Finally([=, this, this_ = MakeStrong(this)] {
        StateChangingLock_.ReleaseReader();
    });

    auto currentState = State_.load();
    if (!CanHandleIncomingActions()) {
        return MakeFuture(TError("Location cannot handle incoming actions")
            << TErrorAttribute("state", currentState));
    }

    auto future = action.Run();

    {
        auto actionsGuard = Guard(ActionsContainerLock_);
        Actions_.insert(future);
    }

    future.AsVoid().Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TErrorOr<void>& /*result*/) {
        auto actionsGuard = Guard(ActionsContainerLock_);
        Actions_.erase(future);
    }));

    return future;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
