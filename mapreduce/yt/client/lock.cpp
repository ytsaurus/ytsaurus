#include "lock.h"

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TLock::TLock(const TLockId& lockId)
    : LockId_(lockId)
    , Acquired_(NThreading::MakeFuture())
    , Client_(nullptr)
{ }

TLock::TLock(const TLockId& lockId, TClientPtr client)
    : LockId_(lockId)
    , Client_(std::move(client))
{ }

const TLockId& TLock::GetId() const
{
    return LockId_;
}

const NThreading::TFuture<void>& TLock::GetAcquiredFuture() const
{
    if (!Acquired_) {
        auto promise = NThreading::NewPromise<void>();
        Client_->GetLockWaiter().Watch(LockId_, promise);
        Acquired_ = promise.GetFuture();
    }
    return *Acquired_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
