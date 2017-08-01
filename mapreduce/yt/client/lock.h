#pragma once

#include "client.h"

#include <mapreduce/yt/interface/client.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TLock
    : public ILock
{
public:
    // Create nonwaitable lock.
    TLock(const TLockId& lockId);

    // Create waitable lock.
    TLock(const TLockId& lockId, TClientPtr client);

    virtual const TLockId& GetId() const override;
    virtual const NThreading::TFuture<void>& GetAcquiredFuture() const override;

private:
    const TLockId LockId_;
    mutable TMaybe<NThreading::TFuture<void>> Acquired_;
    TClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
