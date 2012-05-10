#include "stdafx.h"
#include "semaphore.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TAsyncSemaphore::TAsyncSemaphore(i64 maxFreeSlots)
    : MaxFreeSlots(maxFreeSlots)
    , FreeSlotCount(maxFreeSlots)
    , RequestedSlots(0)
    , AcquireEvent(Null)
    , StaticResult(MakePromise())
{ }

void TAsyncSemaphore::Release(i64 slots /* = 1 */)
{
    TGuard<TSpinLock> guard(SpinLock);
    FreeSlotCount += slots;
    YASSERT(FreeSlotCount <= MaxFreeSlots);

    if (!AcquireEvent.IsNull() && FreeSlotCount > 0) {
        FreeSlotCount -= RequestedSlots;
        RequestedSlots = 0;
        auto event = AcquireEvent;
        AcquireEvent.Reset();

        guard.Release();
        event.Set();
    }
}

TFuture<void> TAsyncSemaphore::AsyncAcquire(i64 slots /* = 1 */)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    TGuard<TSpinLock> guard(SpinLock);
    if (FreeSlotCount > 0) {
        FreeSlotCount -= slots;
        return StaticResult;
    }

    YASSERT(AcquireEvent.IsNull());
    AcquireEvent = NewPromise<void>();
    RequestedSlots = slots;
    return AcquireEvent;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
