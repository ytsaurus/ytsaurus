#include "stdafx.h"
#include "semaphore.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TAsyncSemaphore::TAsyncSemaphore(int maxFreeSlots)
    : MaxFreeSlots(maxFreeSlots)
    , FreeSlotCount(maxFreeSlots)
    , RequestedSlots(0)
    , AcquireEvent(NULL)
    , StaticResult(New< TFuture<TVoid> >())
{
    StaticResult->Set(TVoid());
}

void TAsyncSemaphore::Release(int slots /* = 1 */)
{
    TGuard<TSpinLock> guard(SpinLock);
    FreeSlotCount += slots;
    YASSERT(FreeSlotCount <= MaxFreeSlots);

    if (AcquireEvent && FreeSlotCount > 0) {
        FreeSlotCount -= RequestedSlots;
        RequestedSlots = 0;
        auto event = AcquireEvent;
        AcquireEvent.Reset();

        guard.Release();
        event->Set(TVoid());
    }
}

TFuture<TVoid>::TPtr TAsyncSemaphore::AsyncAcquire(int slots /* = 1 */)
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    TGuard<TSpinLock> guard(SpinLock);
    if (FreeSlotCount > 0) {
        FreeSlotCount -= slots;
        return StaticResult;
    }

    YASSERT(!AcquireEvent);
    AcquireEvent = New< TFuture<TVoid> >();
    RequestedSlots = slots;
    return AcquireEvent;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
