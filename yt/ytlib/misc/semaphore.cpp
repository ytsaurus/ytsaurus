#include "stdafx.h"
#include "semaphore.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TAsyncSemaphore::TAsyncSemaphore(int maxFreeSlots)
    : MaxFreeSlots(maxFreeSlots)
    , FreeSlotCount(maxFreeSlots)
    , AcquireEvent(NULL)
    , StaticResult(New< TFuture<TVoid> >())

{
    StaticResult->Set(TVoid());
}

bool TAsyncSemaphore::Release()
{
    TGuard<TSpinLock> guard(SpinLock);

    if (~AcquireEvent != NULL) {
        YASSERT(FreeSlotCount == 0);
        auto event = AcquireEvent;
        AcquireEvent.Reset();

        guard.Release();
        event->Set(TVoid());
    } else if (FreeSlotCount < MaxFreeSlots) {
         ++FreeSlotCount;
    } else {
        return false;
    }

    return true;
}

TFuture<TVoid>::TPtr TAsyncSemaphore::AsyncAcquire()
{
    VERIFY_THREAD_AFFINITY(ClientThread);

    TGuard<TSpinLock> guard(SpinLock);
    YASSERT(FreeSlotCount >= 0);
    if (FreeSlotCount > 0) {
        --FreeSlotCount;
        return StaticResult;
    }

    YASSERT(~AcquireEvent == NULL);
    AcquireEvent = New< TFuture<TVoid> >();
    return AcquireEvent;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
