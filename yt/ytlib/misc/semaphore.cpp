#include "stdafx.h"
#include "semaphore.h"
#include <ytlib/misc/nullable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TAsyncSemaphore::TAsyncSemaphore(i64 maxFreeSlots)
    : MaxFreeSlots(maxFreeSlots)
    , FreeSlotCount(maxFreeSlots)
    , ReadyEvent(Null)
    , FreeEvent(Null)
    , StaticResult(MakePromise())
{ }

void TAsyncSemaphore::Release(i64 slots /* = 1 */)
{
    TGuard<TSpinLock> guard(SpinLock);
    FreeSlotCount += slots;
    YASSERT(FreeSlotCount <= MaxFreeSlots);

    TPromise<void> ready(Null);
    TPromise<void> free(Null);

    if (!ReadyEvent.IsNull() && FreeSlotCount > 0) {
        ready.Swap(ReadyEvent);
    }

    if (!FreeEvent.IsNull() && FreeSlotCount == MaxFreeSlots) {
        free.Swap(FreeEvent);
    }

    guard.Release();
    if (!ready.IsNull())
        ready.Set();

    if (!free.IsNull())
        free.Set();
}

void TAsyncSemaphore::Acquire(i64 slots /* = 1 */)
{
    TGuard<TSpinLock> guard(SpinLock);
    FreeSlotCount -= slots;
}

bool TAsyncSemaphore::IsReady() const
{
    return FreeSlotCount > 0;
}

TFuture<void> TAsyncSemaphore::GetReadyEvent()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (IsReady()) {
        YASSERT(ReadyEvent.IsNull());
        return StaticResult;
    } else if (ReadyEvent.IsNull()) {
        ReadyEvent = NewPromise<void>();
    }

    return ReadyEvent;
}

TFuture<void> TAsyncSemaphore::GetFreeEvent()
{
    TGuard<TSpinLock> guard(SpinLock);
    if (FreeSlotCount == MaxFreeSlots) {
        YASSERT(FreeEvent.IsNull());
        return StaticResult;
    } else if (FreeEvent.IsNull()) {
        FreeEvent = NewPromise<void>();
    }

    return FreeEvent;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 
