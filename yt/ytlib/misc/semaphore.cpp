#include "stdafx.h"
#include "semaphore.h"
#include <ytlib/misc/nullable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static TFuture<void> PresetResult = MakePromise();

TAsyncSemaphore::TAsyncSemaphore(i64 maxFreeSlots)
    : MaxFreeSlots(maxFreeSlots)
    , FreeSlotCount(maxFreeSlots)
{ }

void TAsyncSemaphore::Release(i64 slots /* = 1 */)
{
    TPromise<void> ready;
    TPromise<void> free;

    {
        TGuard<TSpinLock> guard(SpinLock);

        FreeSlotCount += slots;
        YASSERT(FreeSlotCount <= MaxFreeSlots);

        if (!ReadyEvent.IsNull() && FreeSlotCount > 0) {
            ready.Swap(ReadyEvent);
        }

        if (!FreeEvent.IsNull() && FreeSlotCount == MaxFreeSlots) {
            free.Swap(FreeEvent);
        }
    }

    if (!ready.IsNull()) {
        ready.Set();
    }

    if (!free.IsNull()) {
        free.Set();
    }
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
        YCHECK(ReadyEvent.IsNull());
        return PresetResult;
    } else if (ReadyEvent.IsNull()) {
        ReadyEvent = NewPromise();
    }

    return ReadyEvent;
}

TFuture<void> TAsyncSemaphore::GetFreeEvent()
{
    TGuard<TSpinLock> guard(SpinLock);

    if (FreeSlotCount == MaxFreeSlots) {
        YCHECK(FreeEvent.IsNull());
        return PresetResult;
    } else if (FreeEvent.IsNull()) {
        FreeEvent = NewPromise();
    }

    return FreeEvent;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
