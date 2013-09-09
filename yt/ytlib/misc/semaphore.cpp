#include "stdafx.h"
#include "semaphore.h"
#include <ytlib/misc/nullable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static TFuture<void> PresetResult = MakePromise();

TAsyncSemaphore::TAsyncSemaphore(i64 maxFreeSlots)
    : MaxFreeSlots(maxFreeSlots)
    , FreeSlotCount(maxFreeSlots)
{
    YCHECK(maxFreeSlots > 0);
}

void TAsyncSemaphore::Release(i64 slots /* = 1 */)
{
    TPromise<void> ready;
    TPromise<void> free;

    {
        TGuard<TSpinLock> guard(SpinLock);

        FreeSlotCount += slots;
        YASSERT(FreeSlotCount <= MaxFreeSlots);

        if (ReadyEvent && FreeSlotCount > 0) {
            ready.Swap(ReadyEvent);
        }

        if (FreeEvent && FreeSlotCount == MaxFreeSlots) {
            free.Swap(FreeEvent);
        }
    }

    if (ready) {
        ready.Set();
    }

    if (free) {
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
        YCHECK(!ReadyEvent);
        return PresetResult;
    } else if (!ReadyEvent) {
        ReadyEvent = NewPromise();
    }

    return ReadyEvent;
}

TFuture<void> TAsyncSemaphore::GetFreeEvent()
{
    TGuard<TSpinLock> guard(SpinLock);

    if (FreeSlotCount == MaxFreeSlots) {
        YCHECK(!FreeEvent);
        return PresetResult;
    } else if (!FreeEvent) {
        FreeEvent = NewPromise();
    }

    return FreeEvent;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
