#include "semaphore.h"

#include <util/system/yield.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
    
TSemaphore::TSemaphore(int maxFreeSlots)
    : MaxFreeSlots(maxFreeSlots)
    , FreeSlotCount(maxFreeSlots)
    , FreeSlotExists(Event::rAuto)
{ }

bool TSemaphore::Release()
{
    while (true) {
        if (FreeSlotCount < MaxFreeSlots) {
            if (AtomicCas(&FreeSlotCount, FreeSlotCount + 1, FreeSlotCount)) {
                FreeSlotExists.Signal();
                return true;
            }
        } else {
            return false;
        }
        SpinLockPause();
    }
}

void TSemaphore::Acquire()
{
    while (true) {
        if (!TryAcquire()) {
            FreeSlotExists.Wait();
        }
    }
}

bool TSemaphore::TryAcquire()
{
    YASSERT(FreeSlotCount >= 0);
    while (true) {
        if (FreeSlotCount == 0) {
            return false;
        }

        if (AtomicCas(&FreeSlotCount, FreeSlotCount - 1, FreeSlotCount)) {
            return true;
        }
        SpinLockPause();
    }
}

int TSemaphore::GetFreeSlotCount() const
{
    return FreeSlotCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 