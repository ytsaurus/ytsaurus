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
    YASSERT(FreeSlotCount >= 0);
    while (true) {
        while (FreeSlotCount == 0) {
            FreeSlotExists.Wait();
        }
        if (AtomicCas(&FreeSlotCount, FreeSlotCount - 1, FreeSlotCount)) {
            return;    
        }
        SpinLockPause();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 