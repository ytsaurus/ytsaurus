#include "semaphore.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////


TSemaphore::TSemaphore(int maxFreeSlots)
    : MaxFreeSlots(maxFreeSlots)
    , NumFreeSlots(maxFreeSlots)
    , FreeSlotExists(Event::rAuto)
{ }

 
bool TSemaphore::Release()
{
    while (true) {
        if (NumFreeSlots < MaxFreeSlots) {
            if (AtomicCas(&NumFreeSlots, NumFreeSlots, NumFreeSlots + 1)) {
                FreeSlotExists.Signal();
                return true;
            }
        } else {
            return false;
        }
    }
}

void TSemaphore::Acquire()
{
    YASSERT(NumFreeSlots >= 0);
    while (true) {
        while (NumFreeSlots == 0) {
            FreeSlotExists.Wait();
        }
        if (AtomicCas(&NumFreeSlots, NumFreeSlots, NumFreeSlots - 1)) {
            return;    
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 