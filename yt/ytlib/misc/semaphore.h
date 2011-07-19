#pragma once
#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Custom semaphore class on Events and Atomics
class TSemaphore {
public:
    TSemaphore(int maxFreeSlots);

    //! Increases the semaphore counter.
    //! Returns 'true' if the semaphore counter increased
    bool Release();

    //! Decreases counter
    //! Keeps a thread blocked while the semaphore counter is equal 0.
    void Acquire();

/*  ToDo: Maybe later

    //! Tries to enter the semaphore gate. A non-blocking variant of Acquire.
    //! Returns 'true' if the semaphore counter decreased
    bool TryAcquire();
*/

private:
    const int MaxFreeSlots;
    TAtomic NumFreeSlots;
    Event FreeSlotExists;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 