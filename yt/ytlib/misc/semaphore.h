#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Custom semaphore class based on Event and CAS operations.
/*!
 *  This class was brought into existence primarily because of instability of util's analogue.
 */
class TSemaphore
{
public:
    explicit TSemaphore(int maxFreeSlots);

    //! Decreases the counter.
    /*!
     *  Keeps a thread blocked while the semaphore counter is 0.
     */
    void Acquire();

    //! Increases the counter.
    /*!
     *  Returns 'true' if the semaphore counter has increased.
     */
    bool Release();

    //! Tries to enter the semaphore gate. A non-blocking variant of #Acquire.
    //! Returns 'true' if the semaphore counter has decreased.
    bool TryAcquire();

    //! Returns the current number of free slots in the semaphore.
    /*!
     *  The returned value is only a snapshot of the counter kept by the semaphore.
     *  The caller must make sure that no one calls #Acquire, #Release or #TryAcquire
     *  simultaneously.
     */
    int GetFreeSlotCount() const;

private:
    const int MaxFreeSlots;
    TAtomic FreeSlotCount;
    Event FreeSlotExists;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 