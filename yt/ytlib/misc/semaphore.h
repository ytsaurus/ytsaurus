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

/*  ToDo: Maybe later

    //! Tries to enter the semaphore gate. A non-blocking variant of #Acquire.
    //! Returns 'true' if the semaphore counter has decreased.
    bool TryAcquire();
*/

private:
    const int MaxFreeSlots;
    TAtomic FreeSlotCount;
    Event FreeSlotExists;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 