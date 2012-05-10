#pragma once

#include "common.h"
#include "thread_affinity.h"
#include <ytlib/actions/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Custom semaphore class with async acquire operation.
/*!
 *  Can be used by a single acquire thread and possibly multiple 
 *  release threads. The acquirer must not call AsyncAquire again, 
 *  until returned future is set.
 */
class TAsyncSemaphore
{
public:
    explicit TAsyncSemaphore(i64 maxFreeSlots);

    //! Increases the counter.
    /*!
     *  Returns 'true' if the semaphore counter has increased.
     */
    void Release(i64 slots = 1);

    /*!
     *  Must be called from single thread.
     *  The client must not call AsyncAquire again, until returned
     *  future is set.
     */
    TFuture<void> AsyncAcquire(i64 slots = 1);

private:
    TSpinLock SpinLock;

    const i64 MaxFreeSlots;
    i64 FreeSlotCount;
    i64 RequestedSlots;

    TPromise<void> AcquireEvent;
    TPromise<void> StaticResult;
    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT 