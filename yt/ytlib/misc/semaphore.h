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

    void Acquire(i64 slots = 1);

    /*!  
     *  Quick check without guard.
     */
    bool IsReady() const;

    TFuture<void> GetReadyEvent();
    TFuture<void> GetFreeEvent();

private:
    TSpinLock SpinLock;

    const i64 MaxFreeSlots;
    i64 FreeSlotCount;

    TPromise<void> ReadyEvent;
    TPromise<void> FreeEvent;
    const TPromise<void> StaticResult;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
