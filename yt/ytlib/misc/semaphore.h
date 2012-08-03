#pragma once

#include "common.h"
#include "thread_affinity.h"
#include <ytlib/actions/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Custom semaphore class with async acquire operation.
class TAsyncSemaphore
{
public:
    explicit TAsyncSemaphore(i64 maxFreeSlots);

    //! Increases the counter.
    void Release(i64 slots = 1);

    //! Decreases the counter.
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
    volatile i64 FreeSlotCount;

    TPromise<void> ReadyEvent;
    TPromise<void> FreeEvent;
    const TPromise<void> StaticResult;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
