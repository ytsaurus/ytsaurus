#pragma once

#include "public.h"

#ifdef _linux_
    #include <linux/futex.h>
    #include <sys/time.h>
    #include <sys/syscall.h>
#else
    #include <util/system/mutex.h>
    #include <util/system/condvar.h>
#endif

#include <limits>
#include <atomic>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Event count: a condition variable for lock free algorithms.
/*!
 * This is an adapted version from Facebook's Folly. See
 * https://raw.github.com/facebook/folly/master/folly/experimental/EventCount.h
 * http://www.1024cores.net/home/lock-free-algorithms/eventcounts
 * for details.
 *
 * Event counts allow you to convert a non-blocking lock-free / wait-free
 * algorithm into a blocking one, by isolating the blocking logic. You call
 * PrepareWait() before checking your condition and then either CancelWait()
 * or Wait() depending on whether the condition was true. When another
 * thread makes the condition true, it must call NotifyOne() / NotifyAll() just
 * like a regular condition variable.
 *
 * Let "<" denote the happens-before relationship.
 * Consider 2 threads (T1 and T2) and 3 events:
 * - E1: T1 returns from PrepareWait
 * - E2: T1 calls Wait (obviously E1 < E2, intra-thread)
 * - E3: T2 calls NotifyAll
 *
 * If E1 < E3, then E2's Wait() will complete (and T1 will either wake up,
 * or not block at all)
 *
 * This means that you can use an EventCount in the following manner:
 *
 * Waiter:
 *   if (!condition()) { // Handle fast path first.
 *     for (;;) {
 *       auto cookie = ec.PepareWait();
 *       if (condition()) {
 *         ec.CancelWait();
 *         break;
 *       } else {
 *         ec.Wait(cookie);
 *       }
 *     }
 *  }
 *
 *  (This pattern is encapsulated in Await())
 *
 * Poster:
 *   ... make condition true...
 *   ec.NotifyAll();
 *
 * Note that, just like with regular condition variables, the waiter needs to
 * be tolerant of spurious wakeups and needs to recheck the condition after
 * being woken up. Also, as there is no mutual exclusion implied, "checking"
 * the condition likely means attempting an operation on an underlying
 * data structure (push into a lock-free queue, etc) and returning true on
 * success and false on failure.
 */
class TEventCount
    : private TNonCopyable
{
public:
    TEventCount();

    class TCookie
    {
        friend class TEventCount;

        explicit TCookie(ui32 epoch)
            : Epoch_(epoch)
        { }

        ui32 Epoch_;
    };

    void NotifyOne();
    void NotifyAll();

    TCookie PrepareWait();
    void CancelWait();
    void Wait(TCookie cookie);

    //! Wait for |condition()| to become |true|.
    //! Will clean up appropriately if |condition()| throws, and then rethrow.
    template <class TCondition>
    void Await(TCondition condition);

private:
    void DoNotify(int n);

    //! Lower 32 bits: number of waiters.
    //! Upper 32 bits: epoch
    std::atomic<ui64> Value_;

    static const ui64 AddWaiter  = static_cast<ui64>(1);
    static const ui64 SubWaiter  = static_cast<ui64>(-1);

    static const int  EpochShift = 32;
    static const ui64 AddEpoch   = static_cast<ui64>(1) << EpochShift;
    
    static const ui64 WaiterMask = AddEpoch - 1;

#ifndef _linux_
    TCondVar ConditionVariable_;
    TMutex Mutex_;
#endif

};

////////////////////////////////////////////////////////////////////////////////

//! A single-shot non-resettable event implemented on top of TEventCount.
class TEvent
    : private TNonCopyable
{
public:
    TEvent();

    void NotifyOne();
    void NotifyAll();

    void Wait();

private:
    std::atomic<bool> Set_;
    TEventCount EventCount_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define EVENT_COUNT_INL_H_
#include "event_count-inl.h"
#undef EVENT_COUNT_INL_H_
