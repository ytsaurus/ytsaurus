#pragma once

#include "public.h"

#ifdef _linux_
    #include <unistd.h>
    #include <syscall.h>
    #include <linux/futex.h>
    #include <sys/time.h>
#else
    #include <util/system/mutex.h>
    #include <util/system/condvar.h>
#endif

#include <limits>

#include <util/system/atomic.h>

// This is an adapted version from Facebook's folly.
// See https://raw.github.com/facebook/folly/master/folly/experimental/EventCount.h

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

namespace NDetail {

inline int futex(
    int* uaddr, int op, int val, const timespec* timeout,
    int* uaddr2, int val3)
{
    return syscall(SYS_futex, uaddr, op, val, timeout, uaddr2, val3);
}

} // namespace NDetail

#endif

////////////////////////////////////////////////////////////////////////////////

/**
 * Event count: a condition variable for lock free algorithms.
 *
 * See http://www.1024cores.net/home/lock-free-algorithms/eventcounts for
 * details.
 *
 * Event counts allow you to convert a non-blocking lock-free / wait-free
 * algorithm into a blocking one, by isolating the blocking logic. You call
 * PrepareWait() before checking your condition and then either CancelWait()
 * or Wait() depending on whether the condition was true. When another
 * thread makes the condition true, it must call Notify() / NotifyAll() just
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
 *       auto key = ec.PepareWait();
 *       if (condition()) {
 *         ec.CancelWait();
 *         break;
 *       } else {
 *         ec.Wait(key);
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
{
public:
    TEventCount()
        : Epoch(0)
        , Waiters(0)
    { }

    class TCookie
    {
        friend class TEventCount;

        explicit TCookie(TAtomicBase epoch)
            : Epoch(epoch)
        { }

        TAtomicBase Epoch;
    };

    void Notify();
    void NotifyAll();
    TCookie PrepareWait();
    void CancelWait();
    void Wait(TCookie key);

    /**
     * Wait for condition() to become true. Will clean up appropriately if
     * condition() throws, and then rethrow.
     */
    template <class TCondition>
    void Await(TCondition condition);

private:
    void DoNotify(int n);

    TEventCount(const TEventCount&);
    TEventCount(TEventCount&&);
    TEventCount& operator=(const TEventCount&);
    TEventCount& operator=(TEventCount&&);

    // NB: You have to atomically work with these two.
    // XXX(sandello): Replace with std::atomic<int>.
    TAtomic Epoch;
    TAtomic Waiters;

#ifndef _linux_
    TCondVar ConditionVariable_;
    TMutex Mutex_;
#endif

};

inline void TEventCount::Notify()
{
    DoNotify(1);
}

inline void TEventCount::NotifyAll()
{
    DoNotify(std::numeric_limits<int>::max());
}

inline void TEventCount::DoNotify(int n)
{
    // The order is important: Epoch is incremented before Waiters is checked.
    // prepareWait() increments Waiters before checking Epoch, so it is
    // impossible to miss a wakeup.
#ifndef _linux_
    TGuard<TMutex> guard(Mutex_);
#endif

    AtomicIncrement(Epoch);
    if (AtomicGet(Waiters) != 0) {
#ifdef _linux_
        NDetail::futex((int*) &Epoch, FUTEX_WAKE_PRIVATE, n, nullptr, nullptr, 0);
#else
        if (n == 1) {
            ConditionVariable_.Signal();
        } else {
            ConditionVariable_.BroadCast();
        }   
#endif
    }
}

inline TEventCount::TCookie TEventCount::PrepareWait()
{
    AtomicIncrement(Waiters);
    return TCookie(AtomicGet(Epoch));
}

inline void TEventCount::CancelWait()
{
    AtomicDecrement(Waiters);
}

inline void TEventCount::Wait(TCookie key)
{
#ifdef _linux_
    while (AtomicGet(Epoch) == key.Epoch) {
        NDetail::futex((int*) &Epoch, FUTEX_WAIT_PRIVATE, key.Epoch, nullptr, nullptr, 0);
    }
#else
    TGuard<TMutex> guard(Mutex_);
    if (AtomicGet(Epoch) == key.Epoch) {
        ConditionVariable_.WaitI(Mutex_);
    }
#endif
    AtomicDecrement(Waiters);
}

template <class TCondition>
void TEventCount::Await(TCondition condition)
{
    if (condition()) {
        // Fast path.
        return;
    }

    // condition() is the only thing that may throw, everything else is
    // noexcept, so we can hoist the try/catch block outside of the loop
    try {
        for (;;) {
            auto key = PrepareWait();
            if (condition()) {
                CancelWait();
                break;
            } else {
                Wait(key);
            }
        }
    } catch (...) {
        CancelWait();
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
