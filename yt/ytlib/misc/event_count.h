#pragma once

#include "common.h"

#ifdef _linux_
#include <unistd.h>
#include <syscall.h>
#include <linux/futex.h>
#include <sys/time.h>
#else
static const int FUTEX_WAIT = 0;
static const int FUTEX_WAKE = 0;
#endif

#include <limits>

#include <util/system/atomic.h>

// This is adapted version from Facebook's folly.
// See https://raw.github.com/facebook/folly/master/folly/experimental/EventCount.h

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

inline int futex(
    int* uaddr, int op, int val, const timespec* timeout,
    int* uaddr2, int val3) noexcept
{
#ifdef _linux_
    return syscall(SYS_futex, uaddr, op, val, timeout, uaddr2, val3);
#else
    YUNIMPLEMENTED();
#endif
}

} // namespace detail

////////////////////////////////////////////////////////////////////////////////

/**
 * Event count: a condition variable for lock free algorithms.
 *
 * See http://www.1024cores.net/home/lock-free-algorithms/eventcounts for
 * details.
 *
 * Event counts allow you to convert a non-blocking lock-free / wait-free
 * algorithm into a blocking one, by isolating the blocking logic. You call
 * prepareWait() before checking your condition and then either cancelWait()
 * or wait() depending on whether the condition was true. When another
 * thread makes the condition true, it must call notify() / notifyAll() just
 * like a regular condition variable.
 *
 * Let "<" denote the happens-before relationship.
 * Consider 2 threads (T1 and T2) and 3 events:
 * - E1: T1 returns from prepareWait
 * - E2: T1 calls wait (obviously E1 < E2, intra-thread)
 * - E3: T2 calls notifyAll
 *
 * If E1 < E3, then E2's wait will complete (and T1 will either wake up,
 * or not block at all)
 *
 * This means that you can use an EventCount in the following manner:
 *
 * Waiter:
 *   if (!condition()) { // Handle fast path first.
 *     for (;;) {
 *       auto key = eventCount.prepareWait();
 *       if (condition()) {
 *         eventCount.cancelWait();
 *         break;
 *       } else {
 *         eventCount.wait(key);
 *       }
 *     }
 *  }
 *
 *  (This pattern is encapsulated in await())
 *
 * Poster:
 *   make_condition_true();
 *   eventCount.notifyAll();
 *
 * Note that, just like with regular condition variables, the waiter needs to
 * be tolerant of spurious wakeups and needs to recheck the condition after
 * being woken up.  Also, as there is no mutual exclusion implied, "checking"
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
    void DoNotify(TAtomicBase n);

    TEventCount(const TEventCount&);
    TEventCount(TEventCount&&);
    TEventCount& operator=(const TEventCount&);
    TEventCount& operator=(TEventCount&&);

    // NB: You have to atomically work with these two.
    TAtomic Epoch; 
    TAtomic Waiters;
};

inline void TEventCount::Notify()
{
    DoNotify(1);
}

inline void TEventCount::NotifyAll()
{
    DoNotify(std::numeric_limits<TAtomicBase>::max());
}

inline void TEventCount::DoNotify(TAtomicBase n)
{
    // The order is important: Epoch is incremented before Waiters is checked.
    // prepareWait() increments Waiters before checking Epoch, so it is
    // impossible to miss a wakeup.
    AtomicIncrement(Epoch);
    if (AtomicGet(Waiters) != 0) {
        NDetail::futex((int*)(&Epoch), FUTEX_WAKE, n, NULL, NULL, 0);
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
    while (AtomicGet(Epoch) == key.Epoch) {
        NDetail::futex((int*)(&Epoch), FUTEX_WAIT, key.Epoch, NULL, NULL, 0);
    }
    AtomicDecrement(Waiters);
}

template <class TCondition>
void TEventCount::Await(TCondition condition) {
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

} // namespace NYT
