#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

#include <yt/core/misc/preprocessor.h>

#include <atomic>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK

/*!
 * Allows to annotate certain functions with thread affinity.
 * The checks are performed at run-time to ensure that each function
 * invocation that is annotated with a particular affinity slot
 * takes place in one thread.
 *
 * The usage is as follows.
 * - For each thread that may invoke your functions declare a slot with
 *   \code
 *   DECLARE_THREAD_AFFINITY_SLOT(Thread);
 *   \endcode
 * - Write
 *   \code
 *   VERIFY_THREAD_AFFINITY(Thread);
 *   \endcode
 *   at the beginning of each function in the group.
 *
 * Please refer to the unit test for an actual usage example
 * (unittests/thread_affinity_ut.cpp).
 */
class TThreadAffinitySlot
{
public:
    //! Checks if the slot matches the given thread id.
    void Check(TThreadId threadId);

    //! Checks if the slot matches the current thread id.
    void Check();

private:
    std::atomic<TThreadId> BoundId_ = {InvalidThreadId};

};

#define DECLARE_THREAD_AFFINITY_SLOT(slot) \
    mutable ::NYT::NConcurrency::TThreadAffinitySlot PP_CONCAT(slot, _Slot)

#define VERIFY_THREAD_AFFINITY(slot) \
    PP_CONCAT(slot, _Slot).Check()

#define VERIFY_SPINLOCK_AFFINITY(spinLock) \
    YCHECK((spinLock).IsLocked());

#define VERIFY_INVOKER_AFFINITY(invoker) \
    YCHECK(::NYT::NConcurrency::VerifyInvokerAffinity(invoker))

#define VERIFY_INVOKERS_AFFINITY(invokers) \
    YCHECK(::NYT::NConcurrency::VerifyInvokersAffinity(invokers))

#define VERIFY_INVOKER_THREAD_AFFINITY(invoker, slot) \
    PP_CONCAT(slot, _Slot).Check((invoker)->GetThreadId());

#else

// Expand macros to null but take care of the trailing semicolon.
#define DECLARE_THREAD_AFFINITY_SLOT(slot)               struct PP_CONCAT(TNullThreadAffinitySlot_,  __LINE__) { }
#define VERIFY_THREAD_AFFINITY(slot)                     do { } while (false)
#define VERIFY_SPINLOCK_AFFINITY(spinLock)               do { } while (false)
#define VERIFY_INVOKER_AFFINITY(invoker)                 do { } while (false)
#define VERIFY_INVOKERS_AFFINITY(invokers)               do { } while (false)
#define VERIFY_INVOKER_THREAD_AFFINITY(invoker, slot)    do { } while (false)

#endif

//! This is a mere declaration and intentionally does not check anything.
#define VERIFY_THREAD_AFFINITY_ANY() do { } while (false)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define THREAD_AFFINITY_INL_H_
#include "thread_affinity-inl.h"
#undef THREAD_AFFINITY_INL_H_
