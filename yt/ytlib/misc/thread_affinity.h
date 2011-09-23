#pragma once

#include "common.h"
#include "assert.h"

#include <util/system/thread.h>

namespace NYT {
namespace NThreadAffinity {

////////////////////////////////////////////////////////////////////////////////

/*!
 * Allows to annotate certain functions with thread affinity.
 * The checks are performed at run-time to ensure that each function
 * invocation that is annotated with a particular affinity slot
 * takes place in one thread.
 *
 * The usage is as follows.
 * - For each thread that may invoke your functions declare a slot with
 *   \code
 *   DECLARE_THREAD_AFFINITY_SLOT(ThreadName);
 *   \endcode
 * - Write
 *   \code
 *   VERIFY_THREAD_AFFINITY(ThreadName);
 *   \endcode
 *   at the beginning of each function in the group.
 *
 * Please refer to the unit test for an actual usage example
 * (unittests/thread_affinity_ut.cpp).
 */

// Check that the cast TThread::TId -> TAtomic is safe.
// NB: TAtomic is volatile intptr_t.
STATIC_ASSERT(sizeof(TThread::TId) == sizeof(intptr_t));

class TSlot
{
public:
    TSlot()
        : InvalidId(static_cast<intptr_t>(TThread::ImpossibleThreadId()))
        , BoundId(InvalidId)
    { }

    void Check()
    {
        intptr_t currentThreadId = static_cast<intptr_t>(TThread::CurrentThreadId());
        intptr_t boundThreadId = BoundId;
        if (boundThreadId != InvalidId) {
            YVERIFY(boundThreadId == currentThreadId);
        } else {
            YVERIFY(AtomicCas(&BoundId, currentThreadId, InvalidId));
        }
    }

private:
    intptr_t InvalidId;
    TAtomic BoundId;

};

#ifdef ENABLE_THREAD_AFFINITY_CHECK

#define DECLARE_THREAD_AFFINITY_SLOT(name) \
    mutable ::NYT::NThreadAffinity::TSlot name ## __Slot

#define VERIFY_THREAD_AFFINITY(name)\
    name ## __Slot.Check()

#else

// Expand macros to null but take care about trailing semicolon.
#define DECLARE_THREAD_AFFINITY_SLOT(name) struct TNullThreadAffinitySlot__ ## _LINE_ { }
#define VERIFY_THREAD_AFFINITY(name)       do { } while(0)

#endif
////////////////////////////////////////////////////////////////////////////////

} // namespace NThreadAffinity
} // namespace NYT
