#pragma once

#include "common.h"
#include "assert.h"

#include <util/system/thread.h>

namespace NYT {
namespace NThreadAffinity {

////////////////////////////////////////////////////////////////////////////////

/*!
 * This module is designed to provide the ability of checking the unuqieness
 * of thread calling particulary function.
 *
 * Usage is as following:
 * - For each thread you should write macros #DECLARE_THREAD_AFFINITY_SLOT(ThreadName).
 * - Then in functions that should be called from particular thread use macros
 * #VERIFY_THREAD_AFFINITY(ThreadName) at the beginning.
 *
 * Please refer to the unit test for an actual example of usage
 * (unittests/thread_affinity_ut.cpp).
 */


// check that cast TThread::TId -> intptr_t is safe
STATIC_ASSERT(sizeof(TThread::TId) == sizeof(intptr_t));

class TSlot
{
public:
    TSlot()
    {
        ImpossibleThreadId = static_cast<intptr_t>(TThread::ImpossibleThreadId());
    }

    void Check()
    {
        intptr_t currentThreadId = static_cast<intptr_t>(TThread::CurrentThreadId());
        if (ThreadId != ImpossibleThreadId) {
            YVERIFY(ThreadId == currentThreadId);
        } else {
            YVERIFY(AtomicCas(&ThreadId, currentThreadId, ImpossibleThreadId));
        }
    }

private:
    TAtomic ThreadId;
    intptr_t ImpossibleThreadId;
};

#ifdef ENABLE_THREAD_AFFINITY_CHECK

#define DECLARE_THREAD_AFFINITY_SLOT(name) \
    ::NYT::NThreadAffinity::TSlot Slot__ ## name

#define VERIFY_THREAD_AFFINITY(name)\
    Slot__ ## name.Check()

#else

#define DECLARE_THREAD_AFFINITY_SLOT
#define VERIFY_THREAD_AFFINITY

#endif
////////////////////////////////////////////////////////////////////////////////

} // namespace NThreadAffinity
} // namespace NYT
