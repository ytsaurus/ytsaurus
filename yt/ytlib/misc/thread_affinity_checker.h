#pragma once

#include "common.h"
#include "assert.h"

#include <util/system/thread.h>

namespace NYT {
namespace NThreadAffinity {

////////////////////////////////////////////////////////////////////////////////

class TSlot
{
public:
    TSlot()
        : ThreadId(UnsetThreadId)
    { }

    void Verify()
    {
        intptr_t currentThreadId = static_cast<intptr_t>(TThread::CurrentThreadId());
        if (ThreadId != UnsetThreadId) {
            YVERIFY(ThreadId == currentThreadId);
        } else {
            YVERIFY(AtomicCas(&ThreadId, currentThreadId, UnsetThreadId));
        }
    }

private:
    TAtomic ThreadId;
    const static int UnsetThreadId = -1;

};

#define DECLARE_THREAD_AFFINITY_SLOT(name) \
    ::NYT::NThreadAffinity::TSlot Slot__ ## name

#define THREAD_AFFINITY_ONLY(name)\
    Slot__ ## name.Verify()

////////////////////////////////////////////////////////////////////////////////

} // namespace NThreadAffinity
} // namespace NYT
