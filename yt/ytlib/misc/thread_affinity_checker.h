#pragma once

#include "common.h"
#include "assert.h"

#include <util/system/thread.h>

namespace NYT {
namespace NThreadAffinity {

////////////////////////////////////////////////////////////////////////////////

class TSlot
{
private:
    TAtomic ThreadId;
    const static int UnsetThreadId = -1;

public:
    TSlot():
        ThreadId(UnsetThreadId)
    { }

    void Verify() {
        TAtomic currentThreadId = static_cast<TAtomic>(TThread::CurrentThreadId());
        if (ThreadId != UnsetThreadId) {
            YVERIFY(ThreadId == currentThreadId);
            return;
        }
        YVERIFY(AtomicCas(&ThreadId, currentThreadId, UnsetThreadId));
    }
};

#define DECLARE_THREAD_AFFINITY_SLOT(name) \
    ::NYT::NThreadAffinity::TSlot Slot__ ## name

#define THREAD_AFFINITY_ONLY(name)\
    Slot__ ## name.Verify()

////////////////////////////////////////////////////////////////////////////////

}; // namespace NThreadAffinity
} // namespace NYT
