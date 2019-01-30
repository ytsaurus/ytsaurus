#include "thread_affinity.h"

#include <yt/core/actions/invoker_util.h>
#include <yt/core/actions/invoker_pool.h>
#include <yt/core/actions/invoker.h>

#include <util/system/thread.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK

void TThreadAffinitySlot::Check(TThreadId threadId)
{
    auto expectedId = InvalidThreadId;
    if (!BoundId_.compare_exchange_strong(expectedId, threadId)) {
        YCHECK(expectedId == threadId);
    }
}

void TThreadAffinitySlot::Check()
{
    Check(TThread::CurrentThreadId());
}

bool VerifyInvokerAffinity(const IInvokerPtr& invoker)
{
    return GetCurrentInvoker()->CheckAffinity(invoker);
}

bool VerifyInvokerPoolAffinity(const IInvokerPoolPtr& invokerPool)
{
    for (int index = 0; index < invokerPool->GetSize(); ++index) {
        if (VerifyInvokerAffinity(invokerPool->GetInvoker(index))) {
            return true;
        }
    }
    return false;
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

