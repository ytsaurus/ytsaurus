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
        YT_VERIFY(expectedId == threadId);
    }
}

void TThreadAffinitySlot::Check()
{
    Check(TThread::CurrentThreadId());
}

bool VerifyInvokerAffinity(const IInvokerPtr& invoker)
{
    auto currentInvoker = GetCurrentInvoker();
    return
        currentInvoker->CheckAffinity(invoker) ||
        invoker->CheckAffinity(currentInvoker);
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

