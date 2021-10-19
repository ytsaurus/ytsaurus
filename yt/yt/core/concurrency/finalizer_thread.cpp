#include "finalizer_thread.h"
#include "action_queue.h"
#include "profiling_helpers.h"
#include "single_queue_scheduler_thread.h"

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/shutdown.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TFinalizerManager
{
public:
    static TFinalizerManager* Get()
    {
        return LeakySingleton<TFinalizerManager>();
    }

    const IInvokerPtr& GetInvoker()
    {
        return Invoker_;
    }

private:
    const TIntrusivePtr<TEventCount> CallbackEventCount_ = New<TEventCount>();
    const TString ThreadName_{"Finalizer"};
    const TMpscInvokerQueuePtr Queue_;
    const IInvokerPtr Invoker_;
    const TMpscSingleQueueSchedulerThreadPtr Thread_;
    const TShutdownCookie ShutdownCookie_;


    TFinalizerManager()
        : Queue_(New<TMpscInvokerQueue>(
            CallbackEventCount_,
            GetThreadTags(ThreadName_)))
        , Invoker_(Queue_)
        , Thread_(New<TMpscSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            ThreadName_,
            ThreadName_,
            /*shutdownPriority*/ -201))
        , ShutdownCookie_(RegisterShutdownCallback(
            "FinalizerManager",
            BIND(&TFinalizerManager::Shutdown, this),
            /*priority*/ -200))
    {
        Thread_->Start();
    }

    void Shutdown()
    {
        Thread_->Stop(/*graceful*/ true);
        Queue_->Shutdown();
    }

    DECLARE_LEAKY_SINGLETON_FRIEND()
};

const IInvokerPtr& GetFinalizerInvoker()
{
    return TFinalizerManager::Get()->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

