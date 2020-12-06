#include "thread_pool.h"
#include "single_queue_scheduler_thread.h"
#include "private.h"
#include "profiling_helpers.h"
#include "thread_pool_detail.h"

#include <yt/core/actions/invoker_detail.h>

#include <yt/core/ypath/token.h>

namespace NYT::NConcurrency {

using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TThreadPool::TImpl
    : public TRefCounted
    , public TThreadPoolBase
{
public:
    TImpl(
        int threadCount,
        const TString& threadNamePrefix,
        bool enableLogging,
        bool enableProfiling)
        : TThreadPoolBase(
            threadCount,
            threadNamePrefix,
            enableLogging,
            enableProfiling)
        , Queue_(New<TMpmcInvokerQueue>(
            CallbackEventCount_,
            GetThreadTags(EnableProfiling_, ThreadNamePrefix_),
            EnableLogging_,
            EnableProfiling_))
        , Invoker_(Queue_)
    {
        Configure(threadCount);
    }

    ~TImpl()
    {
        Shutdown();
    }

    const IInvokerPtr& GetInvoker()
    {
        EnsureStarted();
        return Invoker_;
    }

private:
    const std::shared_ptr<TEventCount> CallbackEventCount_ = std::make_shared<TEventCount>();
    const TMpmcInvokerQueuePtr Queue_;
    const IInvokerPtr Invoker_;


    virtual void DoShutdown() override
    {
        Queue_->Shutdown();
        TThreadPoolBase::DoShutdown();
    }

    virtual TClosure MakeFinalizerCallback() override
    {
        return BIND([queue = Queue_, callback = TThreadPoolBase::MakeFinalizerCallback()] {
            callback();
            queue->Drain();
        });
    }

    virtual TSchedulerThreadPtr SpawnThread(int index) override
    {
        return New<TMpmcSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            MakeThreadName(index),
            GetThreadTags(EnableProfiling_, ThreadNamePrefix_),
            EnableLogging_,
            EnableProfiling_);
    }
};

TThreadPool::TThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    bool enableLogging,
    bool enableProfiling)
    : Impl_(New<TImpl>(
        threadCount,
        threadNamePrefix,
        enableLogging,
        enableProfiling))
{ }

TThreadPool::~TThreadPool() = default;

void TThreadPool::Shutdown()
{
    return Impl_->Shutdown();
}

void TThreadPool::Configure(int threadCount)
{
    return Impl_->Configure(threadCount);
}

const IInvokerPtr& TThreadPool::GetInvoker()
{
    return Impl_->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

