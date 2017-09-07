#pragma once

#include "scheduler_thread.h"

#include <yt/contrib/libev/ev++.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TEVSchedulerThread
    : public TSchedulerThread
{
public:
    TEVSchedulerThread(
        const TString& threadName,
        bool enableLogging);

    const IInvokerPtr& GetInvoker();

protected:
    class TInvoker
        : public IInvoker
    {
    public:
        explicit TInvoker(TEVSchedulerThread* owner);

        virtual void Invoke(TClosure callback) override;

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
        virtual TThreadId GetThreadId() const override;
        virtual bool CheckAffinity(const IInvokerPtr& invoker) const override;
#endif

    private:
        TEVSchedulerThread* const Owner_;
    };

    ev::dynamic_loop EventLoop_;
    ev::async CallbackWatcher_;

    const IInvokerPtr Invoker_;
    TLockFreeQueue<TClosure> Queue_;

    virtual void BeforeShutdown() override;
    virtual void AfterShutdown() override;

    virtual EBeginExecuteResult BeginExecute() override;
    virtual void EndExecute() override;

    EBeginExecuteResult BeginExecuteCallbacks();
    void OnCallback(ev::async&, int);

    void EnqueueCallback(TClosure callback);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
