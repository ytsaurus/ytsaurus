#pragma once

#include "scheduler_thread.h"

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TEVSchedulerThread
    : public TSchedulerThread
{
public:
    TEVSchedulerThread(
        const Stroka& threadName,
        bool enableLogging);

    IInvokerPtr GetInvoker();

protected:
    class TInvoker
        : public IInvoker
    {
    public:
        explicit TInvoker(TEVSchedulerThread* owner);

        virtual void Invoke(const TClosure& callback) override;

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
        virtual TThreadId GetThreadId() const override;
        virtual bool CheckAffinity(IInvokerPtr invoker) const override;
#endif

    private:
        TEVSchedulerThread* Owner;

    };

    TEventCount CallbackEventCount; // fake

    ev::dynamic_loop EventLoop;
    ev::async CallbackWatcher;

    TIntrusivePtr<TInvoker> Invoker;
    TLockFreeQueue<TClosure> Queue;

    virtual void OnShutdown() override;

    virtual EBeginExecuteResult BeginExecute() override;
    virtual void EndExecute() override;

    EBeginExecuteResult BeginExecuteCallbacks();
    void OnCallback(ev::async&, int);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
