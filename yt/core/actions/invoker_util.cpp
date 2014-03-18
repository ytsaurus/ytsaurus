#include "stdafx.h"
#include "invoker_util.h"

#include <stack>

#include <core/misc/singleton.h>

#include <core/concurrency/fiber.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSyncInvoker
    : public IInvoker
{
public:
    virtual void Invoke(const TClosure& callback) override
    {
        callback.Run();
    }

    virtual NConcurrency::TThreadId GetThreadId() const override
    {
        return NConcurrency::InvalidThreadId;
    }
};

IInvokerPtr GetSyncInvoker()
{
    return RefCountedSingleton<TSyncInvoker>();
}

IInvokerPtr GetCurrentInvoker()
{
    return TFiber::GetCurrent()->GetCurrentInvoker();
}

void SetCurrentInvoker(IInvokerPtr invoker)
{
    TFiber::GetCurrent()->SetCurrentInvoker(std::move(invoker));
}


////////////////////////////////////////////////////////////////////////////////

void GuardedInvoke(
    IInvokerPtr invoker,
    TClosure onSuccess,
    TClosure onCancel)
{
    YASSERT(invoker);
    YASSERT(onSuccess);
    YASSERT(onCancel);

    class TGuard
    {
    public:
        explicit TGuard(TClosure onCancel)
            : OnCancel_(std::move(onCancel))
        { }

        TGuard(TGuard&& other)
            : OnCancel_(std::move(other.OnCancel_))
        { }

        ~TGuard()
        {
            if (OnCancel_) {
                OnCancel_.Run();
            }
        }

        void Release()
        {
            OnCancel_.Reset();
        }

    private:
        TClosure OnCancel_;

    };

    auto doInvoke = [] (TClosure onSuccess, TGuard guard) {
        guard.Release();
        onSuccess.Run();
    };

    invoker->Invoke(BIND(
        doInvoke,
        Passed(std::move(onSuccess)),
        Passed(TGuard(std::move(onCancel)))));
}

////////////////////////////////////////////////////////////////////////////////

TCurrentInvokerGuard::TCurrentInvokerGuard(IInvokerPtr newInvoker)
{
    OldInvoker = GetCurrentInvoker();
    SetCurrentInvoker(std::move(newInvoker));
}

TCurrentInvokerGuard::~TCurrentInvokerGuard()
{
    SetCurrentInvoker(std::move(OldInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
