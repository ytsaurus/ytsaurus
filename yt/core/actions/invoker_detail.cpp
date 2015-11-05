#include "stdafx.h"
#include "invoker_detail.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TInvokerWrapper::TInvokerWrapper(IInvokerPtr underlyingInvoker)
    : UnderlyingInvoker_(std::move(underlyingInvoker))
{
    YCHECK(UnderlyingInvoker_);
}

void TInvokerWrapper::Invoke(const TClosure& callback)
{
    return UnderlyingInvoker_->Invoke(callback);
}

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
NConcurrency::TThreadId TInvokerWrapper::GetThreadId() const
{
    return UnderlyingInvoker_->GetThreadId();
}

bool TInvokerWrapper::CheckAffinity(IInvokerPtr invoker) const
{
    return
        invoker.Get() == this ||
        UnderlyingInvoker_->CheckAffinity(invoker);
}
#endif

IInvokerPtr IPrioritizedInvoker::BindPriority(i64 boundPriority)
{
    class TBoundPrioritizedInvoker final
        : public IInvoker
    {
    public:
        TBoundPrioritizedInvoker(IPrioritizedInvokerPtr invoker, i64 priority) noexcept
            : Invoker_(std::move(invoker))
            , Priority_(priority)
        { }

        virtual void Invoke(const TClosure& callback) override
        {
            Invoker_->Invoke(callback, Priority_);
        }

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
        virtual NConcurrency::TThreadId GetThreadId() const
        {
            return Invoker_->GetThreadId();
        }

        virtual bool CheckAffinity(IInvokerPtr invoker) const
        {
            return invoker.Get() == this || Invoker_->CheckAffinity(invoker);
        }
#endif

    private:
        const IPrioritizedInvokerPtr Invoker_;
        const i64 Priority_;
    };

    return New<TBoundPrioritizedInvoker>(MakeStrong(this), boundPriority);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
