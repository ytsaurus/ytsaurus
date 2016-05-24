#pragma once

#include "public.h"
#include "invoker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TInvokerWrapper
    : public virtual IInvoker
{
public:
    //! Schedules invocation of a given callback.
    virtual void Invoke(const TClosure& callback) override;

#ifdef YT_ENABLE_THREAD_AFFINITY_CHECK
    virtual NConcurrency::TThreadId GetThreadId() const override;
    virtual bool CheckAffinity(IInvokerPtr invoker) const override;
#endif

protected:
    explicit TInvokerWrapper(IInvokerPtr underlyingInvoker);

    IInvokerPtr UnderlyingInvoker_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
