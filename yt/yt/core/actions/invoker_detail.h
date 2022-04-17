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
    void Invoke(TClosure callback) override;

    NConcurrency::TThreadId GetThreadId() const override;
    bool CheckAffinity(const IInvokerPtr& invoker) const override;

protected:
    explicit TInvokerWrapper(IInvokerPtr underlyingInvoker);

    IInvokerPtr UnderlyingInvoker_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
