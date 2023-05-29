#pragma once

#include "public.h"
#include "invoker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TInvokerWrapper
    : public virtual IInvoker
{
public:
    void Invoke(TClosure callback) override;

    void Invoke(TMutableRange<TClosure> callbacks) override;

    NThreading::TThreadId GetThreadId() const override;
    bool CheckAffinity(const IInvokerPtr& invoker) const override;
    bool IsSerialized() const override;

protected:
    explicit TInvokerWrapper(IInvokerPtr underlyingInvoker);

    IInvokerPtr UnderlyingInvoker_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
