#pragma once

#include "public.h"
#include "invoker.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/profiling/sensor.h>

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

//! A helper base which makes callbacks track their invocation time and profile their wait time.
class TInvokerProfileWrapper
{
public:
    /*! #invokerFamily defines a family of invokers, e.g. "serialized" or "prioriized" and appears in the sensor name.
    *  #invokerName defines a particular instance of the invoker and gets into the tag "invoker".
    */
    TInvokerProfileWrapper(NProfiling::IRegistryImplPtr registry, const TString& invokerFamily, const TString& invokerName);

protected:
    TClosure WrapCallback(TClosure callback);

private:
    NProfiling::TEventTimer WaitTimer_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
