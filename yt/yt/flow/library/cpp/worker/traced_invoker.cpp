#include "traced_invoker.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/actions/current_invoker.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/misc/finally.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

class TTracedInvoker
    : public TInvokerWrapper<false>
{
public:
    TTracedInvoker(
        IInvokerPtr underlyingInvoker,
        TTraceContextPtr traceContext)
        : TInvokerWrapper(std::move(underlyingInvoker))
        , TraceContext_(std::move(traceContext))
    { }

    using TInvokerWrapper::Invoke;

    void Invoke(TClosure callback) override
    {
        UnderlyingInvoker_->Invoke(BIND_NO_PROPAGATE(
            &TTracedInvoker::RunCallback,
            MakeStrong(this),
            std::move(callback)));
    }

private:
    const TTraceContextPtr TraceContext_;

    void RunCallback(TClosure callback)
    {
        TCurrentTraceContextGuard traceContextGuard(TraceContext_);
        callback();
    }
};

IInvokerPtr CreateTracedInvoker(
    IInvokerPtr underlyingInvoker,
    TTraceContextPtr traceContext)
{
    return New<TTracedInvoker>(
        std::move(underlyingInvoker),
        std::move(traceContext));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
