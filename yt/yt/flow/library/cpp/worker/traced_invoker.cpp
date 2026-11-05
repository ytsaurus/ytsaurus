#include "traced_invoker.h"

#include <yt/yt/flow/library/cpp/misc/cpu_accounting.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/actions/current_invoker.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

void TJobCpuTimeAccountant::Add(ui64 microseconds)
{
    Microseconds_.fetch_add(microseconds, std::memory_order::relaxed);
}

TDuration TJobCpuTimeAccountant::GetCpuTime() const
{
    return TDuration::MicroSeconds(Microseconds_.load(std::memory_order::relaxed));
}

////////////////////////////////////////////////////////////////////////////////

class TTracedInvoker
    : public TInvokerWrapper<false>
{
public:
    TTracedInvoker(
        IInvokerPtr underlyingInvoker,
        TTraceContextPtr traceContext,
        TJobCpuTimeAccountantPtr cpuTimeAccountant)
        : TInvokerWrapper(std::move(underlyingInvoker))
        , TraceContext_(std::move(traceContext))
        , CpuTimeAccountant_(std::move(cpuTimeAccountant))
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
    const TJobCpuTimeAccountantPtr CpuTimeAccountant_;

    void RunCallback(TClosure callback)
    {
        TCurrentTraceContextGuard traceContextGuard(TraceContext_);

        RunWithCpuAccounting(callback, [this] (ui64 delta) {
            CpuTimeAccountant_->Add(delta);
        });
    }
};

IInvokerPtr CreateTracedInvoker(
    IInvokerPtr underlyingInvoker,
    TTraceContextPtr traceContext,
    TJobCpuTimeAccountantPtr cpuTimeAccountant)
{
    return New<TTracedInvoker>(
        std::move(underlyingInvoker),
        std::move(traceContext),
        std::move(cpuTimeAccountant));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
