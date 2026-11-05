#pragma once

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/tracing/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJobCpuTimeAccountant)

//! Accumulates real thread CPU time consumed by a job's callbacks. Reads the per-thread
//! CPU clock at fiber suspend/resume boundaries, so neither fiber suspension nor OS
//! preemption of the running thread is charged as CPU — unlike the trace context's
//! wall-clock elapsed time.
class TJobCpuTimeAccountant
    : public TRefCounted
{
public:
    void Add(ui64 microseconds);
    TDuration GetCpuTime() const;

private:
    std::atomic<ui64> Microseconds_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TJobCpuTimeAccountant)

////////////////////////////////////////////////////////////////////////////////

IInvokerPtr CreateTracedInvoker(
    IInvokerPtr underlyingInvoker,
    NTracing::TTraceContextPtr traceContext,
    TJobCpuTimeAccountantPtr cpuTimeAccountant);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
