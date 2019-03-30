#pragma once
#ifndef TRACE_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include trace_context.h"
// For the sake of sane code completion.
#include "trace_context.h"
#endif

#include <util/system/tls.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TTraceContext::TTraceContext()
    : TraceId_(InvalidTraceId)
    , SpanId_(InvalidSpanId)
    , ParentSpanId_(InvalidSpanId)
{
    Y_ASSERT(TraceId_ != InvalidTraceId);
}

Y_FORCE_INLINE TTraceContext::TTraceContext(
    TTraceId traceId,
    TSpanId spanId,
    TSpanId parentSpanId)
    : TraceId_(traceId)
    , SpanId_(spanId)
    , ParentSpanId_(parentSpanId)
{ }

Y_FORCE_INLINE bool TTraceContext::IsVerbose() const
{
    return NTracing::IsVerbose(TraceId_);
}

Y_FORCE_INLINE void TTraceContext::IncrementElapsedCpuTime(NProfiling::TCpuDuration delta)
{
    ElapsedCpuTime_ += delta;
}

Y_FORCE_INLINE NProfiling::TCpuDuration TTraceContext::GetElapsedCpuTime() const
{
    return ElapsedCpuTime_.load(std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TTraceContextGuard::TTraceContextGuard(TTraceContextPtr context)
    : Active_(context.operator bool())
{
    if (Y_UNLIKELY(Active_)) {
        OldContext_ = SwitchTraceContext(std::move(context));
    }
}

Y_FORCE_INLINE TTraceContextGuard::TTraceContextGuard(TTraceContextGuard&& other)
    : Active_(other.Active_)
    , OldContext_(std::move(other.OldContext_   ))
{
    other.Active_ = false;
}

Y_FORCE_INLINE TTraceContextGuard::~TTraceContextGuard()
{
    Release();
}

Y_FORCE_INLINE bool TTraceContextGuard::IsActive() const
{
    return Active_;
}

Y_FORCE_INLINE void TTraceContextGuard::Release()
{
    if (Y_UNLIKELY(Active_)) {
        SwitchTraceContext(std::move(OldContext_));
        Active_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TNullTraceContextGuard::TNullTraceContextGuard()
    : Active_(true)
    , OldContext_(SwitchTraceContext(nullptr))
{ }

Y_FORCE_INLINE TNullTraceContextGuard::TNullTraceContextGuard(TNullTraceContextGuard&& other)
    : Active_(other.Active_)
    , OldContext_(std::move(other.OldContext_))
{
    other.Active_ = false;
}

Y_FORCE_INLINE TNullTraceContextGuard::~TNullTraceContextGuard()
{
    Release();
}

Y_FORCE_INLINE bool TNullTraceContextGuard::IsActive() const
{
    return Active_;
}

Y_FORCE_INLINE void TNullTraceContextGuard::Release()
{
    if (Active_) {
        SwitchTraceContext(std::move(OldContext_));
        Active_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE bool IsVerbose(TTraceId traceId)
{
    return (traceId & 1) != 0;
}

////////////////////////////////////////////////////////////////////////////////

extern Y_POD_THREAD(TTraceContext*) CurrentTraceContext;
extern Y_POD_THREAD(TTraceId) CurrentTraceId;

Y_FORCE_INLINE TTraceContext* GetCurrentTraceContext()
{
    return CurrentTraceContext;
}

Y_FORCE_INLINE TTraceId GetCurrentTraceId()
{
    return CurrentTraceId;
}

template <class T>
void TraceEvent(
    const TTraceContext& context,
    const TString& annotationKey,
    const T& annotationValue)
{
    if (context.IsVerbose()) {
        using ::ToString;
        TraceEvent(
            context,
            annotationKey,
            ToString(annotationValue));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
