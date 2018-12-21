#pragma once
#ifndef TRACE_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include trace_context.h"
// For the sake of sane code completion.
#include "trace_context.h"
#endif

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TTraceContext::TTraceContext()
    : TraceId_(InvalidTraceId)
    , SpanId_(InvalidSpanId)
    , ParentSpanId_(InvalidSpanId)
{ }

Y_FORCE_INLINE TTraceContext::TTraceContext(
    TTraceId traceId,
    TSpanId spanId,
    TSpanId parentSpanId)
    : TraceId_(traceId)
    , SpanId_(spanId)
    , ParentSpanId_(parentSpanId)
{ }

Y_FORCE_INLINE bool TTraceContext::IsEnabled() const
{
    return TraceId_ != InvalidTraceId;
}

Y_FORCE_INLINE bool TTraceContext::IsVerbose() const
{
    return (TraceId_ & 1) != 0;
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TTraceContextGuard::TTraceContextGuard(const TTraceContext& context)
    : Context_(context)
    , Active_(context.IsEnabled())
{
    if (Active_) {
        PushContext(context);
    }
}

Y_FORCE_INLINE TTraceContextGuard::TTraceContextGuard(TTraceContextGuard&& other)
    : Context_(other.Context_)
    , Active_(other.Active_)
{
    other.Active_ = false;
}

Y_FORCE_INLINE TTraceContextGuard::~TTraceContextGuard()
{
    Release();
}

Y_FORCE_INLINE const TTraceContext& TTraceContextGuard::GetContext() const
{
    return Context_;
}

Y_FORCE_INLINE bool TTraceContextGuard::IsActive() const
{
    return Active_;
}

Y_FORCE_INLINE void TTraceContextGuard::Release()
{
    if (Active_) {
        PopContext();
        Active_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TNullTraceContextGuard::TNullTraceContextGuard()
    : Active_(true)
{
    PushContext(NullTraceContext);
}

Y_FORCE_INLINE TNullTraceContextGuard::TNullTraceContextGuard(TNullTraceContextGuard&& other)
    : Active_(other.Active_)
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
        PopContext();
        Active_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE bool IsVerboseTracing(const TString&)
{
    return NTracing::IsVerboseTracing();
}

Y_FORCE_INLINE bool IsVerboseTracing(const char*)
{
    return GetCurrentTraceContext().IsVerbose();
}

Y_FORCE_INLINE bool IsVerboseTracing(const TTraceContext& context)
{
    return context.IsVerbose();
}

template <class T>
void TraceEvent(
    const TString& annotationKey,
    const T& annotationValue)
{
    using ::ToString;
    TraceEvent(
        annotationKey,
        ToString(annotationValue));
}

template <class T>
void TraceEvent(
    const TTraceContext& context,
    const TString& annotationKey,
    const T& annotationValue)
{
    using ::ToString;
    TraceEvent(
        context,
        annotationKey,
        ToString(annotationValue));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
