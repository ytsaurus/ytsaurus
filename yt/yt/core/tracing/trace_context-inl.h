#pragma once
#ifndef TRACE_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include trace_context.h"
// For the sake of sane code completion.
#include "trace_context.h"
#endif

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE bool TTraceContext::IsSampled() const
{
    return SpanContext_.Sampled;
}

Y_FORCE_INLINE bool TTraceContext::IsDebug() const
{
    return SpanContext_.Debug;
}

Y_FORCE_INLINE TSpanContext TTraceContext::GetSpanContext() const
{
    return SpanContext_;
}

Y_FORCE_INLINE TTraceId TTraceContext::GetTraceId() const
{
    return SpanContext_.TraceId;
}

Y_FORCE_INLINE TSpanId TTraceContext::GetSpanId() const
{
    return SpanContext_.SpanId;
}

Y_FORCE_INLINE TSpanId TTraceContext::GetParentSpanId() const
{
    return ParentSpanId_;
}

Y_FORCE_INLINE TSpanId TTraceContext::GetFollowsFromSpanId() const
{
    return FollowsFromSpanId_;
}

Y_FORCE_INLINE TRequestId TTraceContext::GetRequestId() const
{
    return RequestId_;
}

Y_FORCE_INLINE const TString& TTraceContext::GetSpanName() const
{
    return SpanName_;
}

Y_FORCE_INLINE NProfiling::TCpuDuration TTraceContext::GetElapsedCpuTime() const
{
    return ElapsedCpuTime_.load(std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

// For internal use only.
TTraceContextPtr SwitchTraceContext(TTraceContextPtr traceContext);

Y_FORCE_INLINE TCurrentTraceContextGuard::TCurrentTraceContextGuard(TTraceContextPtr traceContext)
    : Active_(static_cast<bool>(traceContext))
{
    if (Active_) {
        OldTraceContext_ = SwitchTraceContext(std::move(traceContext));
    }
}

Y_FORCE_INLINE TCurrentTraceContextGuard::TCurrentTraceContextGuard(TCurrentTraceContextGuard&& other)
    : Active_(other.Active_)
    , OldTraceContext_(std::move(other.OldTraceContext_))
{
    other.Active_ = false;
}

Y_FORCE_INLINE TCurrentTraceContextGuard::~TCurrentTraceContextGuard()
{
    Release();
}

Y_FORCE_INLINE bool TCurrentTraceContextGuard::IsActive() const
{
    return Active_;
}

Y_FORCE_INLINE void TCurrentTraceContextGuard::Release()
{
    if (Active_) {
        SwitchTraceContext(std::move(OldTraceContext_));
        Active_ = false;
    }
}

Y_FORCE_INLINE const TTraceContextPtr& TCurrentTraceContextGuard::GetOldTraceContext() const
{
    return OldTraceContext_;
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TNullTraceContextGuard::TNullTraceContextGuard()
    : Active_(true)
    , OldTraceContext_(SwitchTraceContext(nullptr))
{ }

Y_FORCE_INLINE TNullTraceContextGuard::TNullTraceContextGuard(TNullTraceContextGuard&& other)
    : Active_(other.Active_)
    , OldTraceContext_(std::move(other.OldTraceContext_))
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
        SwitchTraceContext(std::move(OldTraceContext_));
        Active_ = false;
    }
}

Y_FORCE_INLINE const TTraceContextPtr& TNullTraceContextGuard::GetOldTraceContext() const
{
    return OldTraceContext_;
}

////////////////////////////////////////////////////////////////////////////////

inline TTraceContextGuard::TTraceContextGuard(TTraceContextPtr traceContext)
    : TraceContextGuard_(std::move(traceContext))
    , FinishGuard_(GetCurrentTraceContext())
{ }

////////////////////////////////////////////////////////////////////////////////

inline TChildTraceContextGuard::TChildTraceContextGuard(
    const TTraceContextPtr& traceContext,
    const TString& spanName,
    bool forceTracing)
    : TraceContextGuard_(CreateChildTraceContext(
        traceContext,
        spanName,
        forceTracing))
    , FinishGuard_(GetCurrentTraceContext())
{ }

inline TChildTraceContextGuard::TChildTraceContextGuard(
    const TString& spanName,
    bool forceTracing)
    : TChildTraceContextGuard(
        GetCurrentTraceContext(),
        spanName,
        forceTracing)
{ }

////////////////////////////////////////////////////////////////////////////////

inline TTraceContextFinishGuard::TTraceContextFinishGuard(TTraceContextPtr traceContext)
    : TraceContext_(std::move(traceContext))
{ }

inline TTraceContextFinishGuard::~TTraceContextFinishGuard()
{
    if (TraceContext_) {
        TraceContext_->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

extern thread_local TTraceContext* CurrentTraceContext;

Y_FORCE_INLINE TTraceContext* GetCurrentTraceContext()
{
    return CurrentTraceContext;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void AddTag(const TString& tagName, const T& tagValue)
{
    auto context = GetCurrentTraceContext();
    if (!context) {
        return;
    }

    context->AddTag(tagName, ToString(tagValue));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
