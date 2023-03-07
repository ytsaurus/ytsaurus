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

Y_FORCE_INLINE TString TTraceContext::GetSpanName() const
{
    auto guard = Guard(Lock_);
    return SpanName_;
}

Y_FORCE_INLINE NProfiling::TCpuDuration TTraceContext::GetElapsedCpuTime() const
{
    return ElapsedCpuTime_.load(std::memory_order_relaxed);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE TTraceContextGuard::TTraceContextGuard(TTraceContextPtr traceContext)
    : Active_(static_cast<bool>(traceContext))
{
    if (Active_) {
        OldTraceContext_ = SwitchTraceContext(std::move(traceContext));
    }
}

Y_FORCE_INLINE TTraceContextGuard::TTraceContextGuard(TTraceContextGuard&& other)
    : Active_(other.Active_)
    , OldTraceContext_(std::move(other.OldTraceContext_))
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
    if (Active_) {
        SwitchTraceContext(std::move(OldTraceContext_));
        Active_ = false;
    }
}

Y_FORCE_INLINE const TTraceContextPtr& TTraceContextGuard::GetOldTraceContext() const
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

extern thread_local TTraceContext* CurrentTraceContext;
extern thread_local TTraceId CurrentTraceId;

Y_FORCE_INLINE TTraceContext* GetCurrentTraceContext()
{
    return CurrentTraceContext;
}

Y_FORCE_INLINE TTraceId GetCurrentTraceId()
{
    return CurrentTraceId;
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
