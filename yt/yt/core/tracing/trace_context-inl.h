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
    return Sampled_.load();
}

Y_FORCE_INLINE bool TTraceContext::IsDebug() const
{
    return Debug_;
}

Y_FORCE_INLINE TTraceId TTraceContext::GetTraceId() const
{
    return TraceId_;
}

Y_FORCE_INLINE TSpanId TTraceContext::GetSpanId() const
{
    return SpanId_;
}

Y_FORCE_INLINE TSpanId TTraceContext::GetParentSpanId() const
{
    return ParentSpanId_;
}

Y_FORCE_INLINE TRequestId TTraceContext::GetRequestId() const
{
    return RequestId_;
}

Y_FORCE_INLINE const TString& TTraceContext::GetSpanName() const
{
    return SpanName_;
}

Y_FORCE_INLINE const TString& TTraceContext::GetLoggingTag() const
{
    return LoggingTag_;
}

Y_FORCE_INLINE NProfiling::TCpuDuration TTraceContext::GetElapsedCpuTime() const
{
    return ElapsedCpuTime_.load(std::memory_order_relaxed);
}

template <class T>
void TTraceContext::AddTag(const TString& tagName, const T& tagValue)
{
    using ::ToString;
    AddTag(tagName, ToString(tagValue));
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
    TString spanName,
    TString loggingTag,
    bool forceTracing)
    : TraceContextGuard_(CreateChildTraceContext(
        traceContext,
        std::move(spanName),
        std::move(loggingTag),
        forceTracing))
    , FinishGuard_(GetCurrentTraceContext())
{ }

inline TChildTraceContextGuard::TChildTraceContextGuard(
    TString spanName,
    TString loggingTag,
    bool forceTracing)
    : TChildTraceContextGuard(
        GetCurrentTraceContext(),
        std::move(spanName),
        std::move(loggingTag),
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

} // namespace NYT::NTracing
