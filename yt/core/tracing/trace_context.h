#pragma once

#include "public.h"

#include <yt/core/misc/property.h>

#include <yt/core/profiling/public.h>

#include <atomic>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTraceContext
    : public TIntrinsicRefCounted
{
public:
    TTraceContext();
    TTraceContext(
        TTraceId traceId,
        TSpanId spanId,
        TSpanId parentSpanId);

    bool IsVerbose() const;

    TTraceContextPtr CreateChild() const;

    DEFINE_BYVAL_RO_PROPERTY(TTraceId, TraceId);
    DEFINE_BYVAL_RO_PROPERTY(TSpanId, SpanId);
    DEFINE_BYVAL_RO_PROPERTY(TSpanId, ParentSpanId);

    void IncrementElapsedCpuTime(NProfiling::TCpuDuration delta);
    void FlushElapsedTime();
    NProfiling::TCpuDuration GetElapsedCpuTime() const;
    TDuration GetElapsedTime() const;

private:
    std::atomic<NProfiling::TCpuDuration> ElapsedCpuTime_ = {0};

};

DEFINE_REFCOUNTED_TYPE(TTraceContext)

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TTraceContext* context, TStringBuf /*spec*/);
void FormatValue(TStringBuilderBase* builder, const TTraceContext& context, TStringBuf /*spec*/);
void FormatValue(TStringBuilderBase* builder, const TTraceContextPtr& context, TStringBuf /*spec*/);
TString ToString(TTraceContext* context);
TString ToString(const TTraceContext& context);
TString ToString(const TTraceContextPtr& context);

TTraceContextPtr CreateChildTraceContext();
TTraceContextPtr CreateRootTraceContext(bool verbose = true);

bool IsVerbose(TTraceId traceId);

TTraceContext* GetCurrentTraceContext();
TTraceId GetCurrentTraceId();
void FlushCurrentTraceContextTime();

////////////////////////////////////////////////////////////////////////////////

class TTraceContextGuard
{
public:
    explicit TTraceContextGuard(TTraceContextPtr context);
    TTraceContextGuard(TTraceContextGuard&& other);
    ~TTraceContextGuard();

    bool IsActive() const;
    void Release();

private:
    bool Active_;
    TTraceContextPtr OldContext_;
};

////////////////////////////////////////////////////////////////////////////////

class TNullTraceContextGuard
{
public:
    TNullTraceContextGuard();
    TNullTraceContextGuard(TNullTraceContextGuard&& other);
    ~TNullTraceContextGuard();

    bool IsActive() const;
    void Release();

private:
    bool Active_;
    TTraceContextPtr OldContext_;
};

////////////////////////////////////////////////////////////////////////////////

class TTraceSpanGuard
{
public:
    TTraceSpanGuard(
        const TTraceContextPtr& parentContext,
        const TString& serviceName,
        const TString& spanName);
    TTraceSpanGuard(TTraceSpanGuard&& other);
    ~TTraceSpanGuard();

    bool IsActive() const;
    const TTraceContextPtr& GetContext() const;
    void Release();

private:
    TString ServiceName_;
    TString SpanName_;
    TTraceContextPtr Context_;
    bool Active_;

};

////////////////////////////////////////////////////////////////////////////////

class TChildTraceContextGuard
{
public:
    TChildTraceContextGuard(
        const TString& serviceName,
        const TString& spanName);
    TChildTraceContextGuard(TChildTraceContextGuard&& other) = default;

    bool IsActive() const;
    void Release();

    //! Needed for TRACE_CHILD.
    operator bool() const
    {
        return false;
    }

private:
    TTraceSpanGuard SpanGuard_;
    TTraceContextGuard ContextGuard_;

};

////////////////////////////////////////////////////////////////////////////////

extern const TString ClientSendAnnotation;
extern const TString ClientReceiveAnnotation;
extern const TString ServerSendAnnotation;
extern const TString ServerReceiveAnnotation;

////////////////////////////////////////////////////////////////////////////////
// For internal use only.

TTraceContextPtr SwitchTraceContext(TTraceContextPtr newContext);
void InstallTraceContext(NProfiling::TCpuInstant now, TTraceContextPtr newContext);
TTraceContextPtr UninstallTraceContext(NProfiling::TCpuInstant now);

void TraceEvent(
    const TTraceContext& context,
    const TString& serviceName,
    const TString& spanName,
    const TString& annotationName);

void TraceEvent(
    const TTraceContext& context,
    const TString& annotationKey,
    const TString& annotationValue);

template <class T>
void TraceEvent(
    const TTraceContext& context,
    const TString& annotationKey,
    const T& annotationValue);

////////////////////////////////////////////////////////////////////////////////

#define TRACE_ANNOTATION(...) \
    do { \
        if (::NYT::NTracing::IsVerbose(::NYT::NTracing::GetCurrentTraceId())) { \
            ::NYT::NTracing::TraceEvent(*::NYT::NTracing::GetCurrentTraceContext(), __VA_ARGS__); \
        } \
    } while (false)

#define TRACE_ANNOTATION_WITH_CONTEXT(context, ...) \
    do { \
        const auto& pinnedContext = context; \
        if (pinnedContext && pinnedContext->IsVerbose()) { \
            ::NYT::NTracing::TraceEvent(*pinnedContext, __VA_ARGS__); \
        } \
    } while (false)

#define TRACE_CHILD(serviceName, spanName) \
    if (auto TRACE_CHILD__Guard = ::NYT::NTracing::TChildTraceContextGuard(serviceName, spanName)) \
    { Y_UNREACHABLE(); } \
    else

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

#define TRACE_CONTEXT_INL_H_
#include "trace_context-inl.h"
#undef TRACE_CONTEXT_INL_H_
