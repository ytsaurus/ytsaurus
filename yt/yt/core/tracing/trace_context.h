#pragma once

#include "public.h"

#include <yt/core/misc/property.h>
#include <yt/core/misc/guid.h>

#include <yt/core/concurrency/spinlock.h>

#include <yt/core/profiling/public.h>

#include <yt/yt/library/tracing/public.h>

#include <atomic>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

//! TSpanContext represents span identity propagated across the network.
//!
//! See https://opentracing.io/specification/
struct TSpanContext
{
    TTraceId TraceId = InvalidTraceId;
    TSpanId SpanId = InvalidSpanId;
    bool Sampled = false;
    bool Debug = false;

    TSpanContext CreateChild();
};

void FormatValue(TStringBuilderBase* builder, const TSpanContext& context, TStringBuf spec);
TString ToString(const TSpanContext& context);

////////////////////////////////////////////////////////////////////////////////

void SetGlobalTracer(const ITracerPtr& tracer);

////////////////////////////////////////////////////////////////////////////////

struct TFollowsFrom {};

//! TTraceContext accumulates information associated with single tracing span.
class TTraceContext
    : public TRefCounted
{
public:
    TTraceContext(
        TSpanContext parentSpanContext,
        TString spanName,
        TTraceContextPtr parentTraceContext = nullptr);
    TTraceContext(
        TSpanContext parentSpanContext,
        TString spanName,
        TRequestId requestId);
    TTraceContext(
        TFollowsFrom,
        TSpanContext parent,
        TString spanName,
        TTraceContextPtr parentTraceContext = nullptr);

    void Finish();

    bool IsSampled() const;
    bool IsDebug() const;

    TSpanContext GetSpanContext() const;
    TTraceId GetTraceId() const;
    TSpanId GetSpanId() const;
    TSpanId GetParentSpanId() const;
    TSpanId GetFollowsFromSpanId() const;
    TRequestId GetRequestId() const;

    const TString& GetSpanName() const;

    TInstant GetStartTime() const;
    TDuration GetDuration() const;

    using TTagList = SmallVector<std::pair<TString, TString>, 4>;
    TTagList GetTags() const;

    void SetSampled(bool value = true);
    void AddTag(const TString& tagKey, const TString& tagValue);
    void ResetStartTime();

    TTraceContextPtr CreateChild(const TString& spanName);

    void IncrementElapsedCpuTime(NProfiling::TCpuDuration delta);
    NProfiling::TCpuDuration GetElapsedCpuTime() const;
    TDuration GetElapsedTime() const;

private:
    const TSpanId ParentSpanId_ = InvalidSpanId;
    const TSpanId FollowsFromSpanId_ = InvalidSpanId;
    const TRequestId RequestId_;
    const TTraceContextPtr ParentContext_;
    const TString SpanName_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock_);
    NProfiling::TCpuInstant StartTime_;
    NProfiling::TCpuDuration Duration_;
    TSpanContext SpanContext_;
    TTagList Tags_;
    bool Finished_ = false;

    std::atomic<NProfiling::TCpuDuration> ElapsedCpuTime_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TTraceContext)

void FormatValue(TStringBuilderBase* builder, const TTraceContextPtr& context, TStringBuf spec);
TString ToString(const TTraceContextPtr& context);

////////////////////////////////////////////////////////////////////////////////

TTraceContext* GetCurrentTraceContext();
void FlushCurrentTraceContextTime();

void ToProto(NProto::TTracingExt* ext, const TTraceContextPtr& context);

TTraceContextPtr CreateRootTraceContext(
    const TString& spanName,
    TRequestId requestId = {});
TTraceContextPtr CreateChildTraceContext(
    const TTraceContextPtr& parentContext,
    const TString& spanName,
    bool forceTracing = false);
TTraceContextPtr CreateChildTraceContext(
    const NProto::TTracingExt& ext,
    const TString& spanName,
    TRequestId requestId = {},
    bool forceTracing = false);

template <class T>
void AddTag(const TString& tagName, const T& tagValue);

// Add error tag to current span. Spans containing errors are highlited in jaeger UI.
void AddErrorTag();

////////////////////////////////////////////////////////////////////////////////

//! TTraceContextGuard installs trace into the current fiber implicit trace slot.
class TTraceContextGuard
{
public:
    explicit TTraceContextGuard(TTraceContextPtr traceContext);
    TTraceContextGuard(TTraceContextGuard&& other);
    ~TTraceContextGuard();

    bool IsActive() const;
    void Release();

    const TTraceContextPtr& GetOldTraceContext() const;

private:
    bool Active_;
    TTraceContextPtr OldTraceContext_;
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

    const TTraceContextPtr& GetOldTraceContext() const;

private:
    bool Active_;
    TTraceContextPtr OldTraceContext_;
};

////////////////////////////////////////////////////////////////////////////////

class TTraceContextFinishGuard
{
public:
    explicit TTraceContextFinishGuard(TTraceContextPtr traceContext);
    ~TTraceContextFinishGuard();

    TTraceContextFinishGuard(const TTraceContextFinishGuard&) = delete;
    TTraceContextFinishGuard(TTraceContextFinishGuard&&) = default;

    TTraceContextFinishGuard& operator=(const TTraceContextFinishGuard&) = delete;
    TTraceContextFinishGuard& operator=(TTraceContextFinishGuard&&) = default;

private:
    TTraceContextPtr TraceContext_;
};

////////////////////////////////////////////////////////////////////////////////

class TChildTraceContextGuard
{
public:
    explicit TChildTraceContextGuard(const TString& spanName, bool forceTracing = false);
    TChildTraceContextGuard(TChildTraceContextGuard&& other) = default;

private:
    TTraceContextGuard TraceContextGuard_;
    TTraceContextFinishGuard FinishGuard_;
};

////////////////////////////////////////////////////////////////////////////////
// For internal use only.

TTraceContextPtr SwitchTraceContext(TTraceContextPtr traceContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

#define TRACE_CONTEXT_INL_H_
#include "trace_context-inl.h"
#undef TRACE_CONTEXT_INL_H_
