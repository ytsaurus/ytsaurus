#pragma once

#include "public.h"

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/profiling/public.h>

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
};

void FormatValue(TStringBuilderBase* builder, const TSpanContext& context, TStringBuf spec);
TString ToString(const TSpanContext& context);

////////////////////////////////////////////////////////////////////////////////

void SetGlobalTracer(const ITracerPtr& tracer);

////////////////////////////////////////////////////////////////////////////////

//! Accumulates information associated with a single tracing span.
/*!
 *  \note Thread affininty: any unless noted otherwise.
 */
 class TTraceContext
    : public TRefCounted
{
public:
    TTraceContext(
        TSpanContext parentSpanContext,
        TString spanName,
        TRequestId requestId = {},
        TString loggingTag = {},
        TTraceContextPtr parentTraceContext = nullptr);

    //! Finalizes and publishes the context (if sampling is enabled).
    /*!
     *  Safe to call multiple times from arbitrary threads; only the first call matters.
     */
    void Finish();

    bool IsSampled() const;
    void SetSampled(bool value = true);

    bool IsDebug() const;

    TSpanContext GetSpanContext() const;
    TTraceId GetTraceId() const;
    TSpanId GetSpanId() const;
    TSpanId GetParentSpanId() const;
    TRequestId GetRequestId() const;
    const TString& GetSpanName() const;
    const TString& GetLoggingTag() const;
    TInstant GetStartTime() const;

    //! Returns the wall time from the context's construction to #Finish call.
    /*!
     *  Not thread-safe; can only be called after #Finish is complete.
     */
    TDuration GetDuration() const;

    using TTagList = SmallVector<std::pair<TString, TString>, 4>;
    TTagList GetTags() const;

    void AddTag(const TString& tagKey, const TString& tagValue);

    template <class T>
    void AddTag(const TString& tagName, const T& tagValue);

    //! Adds error tag. Spans containing errors are highlited in Jaeger UI.
    void AddErrorTag();

    TTraceContextPtr CreateChild(
        TString spanName,
        TString loggingTag = {});

    void IncrementElapsedCpuTime(NProfiling::TCpuDuration delta);
    NProfiling::TCpuDuration GetElapsedCpuTime() const;
    TDuration GetElapsedTime() const;

private:
    const TTraceId TraceId_;
    const TSpanId SpanId_;
    const TSpanId ParentSpanId_;
    std::atomic<bool> Sampled_;
    const bool Debug_;
    const TTraceContextPtr ParentContext_;
    const TString SpanName_;
    const TRequestId RequestId_;
    const TString LoggingTag_;
    const NProfiling::TCpuInstant StartTime_;

    std::atomic<bool> Finished_ = false;
    NProfiling::TCpuDuration Duration_;

    std::atomic<NProfiling::TCpuDuration> ElapsedCpuTime_ = 0;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, TagsLock_);
    TTagList Tags_;
};

DEFINE_REFCOUNTED_TYPE(TTraceContext)

void FormatValue(TStringBuilderBase* builder, const TTraceContextPtr& context, TStringBuf spec);
TString ToString(const TTraceContextPtr& context);

////////////////////////////////////////////////////////////////////////////////

TTraceContext* GetCurrentTraceContext();
void FlushCurrentTraceContextTime();

void ToProto(NProto::TTracingExt* ext, const TTraceContextPtr& context);

TTraceContextPtr CreateRootTraceContext(
    TString spanName,
    TRequestId requestId = {},
    TString loggingTag = {},
    bool sampled = false,
    bool debug = false);
TTraceContextPtr CreateChildTraceContext(
    const TTraceContextPtr& parentContext,
    TString spanName,
    TString loggingTag = {},
    bool forceTracing = false);
TTraceContextPtr CreateChildTraceContext(
    const NProto::TTracingExt& ext,
    TString spanName,
    TRequestId requestId = {},
    TString loggingTag = {},
    bool forceTracing = false);

////////////////////////////////////////////////////////////////////////////////

//! Installs the given trace into the current fiber implicit trace slot.
class TCurrentTraceContextGuard
{
public:
    explicit TCurrentTraceContextGuard(TTraceContextPtr traceContext);
    TCurrentTraceContextGuard(TCurrentTraceContextGuard&& other);
    ~TCurrentTraceContextGuard();

    bool IsActive() const;
    void Release();

    const TTraceContextPtr& GetOldTraceContext() const;

private:
    bool Active_;
    TTraceContextPtr OldTraceContext_;
};

////////////////////////////////////////////////////////////////////////////////

//! Installs null trace into the current fiber implicit trace slot.
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

//! Invokes TTraceContext::Finish upon destruction.
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

//! Installs the given trace into the current fiber implicit trace slot.
//! Finishes the trace context upon destruction.
class TTraceContextGuard
{
public:
    explicit TTraceContextGuard(TTraceContextPtr traceContext);
    TTraceContextGuard(TTraceContextGuard&& other) = default;

private:
    TCurrentTraceContextGuard TraceContextGuard_;
    TTraceContextFinishGuard FinishGuard_;
};

////////////////////////////////////////////////////////////////////////////////

//! Constructs a child trace context and installs it into the current fiber implicit trace slot.
//! Finishes the child trace context upon destruction.
class TChildTraceContextGuard
{
public:
    TChildTraceContextGuard(
        const TTraceContextPtr& traceContext,
        TString spanName,
        TString loggingTag = {},
        bool forceTracing = false);
    explicit TChildTraceContextGuard(
        TString spanName,
        TString loggingTag = {},
        bool forceTracing = false);
    TChildTraceContextGuard(TChildTraceContextGuard&& other) = default;

private:
    TCurrentTraceContextGuard TraceContextGuard_;
    TTraceContextFinishGuard FinishGuard_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

#define TRACE_CONTEXT_INL_H_
#include "trace_context-inl.h"
#undef TRACE_CONTEXT_INL_H_
