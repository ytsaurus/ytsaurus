#pragma once

#include "public.h"

#include <yt/core/misc/property.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTraceContext
{
public:
    TTraceContext();
    TTraceContext(
        TTraceId traceId,
        TSpanId spanId,
        TSpanId parentSpanId);

    bool IsEnabled() const;
    bool IsVerbose() const;

    TTraceContext CreateChild() const;

    DEFINE_BYVAL_RO_PROPERTY(TTraceId, TraceId);
    DEFINE_BYVAL_RO_PROPERTY(TSpanId, SpanId);
    DEFINE_BYVAL_RO_PROPERTY(TSpanId, ParentSpanId);
};

void FormatValue(TStringBuilder* builder, const TTraceContext& context, TStringBuf /*spec*/);
TString ToString(const TTraceContext& context);

TTraceContext CreateChildTraceContext();
TTraceContext CreateRootTraceContext(bool verbose = true);

extern const TTraceContext NullTraceContext;

////////////////////////////////////////////////////////////////////////////////

class TTraceContextGuard
{
public:
    explicit TTraceContextGuard(const TTraceContext& context);
    TTraceContextGuard(TTraceContextGuard&& other);
    ~TTraceContextGuard();

    const TTraceContext& GetContext() const;

    bool IsActive() const;
    void Release();

private:
    TTraceContext Context_;
    bool Active_;

};

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

};

const TTraceContext& GetCurrentTraceContext();
bool IsVerboseTracing();

void PushContext(const TTraceContext& context);
void PopContext();

////////////////////////////////////////////////////////////////////////////////

class TTraceSpanGuard
{
public:
    TTraceSpanGuard(
        const TTraceContext& parentContext,
        const TString& serviceName,
        const TString& spanName);
    TTraceSpanGuard(TTraceSpanGuard&& other);
    ~TTraceSpanGuard();

    bool IsActive() const;
    const TTraceContext& GetContext() const;
    void Release();

private:
    TString ServiceName_;
    TString SpanName_;
    TTraceContext Context_;
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

void TraceEvent(
    const TString& serviceName,
    const TString& spanName,
    const TString& annotationName);

void TraceEvent(
    const TString& annotationKey,
    const TString& annotationValue);

template <class T>
void TraceEvent(
    const TString& annotationKey,
    const T& annotationValue);

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

#define TRACE_ANNOTATION(head, ...) \
    do { \
        if (::NYT::NTracing::IsVerboseTracing(head)) { \
            ::NYT::NTracing::TraceEvent(head, __VA_ARGS__); \
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
