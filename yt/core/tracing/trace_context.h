#pragma once

#include "public.h"

#include <core/misc/property.h>

namespace NYT {
namespace NTracing {

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

    static TTraceContext CreateRoot();
    TTraceContext CreateChild() const;

    DEFINE_BYVAL_RO_PROPERTY(TTraceId, TraceId);
    DEFINE_BYVAL_RO_PROPERTY(TSpanId, SpanId);
    DEFINE_BYVAL_RO_PROPERTY(TSpanId, ParentSpanId);
};

Stroka ToString(const TTraceContext& context);

extern TTraceContext NullTraceContext;

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
bool IsTracingEnabled();

void PushContext(const TTraceContext& context);
void PopContext();

TTraceContext CreateChildTraceContext();
TTraceContext CreateRootTraceContext();

////////////////////////////////////////////////////////////////////////////////

class TTraceSpanGuard
{
public:
    TTraceSpanGuard(
        const Stroka& serviceName,
        const Stroka& spanName);
    TTraceSpanGuard(TTraceSpanGuard&& other) = default;
    ~TTraceSpanGuard();

    bool IsActive() const;
    void Release();

    //! Needed for TRACE_SPAN.
    operator bool() const
    {
        return false;
    }

private:
    TTraceContextGuard ContextGuard_;
    Stroka ServiceName_;
    Stroka SpanName_;

};

////////////////////////////////////////////////////////////////////////////////

extern Stroka ClientSendAnnotation;
extern Stroka ClientReceiveAnnotation;
extern Stroka ServerSendAnnotation;
extern Stroka ServerReceiveAnnotation;

////////////////////////////////////////////////////////////////////////////////

void TraceEvent(
    const Stroka& serviceName,
    const Stroka& spanName,
    const Stroka& annotationName);

void TraceEvent(
    const Stroka& annotationKey,
    const Stroka& annotationValue);

template <class T>
void TraceEvent(
    const Stroka& annotationKey,
    const T& annotationValue);

void TraceEvent(
    const TTraceContext& context,
    const Stroka& serviceName,
    const Stroka& spanName,
    const Stroka& annotationName);

void TraceEvent(
    const TTraceContext& context,
    const Stroka& annotationKey,
    const Stroka& annotationValue);

template <class T>
void TraceEvent(
    const TTraceContext& context,
    const Stroka& annotationKey,
    const T& annotationValue);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

inline bool IsTracingEnabled(const Stroka&)
{
    return NTracing::IsTracingEnabled();
}

inline bool IsTracingEnabled(const char*)
{
    return GetCurrentTraceContext().IsEnabled();
}

inline bool IsTracingEnabled(const TTraceContext& context)
{
    return context.IsEnabled();
}

} // namespace NDetail

#define TRACE_ANNOTATION(head, ...) \
    do { \
        if (::NYT::NTracing::NDetail::IsTracingEnabled(head)) { \
            ::NYT::NTracing::TraceEvent(head, __VA_ARGS__); \
        } \
    } while (false)

#define TRACE_SPAN(serviceName, spanName) \
    if (auto TRACE_SPAN__Guard = ::NYT::NTracing::TTraceSpanGuard(serviceName, spanName)) \
    { YUNREACHABLE(); } \
    else

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT

#define TRACE_CONTEXT_INL_H_
#include "trace_context-inl.h"
#undef TRACE_CONTEXT_INL_H_
