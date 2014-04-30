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

////////////////////////////////////////////////////////////////////////////////

class TTraceContextGuard
    : private TNonCopyable
{
public:
    explicit TTraceContextGuard(const TTraceContext& context);
    ~TTraceContextGuard();

    const TTraceContext& GetContext() const;

private:
    TTraceContext Context_;

};

const TTraceContext& GetCurrentTraceContext();
bool IsTracingEnabled();

void PushContext(const TTraceContext& context);
void PopContext();

TTraceContext CreateChildTraceContext();
TTraceContext CreateRootTraceContext();

////////////////////////////////////////////////////////////////////////////////

class TTraceSpanGuard
    : private TNonCopyable
{
public:
    TTraceSpanGuard(
        const Stroka& serviceName,
        const Stroka& spanName);
    ~TTraceSpanGuard();

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
    const Stroka& serviceName,
    const Stroka& spanName,
    const Stroka& annotationKey,
    const Stroka& annotationValue);

template <class T>
void TraceEvent(
    const Stroka& serviceName,
    const Stroka& spanName,
    const Stroka& annotationKey,
    const T& annotationValue);

void TraceEvent(
    const TTraceContext& context,
    const Stroka& serviceName,
    const Stroka& spanName,
    const Stroka& annotationName);

void TraceEvent(
    const TTraceContext& context,
    const Stroka& serviceName,
    const Stroka& spanName,
    const Stroka& annotationKey,
    const Stroka& annotationValue);

template <class T>
void TraceEvent(
    const TTraceContext& context,
    const Stroka& serviceName,
    const Stroka& spanName,
    const Stroka& annotationKey,
    const T& annotationValue);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT

#define TRACE_CONTEXT_INL_H_
#include "trace_context-inl.h"
#undef TRACE_CONTEXT_INL_H_
