#include "stdafx.h"
#include "trace_context.h"
#include "trace_manager.h"
#include "private.h"

#include <core/concurrency/fls.h>

#include <core/misc/guid.h>

namespace NYT {
namespace NTracing {

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = TracingLogger;

////////////////////////////////////////////////////////////////////////////////

static ui64 GenerateId()
{
    auto guid = TGuid::Create();
    return
        (static_cast<ui64>(guid.Parts[0]) << 32) +
        (guid.Parts[1] ^ guid.Parts[3]);
}

static TSpanId GenerateTraceId()
{
    return GenerateId();
}

static TSpanId GenerateSpanId()
{
    return GenerateId();
}

////////////////////////////////////////////////////////////////////////////////

TTraceContext::TTraceContext()
    : TraceId_(InvalidTraceId)
    , SpanId_(InvalidSpanId)
    , ParentSpanId_(InvalidSpanId)
{ }

TTraceContext::TTraceContext(
    TTraceId traceId,
    TSpanId spanId,
    TSpanId parentSpanId)
    : TraceId_(traceId)
    , SpanId_(spanId)
    , ParentSpanId_(parentSpanId)
{ }

bool TTraceContext::IsEnabled() const
{
    return TraceId_ != InvalidTraceId;
}

TTraceContext TTraceContext::CreateRoot()
{
    return TTraceContext(
        GenerateTraceId(),
        InvalidSpanId,
        InvalidSpanId);
}

TTraceContext TTraceContext::CreateChild() const
{
    return TTraceContext(TraceId_, GenerateSpanId(), SpanId_);
}

Stroka ToString(const TTraceContext& context)
{
    return Sprintf("%08" PRIx64 ":%08" PRIx64 ":%08" PRIx64,
        context.GetTraceId(),
        context.GetSpanId(),
        context.GetParentSpanId());
}

TTraceContext NullTraceContext;

////////////////////////////////////////////////////////////////////////////////

TTraceContextGuard::TTraceContextGuard(const TTraceContext& context)
    : Context_(context)
    , Active_(true)
{
    PushContext(context);
}

TTraceContextGuard::TTraceContextGuard(TTraceContextGuard&& other)
    : Context_(other.Context_)
    , Active_(true)
{
    other.Active_ = false;
}

TTraceContextGuard::~TTraceContextGuard()
{
    if (Active_) {
        PopContext();
    }
}

const TTraceContext& TTraceContextGuard::GetContext() const
{
    return Context_;
}

bool TTraceContextGuard::IsActive() const
{
    return Active_;
}

////////////////////////////////////////////////////////////////////////////////

typedef NConcurrency::TFls<std::vector<TTraceContext>> TTraceContextStack;

static TTraceContextStack& TraceContextStack()
{
    static TTraceContextStack stack;
    return stack;
}

const TTraceContext& GetCurrentTraceContext()
{
    auto& stack = TraceContextStack();
    return stack->empty() ? NullTraceContext : stack->back();
}

bool IsTracingEnabled()
{
    return GetCurrentTraceContext().IsEnabled();
}

void PushContext(const TTraceContext& context)
{
    LOG_TRACE("Push context %s", ~ToString(context));
    TraceContextStack()->push_back(context);
}

void PopContext()
{
    LOG_TRACE("Pop context %s", ~ToString(GetCurrentTraceContext()));
    TraceContextStack()->pop_back();
}

TTraceContext CreateChildTraceContext()
{
    const auto& current = GetCurrentTraceContext();
    return current.IsEnabled()
        ? current.CreateChild()
        : current;
}

TTraceContext CreateRootTraceContext()
{
    return TTraceContext::CreateRoot();
}

////////////////////////////////////////////////////////////////////////////////

TTraceSpanGuard::TTraceSpanGuard(
    const Stroka& serviceName,
    const Stroka& spanName)
    : ContextGuard_(CreateChildTraceContext())
    , ServiceName_(serviceName)
    , SpanName_(spanName)
{
    TraceEvent(
        ContextGuard_.GetContext(),
        ServiceName_,
        SpanName_,
        ClientSendAnnotation);
}

TTraceSpanGuard::~TTraceSpanGuard()
{
    TraceEvent(
        ContextGuard_.GetContext(),
        ServiceName_,
        SpanName_,
        ClientReceiveAnnotation);
}

////////////////////////////////////////////////////////////////////////////////

Stroka ClientSendAnnotation("cs");
Stroka ClientReceiveAnnotation("cr");
Stroka ServerSendAnnotation("ss");
Stroka ServerReceiveAnnotation("sr");

////////////////////////////////////////////////////////////////////////////////

void TraceEvent(
    const Stroka& serviceName,
    const Stroka& spanName,
    const Stroka& annotationName)
{
    TraceEvent(
        GetCurrentTraceContext(),
        serviceName,
        spanName,
        annotationName);
}

void TraceEvent(
    const Stroka& annotationKey,
    const Stroka& annotationValue)
{
    TraceEvent(
        GetCurrentTraceContext(),
        annotationKey,
        annotationValue);
}

void TraceEvent(
    const TTraceContext& context,
    const Stroka& serviceName,
    const Stroka& spanName,
    const Stroka& annotationName)
{
    if (context.IsEnabled()) {
        TTraceManager::Get()->Enqueue(
            context,
            serviceName,
            spanName,
            annotationName);
    }
}

void TraceEvent(
    const TTraceContext& context,
    const Stroka& annotationKey,
    const Stroka& annotationValue)
{
    if (context.IsEnabled()) {
        TTraceManager::Get()->Enqueue(
            context,
            annotationKey,
            annotationValue);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT

