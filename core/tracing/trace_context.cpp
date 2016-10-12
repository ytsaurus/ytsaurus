#include "trace_context.h"
#include "private.h"
#include "trace_manager.h"

#include <yt/core/concurrency/fls.h>

#include <yt/core/misc/guid.h>

namespace NYT {
namespace NTracing {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TracingLogger;

const TTraceContext NullTraceContext;

////////////////////////////////////////////////////////////////////////////////

static ui64 GenerateId()
{
    auto guid = TGuid::Create();
    return
        (static_cast<ui64>(guid.Parts32[1]) << 32) +
        (guid.Parts32[1] ^ guid.Parts32[3]);
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

TTraceContext TTraceContext::CreateChild() const
{
    return TTraceContext(TraceId_, GenerateSpanId(), SpanId_);
}

Stroka ToString(const TTraceContext& context)
{
    return Format("%08" PRIx64 ":%08" PRIx64 ":%08" PRIx64,
        context.GetTraceId(),
        context.GetSpanId(),
        context.GetParentSpanId());
}

////////////////////////////////////////////////////////////////////////////////

static std::vector<TTraceContext>& TraceContextStack()
{
    static NConcurrency::TFls<std::vector<TTraceContext>> stack;
    return *stack;
}

const TTraceContext& GetCurrentTraceContext()
{
    auto& stack = TraceContextStack();
    return stack.empty() ? NullTraceContext : stack.back();
}

bool IsTracingEnabled()
{
    return GetCurrentTraceContext().IsEnabled();
}

void PushContext(const TTraceContext& context)
{
    LOG_TRACE("Push context %v", context);
    TraceContextStack().push_back(context);
}

void PopContext()
{
    auto& stack = TraceContextStack();
    YCHECK(!stack.empty());
    LOG_TRACE("Pop context %v", stack.back());
    stack.pop_back();
}

TTraceContext CreateChildTraceContext()
{
    const auto& current = GetCurrentTraceContext();
    return current.IsEnabled()
        ? current.CreateChild()
        : NullTraceContext;
}

TTraceContext CreateRootTraceContext()
{
    return TTraceContext(
        GenerateTraceId(),
        GenerateSpanId(),
        InvalidSpanId);
}

////////////////////////////////////////////////////////////////////////////////

TTraceSpanGuard::TTraceSpanGuard(
    const TTraceContext& parentContext,
    const Stroka& serviceName,
    const Stroka& spanName)
    : ServiceName_(serviceName)
    , SpanName_(spanName)
    , Context_(parentContext.CreateChild())
    , Active_(true)
{
    TraceEvent(
        Context_,
        ServiceName_,
        SpanName_,
        ClientSendAnnotation);
}

TTraceSpanGuard::TTraceSpanGuard(TTraceSpanGuard&& other)
    : ServiceName_(other.ServiceName_)
    , SpanName_(other.SpanName_)
    , Context_(other.Context_)
    , Active_(other.Active_)
{
    other.Active_ = false;
}

TTraceSpanGuard::~TTraceSpanGuard()
{
    Release();
}

bool TTraceSpanGuard::IsActive() const
{
    return Active_;
}

const TTraceContext& TTraceSpanGuard::GetContext() const
{
    return Context_;
}

void TTraceSpanGuard::Release()
{
    if (Active_) {
        TraceEvent(
            Context_,
            ServiceName_,
            SpanName_,
            ClientReceiveAnnotation);
        Active_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

TChildTraceContextGuard::TChildTraceContextGuard(
    const Stroka& serviceName,
    const Stroka& spanName)
    : SpanGuard_(
        GetCurrentTraceContext(),
        serviceName,
        spanName)
    , ContextGuard_(SpanGuard_.GetContext())
{ }

bool TChildTraceContextGuard::IsActive() const
{
    return SpanGuard_.IsActive();
}

void TChildTraceContextGuard::Release()
{
    SpanGuard_.Release();
    ContextGuard_.Release();
}

////////////////////////////////////////////////////////////////////////////////

const Stroka ClientSendAnnotation("cs");
const Stroka ClientReceiveAnnotation("cr");
const Stroka ServerSendAnnotation("ss");
const Stroka ServerReceiveAnnotation("sr");

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

