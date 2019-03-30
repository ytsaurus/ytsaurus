#include "trace_context.h"
#include "private.h"
#include "trace_manager.h"

#include <yt/core/concurrency/fls.h>
#include <yt/core/concurrency/fiber.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/profiling/timing.h>

namespace NYT::NTracing {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TracingLogger;

////////////////////////////////////////////////////////////////////////////////

namespace  {

TSpanId GenerateTraceId(bool verbose)
{
    return (RandomNumber<ui64>() & ~1ULL) | (verbose ? 1 : 0);
}

TSpanId GenerateSpanId()
{
    return RandomNumber<ui64>();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTraceContextPtr TTraceContext::CreateChild() const
{
    return New<TTraceContext>(TraceId_, GenerateSpanId(), SpanId_);
}

TDuration TTraceContext::GetElapsedTime() const
{
    return CpuDurationToDuration(GetElapsedCpuTime());
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TTraceContext* context, TStringBuf spec)
{
    if (context) {
        FormatValue(builder, *context, spec);
    } else {
        builder->AppendString(AsStringBuf("<null>"));
    }
}

TString ToString(TTraceContext* context)
{
    return ToStringViaBuilder(context);
}

void FormatValue(TStringBuilderBase* builder, const TTraceContext& context, TStringBuf /*spec*/)
{
    builder->AppendFormat("%08" PRIx64 ":%08" PRIx64 ":%08" PRIx64,
        context.GetTraceId(),
        context.GetSpanId(),
        context.GetParentSpanId());
}

TString ToString(const TTraceContext& context)
{
    return ToStringViaBuilder(context);
}

void FormatValue(TStringBuilderBase* builder, const TTraceContextPtr& context, TStringBuf spec)
{
    FormatValue(builder, context.Get(), spec);
}

TString ToString(const TTraceContextPtr& context)
{
    return ToStringViaBuilder(context);
}

////////////////////////////////////////////////////////////////////////////////

struct TCurrentTraceContextReclaimer
{
    ~TCurrentTraceContextReclaimer()
    {
        if (CurrentTraceContext) {
            CurrentTraceContext->Unref();
            CurrentTraceContext = nullptr;
        }
    }
};

Y_POD_THREAD(TTraceContext*) CurrentTraceContext;
Y_POD_THREAD(TTraceId) CurrentTraceId;
Y_POD_THREAD(TCpuInstant) TraceContextTimingCheckpoint;
Y_STATIC_THREAD(TCurrentTraceContextReclaimer) CurrentTraceContextReclaimer;

TTraceContextPtr SwitchTraceContext(TTraceContextPtr newContext)
{
    auto oldContext = TTraceContextPtr(CurrentTraceContext, false);
    auto now = GetCpuInstant();
    auto delta = now - TraceContextTimingCheckpoint;
    YT_LOG_TRACE("Switching context (OldContext: %v, NewContext: %v, CpuTimeDelta: %v)",
        oldContext,
        newContext,
        NProfiling::CpuDurationToDuration(delta));
    CurrentTraceContext = newContext.Release();
    CurrentTraceId = CurrentTraceContext ? CurrentTraceContext->GetTraceId() : InvalidTraceId;
    TraceContextTimingCheckpoint = now;
    if (oldContext) {
        oldContext->IncrementElapsedCpuTime(delta);
    }
    return oldContext;
}

void InstallTraceContext(NProfiling::TCpuInstant now, TTraceContextPtr context)
{
    YT_LOG_TRACE("Installing context (Context: %v)",
        context);
    Y_ASSERT(!CurrentTraceContext);
    CurrentTraceContext = context.Release();
    CurrentTraceId = CurrentTraceContext ? CurrentTraceContext->GetTraceId() : InvalidTraceId;
    TraceContextTimingCheckpoint = now;
}

TTraceContextPtr UninstallTraceContext(NProfiling::TCpuInstant now)
{
    auto context = TTraceContextPtr(CurrentTraceContext, false);
    auto delta = now - TraceContextTimingCheckpoint;
    YT_LOG_TRACE("Uninstalling context (Context: %v, CpuTimeDelta: %v)",
        context,
        NProfiling::CpuDurationToDuration(delta));
    CurrentTraceContext = nullptr;
    CurrentTraceId = InvalidTraceId;
    if (context) {
        context->IncrementElapsedCpuTime(delta);
    }
    return context;
}

void FlushCurrentTraceContextTime()
{
    auto* context = static_cast<TTraceContext*>(CurrentTraceContext);
    if (!context) {
        return;
    }

    auto now = GetCpuInstant();
    auto delta = now - TraceContextTimingCheckpoint;
    YT_LOG_TRACE("Flushing context time (Context: %v, CpuTimeDelta: %v)",
        context,
        NProfiling::CpuDurationToDuration(delta));
    context->IncrementElapsedCpuTime(delta);
    TraceContextTimingCheckpoint = now;
}

TTraceContextPtr CreateChildTraceContext()
{
    const auto* current = GetCurrentTraceContext();
    return current ? current->CreateChild() : nullptr;
}

TTraceContextPtr CreateRootTraceContext(bool verbose)
{
    return New<TTraceContext>(
        GenerateTraceId(verbose),
        GenerateSpanId(),
        InvalidSpanId);
}

////////////////////////////////////////////////////////////////////////////////

TTraceSpanGuard::TTraceSpanGuard(
    const TTraceContextPtr& parentContext,
    const TString& serviceName,
    const TString& spanName)
    : ServiceName_(serviceName)
    , SpanName_(spanName)
    , Context_(parentContext ? parentContext->CreateChild() : CreateRootTraceContext())
    , Active_(true)
{
    TraceEvent(
        *Context_,
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

const TTraceContextPtr& TTraceSpanGuard::GetContext() const
{
    return Context_;
}

void TTraceSpanGuard::Release()
{
    if (Active_) {
        TraceEvent(
            *Context_,
            ServiceName_,
            SpanName_,
            ClientReceiveAnnotation);
        Active_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

TChildTraceContextGuard::TChildTraceContextGuard(
    const TString& serviceName,
    const TString& spanName)
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

const TString ClientSendAnnotation("cs");
const TString ClientReceiveAnnotation("cr");
const TString ServerSendAnnotation("ss");
const TString ServerReceiveAnnotation("sr");

////////////////////////////////////////////////////////////////////////////////

void TraceEvent(
    const TTraceContext& context,
    const TString& serviceName,
    const TString& spanName,
    const TString& annotationName)
{
    if (context.IsVerbose()) {
        TTraceManager::Get()->Enqueue(
            context,
            serviceName,
            spanName,
            annotationName);
    }
}

void TraceEvent(
    const TTraceContext& context,
    const TString& annotationKey,
    const TString& annotationValue)
{
    if (context.IsVerbose()) {
        TTraceManager::Get()->Enqueue(
            context,
            annotationKey,
            annotationValue);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

