#include "trace_context.h"
#include "private.h"

#include <yt/core/profiling/timing.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/shutdown.h>
#include <yt/core/misc/singleton.h>

#include <yt/core/tracing/proto/tracing_ext.pb.h>

#include <yt/yt/library/tracing/tracer.h>

namespace NYT::NTracing {

using namespace NConcurrency;
using namespace NProfiling;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TracingLogger;

////////////////////////////////////////////////////////////////////////////////

struct TGlobalTracer
{
    TSpinLock Lock;
    ITracerPtr Tracer;
};

// TODO(prime@): Switch constinit global variable, once gcc supports it.
static TGlobalTracer* GlobalTracerStorage()
{
    return LeakySingleton<TGlobalTracer>();
}

ITracerPtr GetGlobalTracer()
{
    auto tracerStorage = GlobalTracerStorage();
    auto guard = Guard(tracerStorage->Lock);
    return tracerStorage->Tracer;
}

void SetGlobalTracer(const ITracerPtr& tracer)
{
    ITracerPtr oldTracer;

    {
        auto tracerStorage = GlobalTracerStorage();
        auto guard = Guard(tracerStorage->Lock);
        oldTracer = tracerStorage->Tracer;
        tracerStorage->Tracer = tracer;
    }

    if (oldTracer) {
        oldTracer->Stop();
    }
}

void ShutdownTracer()
{
    SetGlobalTracer(nullptr);
}

REGISTER_SHUTDOWN_CALLBACK(8, ShutdownTracer)

////////////////////////////////////////////////////////////////////////////////

namespace  {

TSpanId GenerateSpanId()
{
    return RandomNumber<ui64>(std::numeric_limits<ui64>::max() - 1) + 1;
}

} // namespace

TSpanContext TSpanContext::CreateChild()
{
    return {
        TraceId,
        GenerateSpanId(),
        Sampled,
        Debug,
    };
}

void AddErrorTag()
{
    static const TString ErrorAnnotationName("error");
    static const TString ErrorAnnotationValue("true");
    AddTag(ErrorAnnotationName, ErrorAnnotationValue);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TSpanContext& context, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v:%08" PRIx64 ":%v",
        context.TraceId,
        context.SpanId,
        (context.Sampled ? 1u : 0) | (context.Debug ? 2u : 0));
}

TString ToString(const TSpanContext& context)
{
    return ToStringViaBuilder(context);
}

////////////////////////////////////////////////////////////////////////////////

TTraceContext::TTraceContext(TSpanContext parentSpanContext, TString spanName, TTraceContextPtr parentTraceContext)
    : ParentSpanId_(parentSpanContext.SpanId)
    , ParentContext_(std::move(parentTraceContext))
    , SpanName_(std::move(spanName))
    , StartTime_(GetCpuInstant())
    , SpanContext_(parentSpanContext.CreateChild())
{ }

TTraceContext::TTraceContext(TSpanContext parentSpanContext, TString spanName, TRequestId requestId)
    : ParentSpanId_(parentSpanContext.SpanId)
    , RequestId_(requestId)
    , SpanName_(std::move(spanName))
    , StartTime_(GetCpuInstant())
    , SpanContext_(parentSpanContext.CreateChild())
{ }

TTraceContext::TTraceContext(TFollowsFrom, TSpanContext parent, TString spanName, TTraceContextPtr parentTraceContext)
    : FollowsFromSpanId_(parent.SpanId)
    , ParentContext_(std::move(parentTraceContext))
    , SpanName_(std::move(spanName))
    , StartTime_(GetCpuInstant())
    , SpanContext_(parent.CreateChild())
{ }

TTraceContextPtr TTraceContext::CreateChild(const TString& name)
{
    return New<TTraceContext>(SpanContext_, name, this);
}

TDuration TTraceContext::GetElapsedTime() const
{
    return CpuDurationToDuration(GetElapsedCpuTime());
}

void TTraceContext::SetSampled(bool value)
{
    auto guard = Guard(Lock_);
    SpanContext_.Sampled = value;
}

void TTraceContext::AddTag(const TString& tagKey, const TString& tagValue)
{
    auto guard = Guard(Lock_);
    if (Finished_) {
        return;
    }
    Tags_.emplace_back(tagKey, tagValue);
}

void TTraceContext::ResetStartTime()
{
    auto guard = Guard(Lock_);
    StartTime_ = GetCpuInstant();
}

TInstant TTraceContext::GetStartTime() const
{
    auto guard = Guard(Lock_);
    return NProfiling::CpuInstantToInstant(StartTime_);
}

TDuration TTraceContext::GetDuration() const
{
    auto guard = Guard(Lock_);
    return NProfiling::CpuDurationToDuration(Duration_);
}

TTraceContext::TTagList TTraceContext::GetTags() const
{
    auto guard = Guard(Lock_);
    return Tags_;
}

void TTraceContext::Finish()
{
    auto sampled = false;
    {
        auto guard = Guard(Lock_);
        if (Finished_) {
            return;
        }

        Finished_ = true;
        sampled = SpanContext_.Sampled;
        Duration_ = GetCpuInstant() - StartTime_;
    }

    if (sampled) {
        if (auto tracer = GetGlobalTracer(); tracer) {
            tracer->Enqueue(MakeStrong(this));
        }
    }
}

void FormatValue(TStringBuilderBase* builder, const TTraceContextPtr& context, TStringBuf /*spec*/)
{
    if (context) {
        builder->AppendFormat("%v %v",
            context->GetSpanName(),
            context->GetSpanContext());
    } else {
        builder->AppendString(TStringBuf("<null>"));
    }
}

TString ToString(const TTraceContextPtr& context)
{
    return ToStringViaBuilder(context);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TTracingExt* ext, const TTraceContextPtr& context)
{
    if (!context) {
        ext->Clear();
        return;
    }

    ToProto(ext->mutable_trace_id(), context->GetTraceId());
    ext->set_span_id(context->GetSpanId());
    ext->set_sampled(context->IsSampled());
    ext->set_debug(context->IsDebug());
}

TTraceContextPtr CreateRootTraceContext(
    const TString& spanName,
    TRequestId requestId)
{
    return New<TTraceContext>(
        TSpanContext{TTraceId::Create(), InvalidSpanId, false, false},
        spanName,
        requestId);
}

TTraceContextPtr CreateChildTraceContext(
    const TTraceContextPtr& parentContext,
    const TString& spanName,
    bool forceTracing)
{
    if (parentContext) {
        return parentContext->CreateChild(spanName);
    }

    if (!forceTracing) {
        return nullptr;
    }

    auto newContext = CreateRootTraceContext(spanName);
    newContext->SetSampled();
    return newContext;
}

TTraceContextPtr CreateChildTraceContext(
    const NProto::TTracingExt& ext,
    const TString& spanName,
    TRequestId requestId,
    bool forceTracing)
{
    auto traceId = FromProto<TTraceId>(ext.trace_id());
    if (!traceId) {
        if (!forceTracing) {
            return nullptr;
        }
        return CreateRootTraceContext(spanName, requestId);
    }

    TSpanContext spanContext{
        traceId,
        ext.span_id(),
        ext.sampled(),
        ext.debug()
    };

    return New<TTraceContext>(spanContext, spanName, requestId);
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

thread_local TTraceContext* CurrentTraceContext;
thread_local TCpuInstant TraceContextTimingCheckpoint;
static thread_local TCurrentTraceContextReclaimer CurrentTraceContextReclaimer;

TTraceContextPtr SwitchTraceContext(TTraceContextPtr newContext, NProfiling::TCpuInstant now)
{
    auto oldContext = TTraceContextPtr(CurrentTraceContext, false);

    // Invalid if no oldContext
    auto delta = now - TraceContextTimingCheckpoint;

    if (oldContext && newContext) {
        YT_LOG_TRACE("Switching context (OldContext: %v, NewContext: %v, CpuTimeDelta: %v)",
            oldContext,
            newContext,
            NProfiling::CpuDurationToDuration(delta));
    } else if (oldContext) {
        YT_LOG_TRACE("Uninstalling context (Context: %v, CpuTimeDelta: %v)",
            oldContext,
            NProfiling::CpuDurationToDuration(delta));
    } else if (newContext) {
        YT_LOG_TRACE("Installing context (Context: %v)",
            newContext);
    }

    if (oldContext) {
        oldContext->IncrementElapsedCpuTime(delta);
    }

    CurrentTraceContext = newContext.Release();
    TraceContextTimingCheckpoint = now;

    return oldContext;
}

TTraceContextPtr SwitchTraceContext(TTraceContextPtr newContext)
{
    return SwitchTraceContext(newContext, GetCpuInstant());
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

void TTraceContext::IncrementElapsedCpuTime(NProfiling::TCpuDuration delta)
{
    auto* current = this;
    while (current) {
        current->ElapsedCpuTime_ += delta;
        current = current->ParentContext_.Get();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

