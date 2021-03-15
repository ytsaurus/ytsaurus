#include "trace_context.h"
#include "private.h"

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/singleton.h>

#include <yt/yt_proto/yt/core/tracing/proto/tracing_ext.pb.h>

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

TTraceContext::TTraceContext(
    TSpanContext parentSpanContext,
    TString spanName,
    TRequestId requestId,
    TString loggingTag,
    TTraceContextPtr parentTraceContext)
    : TraceId_(parentSpanContext.TraceId)
    , SpanId_(GenerateSpanId())
    , ParentSpanId_(parentSpanContext.SpanId)
    , Sampled_(parentSpanContext.Sampled)
    , Debug_(parentSpanContext.Debug)
    , ParentContext_(std::move(parentTraceContext))
    , SpanName_(std::move(spanName))
    , RequestId_(requestId)
    , LoggingTag_(loggingTag || !ParentContext_? std::move(loggingTag) : ParentContext_->GetLoggingTag())
    , StartTime_(GetCpuInstant())
{ }

TTraceContextPtr TTraceContext::CreateChild(
    TString spanName,
    TString loggingTag)
{
    return New<TTraceContext>(
        GetSpanContext(),
        std::move(spanName),
        /* requestId */ TRequestId(),
        std::move(loggingTag),
        /* parentTraceContext */ this);
}

TSpanContext TTraceContext::GetSpanContext() const
{
    return TSpanContext{
        .TraceId = GetTraceId(),
        .SpanId = GetSpanId(),
        .Sampled = IsSampled(),
        .Debug = IsDebug()
    };
}

TDuration TTraceContext::GetElapsedTime() const
{
    return CpuDurationToDuration(GetElapsedCpuTime());
}

void TTraceContext::SetSampled(bool value)
{
    Sampled_.store(true);
}

TInstant TTraceContext::GetStartTime() const
{
    return NProfiling::CpuInstantToInstant(StartTime_);
}

TDuration TTraceContext::GetDuration() const
{
    YT_ASSERT(Finished_.load());
    return NProfiling::CpuDurationToDuration(Duration_);
}

TTraceContext::TTagList TTraceContext::GetTags() const
{
    auto guard = Guard(TagsLock_);
    return Tags_;
}

void TTraceContext::AddTag(const TString& tagKey, const TString& tagValue)
{
    if (Finished_.load()) {
        return;
    }
    auto guard = Guard(TagsLock_);
    Tags_.emplace_back(tagKey, tagValue);
}

void TTraceContext::Finish()
{
    if (Finished_.exchange(true)) {
        return;
    }

    Duration_ = GetCpuInstant() - StartTime_;

    if (IsSampled()) {
        if (auto tracer = GetGlobalTracer(); tracer) {
            tracer->Enqueue(MakeStrong(this));
        }
    }
}

void TTraceContext::AddErrorTag()
{
    static const TString ErrorAnnotationName("error");
    static const TString ErrorAnnotationValue("true");
    AddTag(ErrorAnnotationName, ErrorAnnotationValue);
}

////////////////////////////////////////////////////////////////////////////////

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
    TString spanName,
    TRequestId requestId,
    TString loggingTag,
    bool sampled,
    bool debug)
{
    return New<TTraceContext>(
        TSpanContext{
            .TraceId = TTraceId::Create(),
            .SpanId = InvalidSpanId,
            .Sampled = sampled,
            .Debug = debug
        },
        std::move(spanName),
        requestId,
        std::move(loggingTag));
}

TTraceContextPtr CreateChildTraceContext(
    const TTraceContextPtr& parentContext,
    TString spanName,
    TString loggingTag,
    bool forceTracing)
{
    if (parentContext) {
        return parentContext->CreateChild(
            std::move(spanName),
            std::move(loggingTag));
    }

    if (!forceTracing) {
        return nullptr;
    }

    return CreateRootTraceContext(
        std::move(spanName),
        /* requestId */ {},
        /* loggingTag */ std::move(loggingTag),
        /* sampled */ true,
        /* debug */ false);
}

TTraceContextPtr CreateChildTraceContext(
    const NProto::TTracingExt& ext,
    TString spanName,
    TRequestId requestId,
    TString loggingTag,
    bool forceTracing)
{
    auto traceId = FromProto<TTraceId>(ext.trace_id());
    if (!traceId) {
        if (!forceTracing) {
            return nullptr;
        }
        return CreateRootTraceContext(
            std::move(spanName),
            requestId,
            std::move(loggingTag));
    }

    return New<TTraceContext>(
        TSpanContext{
            traceId,
            ext.span_id(),
            ext.sampled(),
            ext.debug()
        },
        std::move(spanName),
        requestId,
        std::move(loggingTag));
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

