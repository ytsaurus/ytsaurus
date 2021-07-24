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
    TTraceContextPtr parentTraceContext)
    : TraceId_(parentSpanContext.TraceId)
    , SpanId_(GenerateSpanId())
    , ParentSpanId_(parentSpanContext.SpanId)
    , Debug_(parentSpanContext.Debug)
    , State_(parentTraceContext
        ? parentTraceContext->State_.load()
        : (parentSpanContext.Sampled ? ETraceContextState::Sampled : ETraceContextState::Disabled))
    , ParentContext_(std::move(parentTraceContext))
    , SpanName_(std::move(spanName))
    , RequestId_(ParentContext_ ? ParentContext_->GetRequestId() : TRequestId{})
    , LoggingTag_(ParentContext_ ? ParentContext_->GetLoggingTag() : TString{})
    , StartTime_(GetCpuInstant())
{

}

void TTraceContext::SetRequestId(TRequestId requestId)
{
    RequestId_ = requestId;
}

void TTraceContext::SetLoggingTag(const TString& loggingTag)
{
    LoggingTag_ = loggingTag;
}

void TTraceContext::SetRecorded()
{
    auto disabled = ETraceContextState::Disabled;
    State_.compare_exchange_strong(disabled, ETraceContextState::Recorded);
}

TTraceContextPtr TTraceContext::CreateChild(
    TString spanName)
{
    return New<TTraceContext>(
        GetSpanContext(),
        std::move(spanName),
        /* parentTraceContext */ this);
}

TSpanContext TTraceContext::GetSpanContext() const
{
    return TSpanContext{
        .TraceId = GetTraceId(),
        .SpanId = GetSpanId(),
        .Sampled = IsSampled(),
        .Debug = Debug_,
    };
}

TDuration TTraceContext::GetElapsedTime() const
{
    return CpuDurationToDuration(GetElapsedCpuTime());
}

void TTraceContext::SetSampled(bool value)
{
    if (!value) {
        State_ = ETraceContextState::Disabled;
    } else {
        State_ = ETraceContextState::Sampled;
    }
}

TInstant TTraceContext::GetStartTime() const
{
    return NProfiling::CpuInstantToInstant(StartTime_);
}

TDuration TTraceContext::GetDuration() const
{
    YT_ASSERT(Finished_.load());
    return NProfiling::CpuDurationToDuration(Duration_.load());
}

TTraceContext::TTagList TTraceContext::GetTags() const
{
    auto guard = Guard(Lock_);
    return Tags_;
}

TTraceContext::TLogList TTraceContext::GetLogEntries() const
{
    auto guard = Guard(Lock_);
    return Logs_;
}

TTraceContext::TAsyncChildrenList TTraceContext::GetAsyncChildren() const
{
    auto guard = Guard(Lock_);
    return AsyncChildren_;
}

void TTraceContext::AddTag(const TString& tagKey, const TString& tagValue)
{
    if (!IsRecorded()) {
        return;
    }

    if (Finished_.load()) {
        return;
    }

    auto guard = Guard(Lock_);
    Tags_.emplace_back(tagKey, tagValue);
}

void TTraceContext::AddAsyncChild(const TTraceId& traceId)
{
    if (!IsRecorded()) {
        return;
    }

    if (Finished_.load()) {
        return;
    }

    auto guard = Guard(Lock_);
    AsyncChildren_.push_back(traceId);
}

void TTraceContext::AddErrorTag()
{
    if (!IsRecorded()) {
        return;
    }

    static const TString ErrorAnnotationName("error");
    static const TString ErrorAnnotationValue("true");
    AddTag(ErrorAnnotationName, ErrorAnnotationValue);
}

void TTraceContext::AddLogEntry(TCpuInstant at, TString message)
{
    if (!IsRecorded()) {
        return;
    }

    if (Finished_.load()) {
        return;
    }

    auto guard = Guard(Lock_);
    Logs_.push_back(TTraceLogEntry{at, std::move(message)});
}

bool TTraceContext::IsFinished()
{
    return Finished_.load();
}

bool TTraceContext::IsSampled() const
{
    auto traceContext = this;
    while (traceContext) {
        auto state = traceContext->State_.load(std::memory_order_relaxed);
        if (state == ETraceContextState::Sampled) {
            return true;
        } else if (state == ETraceContextState::Disabled) {
            return false;
        }

        traceContext = traceContext->ParentContext_.Get();
    }

    return false;
}

void TTraceContext::SetDuration()
{
    if (Duration_.load() == 0) {
        Duration_ = GetCpuInstant() - StartTime_;
    }
}

void TTraceContext::Finish()
{
    if (Finished_.exchange(true)) {
        return;
    }
    SetDuration();

    auto state = State_.load(std::memory_order_relaxed);
    if (state == ETraceContextState::Disabled) {
        return;
    } else if (state == ETraceContextState::Sampled) {
        if (auto tracer = GetGlobalTracer(); tracer) {
            tracer->Enqueue(MakeStrong(this));
        }
    } else if (state == ETraceContextState::Recorded) {
        if (!IsSampled()) {
            return;
        }

        if (auto tracer = GetGlobalTracer(); tracer) {
            auto traceContext = this;
            while (traceContext) {
                if (traceContext->State_.load() != ETraceContextState::Recorded) {
                    break;
                }

                if (traceContext->Finished_.load() && !traceContext->Submitted_.exchange(true)) {
                    traceContext->SetDuration();
                    tracer->Enqueue(MakeStrong(traceContext));
                }

                traceContext = traceContext->ParentContext_.Get();
            }
        }
    }
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

TTraceContextPtr TTraceContext::NewRoot(TString spanName)
{
    return New<TTraceContext>(
        TSpanContext{
            .TraceId = TTraceId::Create(),
            .SpanId = InvalidSpanId,
            .Sampled = false,
            .Debug = false,
        },
        std::move(spanName));
}

TTraceContextPtr TTraceContext::NewChildFromSpan(
    TSpanContext parentSpanContext,
    TString spanName)
{
    return New<TTraceContext>(
        parentSpanContext,
        std::move(spanName));
}

TTraceContextPtr TTraceContext::NewChildFromRpc(
    const NProto::TTracingExt& ext,
    TString spanName,
    TRequestId requestId,
    bool forceTracing)
{
    auto traceId = FromProto<TTraceId>(ext.trace_id());
    if (!traceId) {
        if (!forceTracing) {
            return nullptr;
        }

        auto root = NewRoot(std::move(spanName));
        root->SetRequestId(requestId);
        root->SetRecorded();
        return root;
    }

    auto traceContext = New<TTraceContext>(
        TSpanContext{
            traceId,
            ext.span_id(),
            ext.sampled(),
            ext.debug()
        },
        std::move(spanName));
    traceContext->SetRequestId(requestId);
    return traceContext;
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

