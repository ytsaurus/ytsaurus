#include "trace_context.h"
#include "private.h"
#include "trace_manager.h"

#include <yt/core/profiling/timing.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/tracing/proto/tracing_ext.pb.h>

namespace NYT::NTracing {

using namespace NConcurrency;
using namespace NProfiling;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TracingLogger;

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
    AddTag("error", TString("true"));
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

TTraceContext::TTraceContext(TSpanContext parentSpanContext, const TString& spanName, TTraceContextPtr parentTraceContext)
    : ParentSpanId_(parentSpanContext.SpanId)
    , StartTime_(GetCpuInstant())
    , SpanContext_(parentSpanContext.CreateChild())
    , SpanName_(spanName)
    , ParentContext_(std::move(parentTraceContext))
{ }

TTraceContext::TTraceContext(TFollowsFrom, TSpanContext parent, const TString& spanName, TTraceContextPtr parentTraceContext)
    : FollowsFromSpanId_(parent.SpanId)
    , StartTime_(GetCpuInstant())
    , SpanContext_(parent.CreateChild())
    , SpanName_(spanName)
    , ParentContext_(std::move(parentTraceContext))
{ }

TTraceContextPtr TTraceContext::CreateChild(const TString& name)
{
    return New<TTraceContext>(SpanContext_, name, this);
}

TDuration TTraceContext::GetElapsedTime() const
{
    return CpuDurationToDuration(GetElapsedCpuTime());
}

void TTraceContext::SetSpanName(const TString& spanName)
{
    auto guard = Guard(Lock_);
    SpanName_ = spanName;
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

const TTraceContext::TTagList& TTraceContext::GetTags() const
{
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
        TTraceManager::Get()->Enqueue(MakeStrong(this));
    }
}

void FormatValue(TStringBuilderBase* builder, const TTraceContextPtr& context, TStringBuf /*spec*/)
{
    if (context) {
        builder->AppendFormat("%v %v",
            context->GetSpanName(),
            context->GetSpanContext());
    } else {
        builder->AppendString(AsStringBuf("<null>"));
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

TTraceContextPtr CreateRootTraceContext(const TString& spanName)
{
    return New<TTraceContext>(
        TSpanContext{TTraceId::Create(), InvalidSpanId, false, false},
        spanName);
}

TTraceContextPtr CreateChildTraceContext(const TTraceContextPtr& parentContext, const TString& spanName, bool forceTracing)
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

TTraceContextPtr CreateChildTraceContext(const NProto::TTracingExt& ext, const TString& spanName, bool forceTracing)
{
    auto traceId = FromProto<TTraceId>(ext.trace_id());
    if (!traceId) {
        if (!forceTracing) {
            return nullptr;
        }
        return CreateRootTraceContext(spanName);
    }

    TSpanContext spanContext{
        traceId,
        ext.span_id(),
        ext.sampled(),
        ext.debug()
    };

    return New<TTraceContext>(spanContext, spanName);
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
thread_local TTraceId CurrentTraceId;
thread_local TCpuInstant TraceContextTimingCheckpoint;
static thread_local TCurrentTraceContextReclaimer CurrentTraceContextReclaimer;

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
    YT_ASSERT(!CurrentTraceContext);
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

void TTraceContext::IncrementElapsedCpuTime(NProfiling::TCpuDuration delta)
{
    auto* current = this;
    while (current) {
        ElapsedCpuTime_ += delta;
        current = current->ParentContext_.Get();
    }
}

////////////////////////////////////////////////////////////////////////////////

TChildTraceContextGuard::TChildTraceContextGuard(
    const TString& spanName,
    bool forceTracing)
    : TraceContextGuard_(CreateChildTraceContext(
        GetCurrentTraceContext(),
        spanName,
        forceTracing))
    , FinishGuard_(GetCurrentTraceContext())
{ }

////////////////////////////////////////////////////////////////////////////////

TTraceContextFinishGuard::TTraceContextFinishGuard(TTraceContextPtr traceContext)
    : TraceContext_(std::move(traceContext))
{ }

TTraceContextFinishGuard::~TTraceContextFinishGuard()
{
    if (TraceContext_) {
        TraceContext_->Finish();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

