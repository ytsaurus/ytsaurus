#include "trace_context.h"
#include "private.h"
#include "trace_manager.h"

#include <yt/core/profiling/timing.h>

namespace NYT::NTracing {

using namespace NConcurrency;
using namespace NProfiling;

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

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TSpanContext spanContext, TStringBuf spec)
{
    int flags = (spanContext.Sampled ? 1 : 0) | (spanContext.Debug ? 2 : 0);

    builder->AppendFormat("%08" PRIx64 "%08" PRIx64 ":%08" PRIx64 ":%08" PRIx64 ":%d",
        spanContext.TraceId.Parts64[1],
        spanContext.TraceId.Parts64[0],
        spanContext.SpanId,
        flags);
}

TString ToString(TSpanContext spanContext)
{
    return ToStringViaBuilder(spanContext);
}

////////////////////////////////////////////////////////////////////////////////

TTraceContext::TTraceContext(TSpanContext parent, const TString& name, TTraceContextPtr parentContext)
    : ParentSpanId_(parent.SpanId)
    , StartTime_(GetCpuInstant())
    , SpanContext_(parent.CreateChild())
    , Name_(name)
    , ParentContext_(parentContext)
{ }

TTraceContext::TTraceContext(TFollowsFrom, TSpanContext parent, const TString& name, TTraceContextPtr parentContext)
    : FollowsFromSpanId_(parent.SpanId)
    , StartTime_(GetCpuInstant())
    , SpanContext_(parent.CreateChild())
    , Name_(name)
    , ParentContext_(parentContext)
{ }

TTraceContextPtr TTraceContext::CreateChild(const TString& name)
{
    return New<TTraceContext>(SpanContext_, name, this);
}

TDuration TTraceContext::GetElapsedTime() const
{
    return CpuDurationToDuration(GetElapsedCpuTime());
}

void TTraceContext::SetName(const TString& name)
{
    auto guard = Guard(Lock_);
    Name_ = name;
}

void TTraceContext::SetSampled()
{
    auto guard = Guard(Lock_);
    SpanContext_.Sampled = true;
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

////////////////////////////////////////////////////////////////////////////////

TTraceContextPtr CreateRootTraceContext(const TString& name)
{
    TSpanContext context{TTraceId::Create(), InvalidSpanId, false, false};
    return New<TTraceContext>(context, name);
}

TTraceContextPtr CreateChildTraceContext(const TString& spanName, bool forceTracing)
{
    if (auto context = GetCurrentTraceContext()) {
        return context->CreateChild(spanName);
    } else {
        if (!forceTracing) {
            return nullptr;
        } else {
            auto newContext = CreateRootTraceContext(spanName);
            newContext->SetSampled();
            return newContext;
        }
    }
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

TString ToString(const TTraceContextPtr& context)
{
    if (!context) {
        static TString Null("<null>");
        return Null;
    }

    return ToString(context->GetContext());
}

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
    : TraceContextGuard_(CreateChildTraceContext(spanName, forceTracing))
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

