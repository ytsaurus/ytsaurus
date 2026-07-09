#include "computation_tracer.h"

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/tracing/trace_context.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/generic/adaptor.h>
#include <util/random/random.h>

namespace NYT::NFlow {

using namespace NProfiling;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

class TComputationTracer
    : public IComputationTracer
{
public:
    TComputationTracer(TComputationContextPtr context, TComputationSpecPtr spec, TDynamicPartitionTracerSpecPtr dynamicSpec)
        : Context_(std::move(context))
        , Spec_(std::move(spec))
    {
        DoReconfigure(dynamicSpec);
        SubscribeReconfigured(BIND(&TComputationTracer::DoReconfigure, MakeWeak(this)));
    }

    TTraceContextPtr CreateInitTraceContext() override
    {
        auto context = CreateTraceContextFromCurrentWithNewTraceId("YTFlow.Worker.Computation.Init");
        context->SetRecorded(); // For adequate finish time.
        AddComputationProfilingTags(context);
        if (RandomNumber<double>() < TraceProbability_.load(std::memory_order::relaxed)) {
            PrepareSampledTraceContext(context);
        }
        RegisterPart("Init", context);
        return context;
    }

    TTraceContextPtr StartEpochTraceContext(i64 epochId) override
    {
        if (RootTraceContext_ && EpochInRoot_ >= EpochsInTraceContext_.load(std::memory_order::relaxed)) {
            RootTraceContext_->Finish();
            RootTraceContext_ = {};
            EpochInRoot_ = 0;
        }
        if (!RootTraceContext_) {
            RootTraceContext_ = CreateTraceContextFromCurrentWithNewTraceId("YTFlow.Worker.Computation.EpochsRoot");
            RootTraceContext_->SetRecorded(); // For adequate finish time.
            AddComputationProfilingTags(RootTraceContext_);
            if (RandomNumber<double>() < TraceProbability_.load(std::memory_order::relaxed)) {
                PrepareSampledTraceContext(RootTraceContext_);
            }
        }

        EpochTraceContext_ = RootTraceContext_->CreateChild("YTFlow.Worker.Computation.Epoch");
        EpochTraceContext_->AddTag("ytflow.epoch_id", epochId);
        EpochTraceContext_->SetLoggingTag(Format("%v, EpochId: %v", RootTraceContext_->GetLoggingTag(), epochId));
        YT_VERIFY(!EpochTraceContext_->IsFinished());
        ++EpochInRoot_;
        RegisterPart("Unknown", EpochTraceContext_);
        return EpochTraceContext_;
    }

    TTraceContextPtr CreateEpochPartTraceContext(TStringBuf partName) override
    {
        const std::string name{partName};
        YT_VERIFY(EpochTraceContext_);
        YT_VERIFY(!EpochTraceContext_->IsFinished());
        auto parentTraceContext = EpochTraceContext_;

        // If current trace context is from our hierarchy, then take it instead of epoch root.
        if (const auto* currentTraceContext = TryGetCurrentTraceContext();
            currentTraceContext && currentTraceContext != parentTraceContext.Get())
        {
            auto guard = Guard(Lock_);
            for (const auto& [traceContext, partPtr] : Reversed(Parts_)) {
                if (traceContext.Get() == currentTraceContext) {
                    parentTraceContext = traceContext;
                    break;
                }
            }
        }

        auto partTraceContext = parentTraceContext->CreateChild(Format("%v.%v", parentTraceContext->GetSpanName(), name));
        RegisterPart(name, partTraceContext);
        return partTraceContext;
    }

    THashMap<std::string, TPartState> GetPartStates() override
    {
        auto guard = Guard(Lock_);
        Flush(guard);

        const TInstant now = GetInstant();
        const auto halfDecayPeriod = TDuration::Seconds(WallTimeHalfDecayPeriodSeconds_.load(std::memory_order::relaxed));
        for (auto& [partName, part] : PartStates_) {
            DecayWallTime(part, now, halfDecayPeriod);
        }

        return PartStates_;
    }

private:
    const TComputationContextPtr Context_;
    const TComputationSpecPtr Spec_;

    //! Reconfigurable.
    std::atomic<double> TraceProbability_ = 0;
    std::atomic<int> EpochsInTraceContext_ = 0;
    std::atomic<double> WallTimeHalfDecayPeriodSeconds_ = 0;

    int EpochInRoot_ = 0;
    TTraceContextPtr RootTraceContext_ = {};
    TTraceContextPtr EpochTraceContext_ = {};

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    // Guarded fields.
    TInstant FlushInstant_ = TInstant::Zero();
    THashMap<std::string, TPartState> PartStates_;
    std::list<std::pair<TTraceContextPtr, TPartState*>> Parts_;

private:
    void DoReconfigure(const TDynamicPartitionTracerSpecPtr& tracerSpec)
    {
        EpochsInTraceContext_.store(tracerSpec->EpochsInTraceContext, std::memory_order::relaxed);
        TraceProbability_.store(
            GetOrDefault(
                tracerSpec->TraceProbabilityPartitionOverride,
                Context_->Partition->PartitionId,
                tracerSpec->TraceProbability),
            std::memory_order::relaxed);
        // WallTimeHalfDecayPeriod is validated in yson struct checks. But check it explicitly here to avoid division by zero.
        YT_VERIFY(tracerSpec->WallTimeHalfDecayPeriod >= TDuration::MicroSeconds(2));
        WallTimeHalfDecayPeriodSeconds_.store(tracerSpec->WallTimeHalfDecayPeriod.SecondsFloat(), std::memory_order::relaxed);
    }

    void PrepareSampledTraceContext(const TTraceContextPtr& context)
    {
        context->SetSampled();
        context->SetPropagated(false);
        context->AddTag("ytflow.computation_id", Context_->Partition->ComputationId);
        context->AddTag("ytflow.computation_class_name", Spec_->ComputationClassName);
        context->AddTag("ytflow.partition", Context_->Partition->PartitionId);
    }

    // Profiling tags are attached to CPU profiler samples as pprof labels, so that flamegraphs
    // can be sliced by computation. Unlike the sampled tracing tags above, they must be set on
    // every trace context regardless of the trace sampling probability to cover all CPU samples.
    void AddComputationProfilingTags(const TTraceContextPtr& context)
    {
        context->AddProfilingTag("ytflow.computation_id", std::string{Context_->Partition->ComputationId.Underlying()});
        context->AddProfilingTag("ytflow.computation_class_name", Spec_->ComputationClassName);
    }

    void RegisterPart(const std::string& partName, TTraceContextPtr context)
    {
        auto guard = Guard(Lock_);
        Flush(guard);
        auto [it, emplaced] = PartStates_.try_emplace(partName);
        if (emplaced) {
            it->second.Timer = Context_->Profiler.WithTag("part", partName).Timer("/epoch_parts_time_distribution");
        }
        Parts_.push_back({std::move(context), &it->second});
        guard.Release();
        if (emplaced) {
            // Use func counter to provide properties:
            // 1) If one part hungs, this part is continuously reported.
            // 2) Metric is as smooth as possible.
            Context_->Profiler.WithTag("part", partName).AddFuncCounter("/epoch_parts_time", MakeStrong(this), [this, partPtr = &it->second] {
                auto guard = Guard(Lock_);
                Flush(guard);
                return partPtr->TotalDuration.MilliSeconds();
            });
        }
    }

    double GetWallTimeDecayCoefficient(TDuration delta, TDuration halfDecayPeriod) const
    {
        return std::pow(0.5, delta / halfDecayPeriod);
    }

    void DecayWallTime(TPartState& part, TInstant instant, TDuration halfDecayPeriod) const
    {
        part.WallTimeEma *= GetWallTimeDecayCoefficient(instant - part.WallTimeUpdateTime, halfDecayPeriod);
        part.WallTimeUpdateTime = instant;
    }

    void Flush(TGuard<NThreading::TSpinLock>& guard)
    {
        YT_ASSERT(guard.WasAcquired());
        TInstant now = GetInstant();

        auto halfDecayPeriod = TDuration::Seconds(WallTimeHalfDecayPeriodSeconds_.load(std::memory_order::relaxed));

        auto incrementDurations = [&] (auto& part, TInstant instant) {
            const auto delta = instant - FlushInstant_;
            part.TotalDuration += delta;

            DecayWallTime(part, instant, halfDecayPeriod);
            part.WallTimeEma += halfDecayPeriod / std::log(2.) * (1 - GetWallTimeDecayCoefficient(delta, halfDecayPeriod));
        };

        // We account time for active span that was added most recently.
        while (!Parts_.empty()) {
            auto it = Parts_.end();
            --it;
            auto context = it->first;
            if (!context->IsFinished()) {
                if (now > FlushInstant_) {
                    incrementDurations(*it->second, now);
                    it->second->MaxDuration = std::max(it->second->MaxDuration, now - context->GetStartTime());
                    FlushInstant_ = now;
                }
                break;
            }
            it->second->MaxDuration = std::max(it->second->MaxDuration, context->GetDuration());
            it->second->Timer.Record(context->GetDuration());
            auto finishTime = context->GetStartTime() + context->GetDuration();
            if (finishTime > FlushInstant_) {
                incrementDurations(*it->second, finishTime);
                FlushInstant_ = finishTime;
            }
            Parts_.erase(it);
        }
        if (now > FlushInstant_) {
            FlushInstant_ = now;
        }
        // Cleanup old not interesting spans.
        for (auto it = Parts_.begin(); it != Parts_.end();) {
            auto context = it->first;
            if (context->IsFinished()) {
                Parts_.erase(it++);
            } else {
                ++it;
            }
        }
    }

    TTraceContextPtr CreateTraceContextFromCurrentWithNewTraceId(const std::string& spanName)
    {
        auto* context = TryGetCurrentTraceContext();
        if (!context) {
            return TTraceContext::NewRoot(spanName);
        }

        // NB: We manually replace trace id, because top-level trace context is used to collect
        // job's performance metrics and is active as long as job exists. By generating new trace id
        // we avoid flooding the tracing system with a huge amount of spans with the same trace id.
        auto spanContext = context->GetSpanContext();
        spanContext.TraceId = TTraceId::Create();

        auto child = New<TTraceContext>(
            spanContext,
            spanName,
            /*parentTraceContext*/ context,
            /*startTime*/ std::nullopt);
        child->SetProfilingTags(context->GetProfilingTags());
        child->SetTargetEndpoint(context->GetTargetEndpoint());
        child->SetAllocationTagList(context->GetAllocationTagList());
        child->SetLoggingTag(context->GetLoggingTag());

        return child;
    }
};

IComputationTracerPtr CreateComputationTracer(
    TComputationContextPtr context,
    TComputationSpecPtr spec,
    TDynamicPartitionTracerSpecPtr dynamicSpec)
{
    return New<TComputationTracer>(std::move(context), std::move(spec), std::move(dynamicSpec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
