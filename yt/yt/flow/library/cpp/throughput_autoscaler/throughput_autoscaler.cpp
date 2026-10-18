#include "throughput_autoscaler.h"

#include <yt/yt/core/misc/collection_helpers.h>

#include <algorithm>
#include <cmath>
#include <functional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

const NLogging::TLogger Logger("FlowThroughput");

std::optional<double> FilterInvalidValue(std::optional<double> value)
{
    return value && std::isfinite(*value) && *value >= 0 ? value : std::nullopt;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TThroughputAutoscaler::InitializeComputation(
    const TComputationId& computationId,
    TThroughputFeedbackState state,
    NYson::TYsonString demandHistory)
{
    DemandHistories_.try_emplace(computationId, std::move(demandHistory));
    EmplaceOrCrash(State_.Policies, computationId, TThroughputFeedbackPolicy(std::move(state), /*config*/ nullptr, /*partitioningSpec*/ nullptr, Logger.WithTag("ComputationId", computationId)));
}

const TThroughputFeedbackState& TThroughputAutoscaler::GetFeedbackState(const TComputationId& computationId) const
{
    return GetOrCrash(State_.Policies, computationId).GetState();
}

const NYson::TYsonString& TThroughputAutoscaler::SaveDemandHistory(const TComputationId& computationId) const
{
    return GetOrCrash(DemandHistories_, computationId).Save();
}

void TThroughputAutoscaler::BeginIteration()
{
    if (PreviousState_) {
        State_ = std::move(*PreviousState_);
        PreviousState_.reset();
    }
    Computations_.clear();
    Prepared_.clear();
    Decisions_.clear();
}

void TThroughputAutoscaler::Commit()
{
    if (PreviousState_) {
        EraseNodesIf(DemandHistories_, [&] (const auto& item) {
            return !Computations_.contains(item.first);
        });
    }
    PreviousState_.reset();
}

TError TThroughputAutoscaler::CheckComputationStatus(const TComputationId& computationId) const
{
    const auto& feedback = GetFeedbackState(computationId);
    const auto* computation = Computations_.FindPtr(computationId);
    const bool scalable = computation && IsScalable(*computation);
    if (scalable && (feedback.Phase == EThroughputSearchPhase::RatePlateau || feedback.Phase == EThroughputSearchPhase::ExplorationLimited)) {
        const auto telemetry = FindTelemetry(computationId);
        if (telemetry && telemetry->Demand && telemetry->Capacity && *telemetry->Demand <= *telemetry->Capacity) {
            return {};
        }
        return TError("Throughput exploration stopped without further supported processing progress")
            .With("phase", feedback.Phase)
            .With("origin_partition_count", feedback.Exploration->OriginPartitionCount)
            .With("origin_processed", feedback.Exploration->OriginProcessed)
            .With("best_processed", feedback.Exploration->BestProcessed);
    }
    const auto telemetry = FindTelemetry(computationId);
    if (telemetry && !telemetry->Repartitioning && telemetry->MaxPartitionCount &&
        telemetry->PartitionCount >= *telemetry->MaxPartitionCount &&
        telemetry->Demand && telemetry->Capacity && *telemetry->Demand > *telemetry->Capacity)
    {
        return TError("Throughput demand exceeds capacity at the partition count limit")
            .With("partition_count", telemetry->PartitionCount)
            .With("max_partition_count", *telemetry->MaxPartitionCount)
            .With("demand", *telemetry->Demand)
            .With("capacity", *telemetry->Capacity);
    }
    if (!scalable || !feedback.CapacityDegraded) {
        return {};
    }
    return TError("Capacity fell after increasing parallelism; recalibrating the working point")
        .With("phase", feedback.Phase)
        .With("retry_warmup_period", feedback.RetryWarmupPeriod)
        .With("anchor_partition_count", feedback.Anchor ? feedback.Anchor->PartitionCount : 0);
}

void TThroughputAutoscaler::Reconfigure(TComputations computations, const TThroughputSpecPtr& config)
{
    if (!PreviousState_) {
        PreviousState_ = State_;
    }
    EraseNodesIf(State_.Policies, [&] (const auto& item) {
        return !computations.contains(item.first);
    });
    Computations_ = std::move(computations);
    for (const auto& [computationId, computation] : Computations_) {
        YT_VERIFY(computation.Spec);
        if (!State_.Policies.contains(computationId)) {
            InitializeComputation(computationId, {});
        }
        GetOrCrash(State_.Policies, computationId).Reconfigure(config, computation.PartitioningSpec);
        if (!IsScalable(computation)) {
            if (auto* runtime = State_.Computations.FindPtr(computationId)) {
                runtime->Warmup.Reset(State_.CurrentTime);
                runtime->ShortageSince.reset();
                runtime->ScaleDownRecommendation.reset();
            }
        }
    }
    EraseNodesIf(State_.Computations, [&] (const auto& item) {
        return !Computations_.contains(item.first);
    });
}

bool TThroughputAutoscaler::IsScalable(const TComputation& computation)
{
    return computation.PartitioningSpec && !computation.PartitioningSpec->DesiredPartitionCount;
}

bool TThroughputAutoscaler::HasScalableComputation() const
{
    for (const auto& [_, computation] : Computations_) {
        if (IsScalable(computation)) {
            return true;
        }
    }
    return false;
}

TThroughputMeasurementRequests TThroughputAutoscaler::ComputeMeasurementRequests() const
{
    TThroughputMeasurementRequests requests;
    for (const auto& [computationId, registered] : Computations_) {
        const auto& feedback = GetOrCrash(State_.Policies, computationId);
        auto period = feedback.GetWarmupPeriod();
        requests.emplace(
            computationId,
            TThroughputMeasurementRequest{
                .Spec = registered.Spec,
                .Period = period,
                .RecoveryPeriod = feedback.GetState().CapacityDegraded && period > feedback.GetMeasurementPeriod() ? std::optional(feedback.GetMeasurementPeriod()) : std::nullopt,
            });
    }
    return requests;
}

void TThroughputAutoscaler::PropagateDemand(const TThroughputSpecPtr& config)
{
    using TStreamDemand = TThroughputDemand;

    THashMap<TStreamId, TComputationId> producers;
    for (const auto& [computationId, registered] : Computations_) {
        for (const auto& outputStreamId : registered.Spec->OutputStreamIds) {
            producers[outputStreamId] = computationId;
        }
        auto& computation = GetOrCrash(Prepared_, computationId);
        computation.Demand = {
            .SteadyDemand = 0.0,
            .CatchUpDemand = 0.0,
            .NormalBacklog = 0.0,
            .ExcessBacklog = 0.0,
        };
    }

    auto add = [] (std::optional<double>* total, const std::optional<double>& value) {
        auto increment = FilterInvalidValue(value);
        *total = *total && increment ? FilterInvalidValue(**total + *increment) : std::nullopt;
    };

    THashMap<TStreamId, TStreamDemand> forecasts;
    THashSet<TStreamId> activeForecasts;
    THashMap<TComputationId, THashMap<TStreamId, TStreamDemand>> targets;
    THashMap<TComputationId, THashSet<TStreamId>> activeTargets;
    std::function<TStreamDemand(const TComputationId&, const TStreamId&)> getTarget;
    std::function<TStreamDemand(const TStreamId&)> getForecast;

    getTarget = [&] (const TComputationId& computationId, const TStreamId& streamId) {
        if (const auto* computationTargets = targets.FindPtr(computationId)) {
            if (const auto* target = computationTargets->FindPtr(streamId)) {
                return *target;
            }
        }
        if (!activeTargets[computationId].insert(streamId).second) {
            return TStreamDemand{};
        }

        const auto& computation = GetOrCrash(Prepared_, computationId);
        const auto& registered = GetOrCrash(Computations_, computationId);
        const auto* stream = computation.Observation.Streams.FindPtr(streamId);
        const bool fromUpstream = registered.Spec->InputStreamIds.contains(streamId) && producers.contains(streamId);
        const auto upstream = fromUpstream ? getForecast(streamId) : TStreamDemand{};
        TStreamDemand result;
        if (registered.Spec->OutputStreamIds.contains(streamId)) {
            result = getForecast(streamId);
        } else if (stream) {
            result = BuildThroughputDemand(
                config,
                stream->Offered,
                stream->Ready,
                computation.Observation.ActualBatchDuration,
                fromUpstream ? &upstream : nullptr);
        }
        activeTargets[computationId].erase(streamId);
        targets[computationId][streamId] = result;
        return result;
    };

    getForecast = [&] (const TStreamId& outputStreamId) {
        if (const auto* forecast = forecasts.FindPtr(outputStreamId)) {
            return *forecast;
        }
        const auto* producer = producers.FindPtr(outputStreamId);
        if (!producer || !activeForecasts.insert(outputStreamId).second) {
            return TStreamDemand{};
        }

        const auto& registered = GetOrCrash(Computations_, *producer);
        auto& computation = GetOrCrash(Prepared_, *producer);
        const auto* parents = registered.Spec->StreamsDependency.FindPtr(outputStreamId);
        const auto* lineageRatios = computation.Observation.LineageRatios.FindPtr(outputStreamId);
        TStreamDemand forecast;
        if (parents && lineageRatios) {
            forecast.CatchUpDemand = 0.0;
            forecast.SteadyDemand = 0.0;
            for (const auto& parentStreamId : *parents) {
                const auto* lineage = lineageRatios->FindPtr(parentStreamId);
                const auto parent = getTarget(*producer, parentStreamId);
                auto convert = [&] (const std::optional<double>& demand) {
                    std::optional<double> result;
                    const auto ratio = lineage ? lineage->Count : std::nullopt;
                    if (ratio && ratio->Weight > 0 &&
                        FilterInvalidValue(ratio->Ratio) && demand)
                    {
                        result = FilterInvalidValue(*demand * ratio->Ratio);
                    }
                    return result;
                };
                add(&forecast.CatchUpDemand, convert(parent.CatchUpDemand));
                add(&forecast.SteadyDemand, convert(parent.SteadyDemand));
            }
        }
        activeForecasts.erase(outputStreamId);
        forecasts[outputStreamId] = forecast;
        return forecast;
    };

    for (const auto& [computationId, registered] : Computations_) {
        THashSet<TStreamId> workStreams = registered.Spec->InputStreamIds;
        for (const auto& streamId : GetKeys(registered.Spec->SourceStreams)) {
            workStreams.insert(streamId);
        }
        for (const auto& streamId : GetKeys(registered.Spec->TimerStreams)) {
            workStreams.insert(streamId);
        }
        for (const auto& streamId : GetKeys(registered.Spec->KeyVisitorStreams)) {
            workStreams.insert(streamId);
        }
        auto& computation = GetOrCrash(Prepared_, computationId);
        for (const auto& streamId : workStreams) {
            const auto demand = getTarget(computationId, streamId);
            add(&computation.Demand.CatchUpDemand, demand.CatchUpDemand);
            add(&computation.Demand.SteadyDemand, demand.SteadyDemand);
            add(&computation.Demand.NormalBacklog, demand.NormalBacklog);
            add(&computation.Demand.ExcessBacklog, demand.ExcessBacklog);
        }
        for (const auto& outputStreamId : registered.Spec->OutputStreamIds) {
            getForecast(outputStreamId);
        }
    }
}

void TThroughputAutoscaler::Evaluate(TThroughputPipelineObservation sample)
{
    Prepared_.clear();
    Decisions_.clear();
    if (!HasScalableComputation()) {
        return;
    }
    if (sample.Now < State_.CurrentTime) {
        for (auto& [_, runtime] : State_.Computations) {
            runtime.Warmup.Reset(sample.Now);
            runtime.ShortageSince.reset();
            runtime.ScaleDownRecommendation.reset();
        }
    }
    State_.CurrentTime = sample.Now;
    if (!State_.LastScaleDownBatchTime || sample.Now < *State_.LastScaleDownBatchTime) {
        State_.LastScaleDownBatchTime = sample.Now;
    }
    const bool allowOrdinaryScaleDown = sample.Now - *State_.LastScaleDownBatchTime >= sample.Config->ScaleDownInterval;
    for (const auto& [computationId, _] : Computations_) {
        Prepared_[computationId].Observation = std::move(GetOrCrash(sample.Computations, computationId));
    }
    PropagateDemand(sample.Config);

    for (const auto& [computationId, _] : Prepared_) {
        EvaluateComputation(computationId, sample.Config, sample.Now, allowOrdinaryScaleDown);
    }
    for (const auto& [computationId, decision] : Decisions_) {
        YT_TLOG_DEBUG("Throughput partition count selected")
            .With("ComputationId", computationId)
            .With("CurrentPartitionCount", GetOrCrash(Prepared_, computationId).Observation.PartitionCount)
            .With("TargetPartitionCount", decision.TargetPartitionCount);
    }
}

void TThroughputAutoscaler::EvaluateComputation(
    const TComputationId& computationId,
    const TThroughputSpecPtr& config,
    TInstant now,
    bool allowOrdinaryScaleDown)
{
    auto& prepared = GetOrCrash(Prepared_, computationId);
    const auto& registered = GetOrCrash(Computations_, computationId);
    if (!IsScalable(registered)) {
        return;
    }
    auto& runtime = State_.Computations[computationId];
    auto& warmup = runtime.Warmup;
    auto& feedback = GetOrCrash(State_.Policies, computationId);
    const auto& current = prepared.Observation;
    auto& demandHistory = GetOrCrash(DemandHistories_, computationId);
    const auto measurementPeriod = feedback.GetMeasurementPeriod();
    if (current.AllJobsHealthy && !current.Repartitioning && current.Measurement && prepared.Demand.SteadyDemand &&
        current.Measurement->End <= now && current.Measurement->End >= current.Measurement->Begin &&
        current.Measurement->End - current.Measurement->Begin >= measurementPeriod &&
        current.Measurement->End >= current.ReadyAt && current.Measurement->End - current.ReadyAt >= measurementPeriod)
    {
        demandHistory.Observe(current.Measurement->End, *prepared.Demand.SteadyDemand, measurementPeriod);
    }
    prepared.PeakSteadyDemand = demandHistory.GetPeak(now, config->PeakDemandWindow);
    auto invalidate = [&] {
        runtime.ShortageSince.reset();
        runtime.ScaleDownRecommendation.reset();
    };
    if (feedback.ApplyResetGeneration() ||
        runtime.ObservedPartitionCount != current.PartitionCount || runtime.ObservedReadyAt != current.ReadyAt ||
        now < current.ReadyAt || (runtime.ShortageSince && now < *runtime.ShortageSince))
    {
        warmup.Reset(now);
        invalidate();
    }
    runtime.ObservedPartitionCount = current.PartitionCount;
    runtime.ObservedReadyAt = current.ReadyAt;
    if (current.PartitionCount <= 0 || current.Repartitioning || !current.AllJobsHealthy) {
        warmup.Reset(now);
        invalidate();
        return;
    }
    // Layout acknowledgement must not depend on another sustained-shortage interval.
    feedback.AcknowledgeAppliedPartitionCount(current.PartitionCount);
    if (!current.Measurement || !current.Measurement->Capacity) {
        runtime.ScaleDownRecommendation.reset();
        return;
    }
    const auto* measurement = &*current.Measurement;
    if (!prepared.Demand.SteadyDemand || !prepared.Demand.CatchUpDemand || !prepared.Demand.NormalBacklog || !prepared.Demand.ExcessBacklog ||
        !measurement->Processed || *measurement->Capacity <= 0 || measurement->End < measurement->Begin ||
        measurement->OutputWaitingShare >= config->MaximumOutputWaitingShare)
    {
        invalidate();
        return;
    }
    const auto [minPartitionCount, maxPartitionCount] = registered.PartitioningSpec->GetPartitionCountBounds();
    auto makeObservation = [&] (const TThroughputProcessingMeasurement& measured) {
        return TThroughputPartitioningObservation{
            .PartitionCount = current.PartitionCount,
            .MinPartitionCount = minPartitionCount,
            .MaxPartitionCount = maxPartitionCount,
            .Capacity = *measured.Capacity,
            .Processed = *measured.Processed,
            .ProcessedCredit = measured.ProcessedCredit,
            .BusyPartitionShare = measured.BusyPartitionShare,
            .SteadyDemand = *prepared.Demand.SteadyDemand,
            .PeakSteadyDemand = prepared.PeakSteadyDemand,
            .Demand = *prepared.Demand.CatchUpDemand,
            .NormalBacklog = *prepared.Demand.NormalBacklog,
            .ExcessBacklog = *prepared.Demand.ExcessBacklog,
        };
    };
    auto observation = makeObservation(*measurement);
    if (observation.HasShortage()) {
        runtime.ScaleDownRecommendation.reset();
        if (!runtime.ShortageSince) {
            runtime.ShortageSince = now;
            warmup.Reset(now);
        }
    } else {
        runtime.ShortageSince.reset();
    }
    auto period = feedback.GetWarmupPeriod();
    if (current.RecoveryMeasurement) {
        const auto& recovery = *current.RecoveryMeasurement;
        if (recovery.Capacity && *recovery.Capacity > 0 && recovery.Processed &&
            recovery.End >= recovery.Begin && recovery.OutputWaitingShare < config->MaximumOutputWaitingShare)
        {
            auto candidate = makeObservation(recovery);
            candidate.AllowScaleUp = runtime.ShortageSince &&
                recovery.End >= *runtime.ShortageSince + feedback.GetMeasurementPeriod();
            auto recoveryWarmup = warmup;
            if (feedback.CanRecoverWithoutBackoff(candidate) &&
                recoveryWarmup.Observe({
                    .Now = now,
                    .CohortReadyAt = current.ReadyAt,
                    .MeasurementBegin = recovery.Begin,
                    .MeasurementEnd = recovery.End,
                    .Period = feedback.GetMeasurementPeriod(),
                }))
            {
                measurement = &recovery;
                observation = candidate;
                observation.ResumeAfterBackoff = true;
                period = feedback.GetMeasurementPeriod();
                prepared.UseRecoveryMeasurement = true;
            }
        }
    }
    observation.AllowScaleUp = runtime.ShortageSince && measurement->End >= *runtime.ShortageSince + period;
    if (runtime.ScaleDownRecommendation &&
        std::abs(observation.Capacity - runtime.ScaleDownRecommendation->Capacity) >
            config->CapacityChangeTolerance * runtime.ScaleDownRecommendation->Capacity)
    {
        runtime.ScaleDownRecommendation.reset();
        warmup.Reset(now);
    }
    prepared.WarmupReady = warmup.Observe({
        .Now = now,
        .CohortReadyAt = current.ReadyAt,
        .MeasurementBegin = measurement->Begin,
        .MeasurementEnd = measurement->End,
        .Period = period,
    });
    if (!prepared.WarmupReady && !runtime.ScaleDownRecommendation) {
        return;
    }
    if (runtime.ScaleDownRecommendation &&
        measurement->End <= runtime.ScaleDownRecommendation->MeasuredThrough)
    {
        return;
    }
    const auto logger = Logger.WithTag("ComputationId", computationId)
        .WithTag("MeasurementBegin", measurement->Begin)
        .WithTag("MeasurementEnd", measurement->End)
        .WithTag("ProcessedCreditBegin", measurement->ProcessedCreditBegin)
        .WithTag("ProcessedCreditEnd", measurement->ProcessedCreditEnd);
    YT_TLOG_EVENT(logger, NLogging::ELogLevel::Debug, "Evaluating throughput feedback");
    const auto proposal = prepared.WarmupReady
        ? std::optional(feedback.SuggestPartitionCount(observation))
        : feedback.RevalidateScaleDown(observation);
    if (!proposal) {
        runtime.ScaleDownRecommendation.reset();
        return;
    }
    auto decision = *proposal;
    if (decision.Action == EThroughputPartitioningAction::Regular &&
        decision.TargetPartitionCount < current.PartitionCount)
    {
        if (!runtime.ScaleDownRecommendation) {
            runtime.ScaleDownRecommendation = TScaleDownRecommendation{
                .PartitionCount = decision.TargetPartitionCount,
                .Capacity = observation.Capacity,
                .MeasuredThrough = measurement->End,
            };
        }
        runtime.ScaleDownRecommendation->MeasuredThrough = measurement->End;
        auto& target = runtime.ScaleDownRecommendation->PartitionCount;
        target = std::clamp(
            std::max(target, decision.TargetPartitionCount),
            observation.MinPartitionCount,
            observation.MaxPartitionCount);
        if (!allowOrdinaryScaleDown) {
            return;
        }
        decision.TargetPartitionCount = target;
    } else {
        runtime.ScaleDownRecommendation.reset();
        if (!prepared.WarmupReady) {
            return;
        }
    }
    if (decision.TargetPartitionCount != current.PartitionCount) {
        Decisions_[computationId] = decision;
        warmup.Reset(now);
    }
}

std::optional<TThroughputAutoscalerDecision> TThroughputAutoscaler::FindDecision(
    const TComputationId& computationId) const
{
    if (const auto* decision = Decisions_.FindPtr(computationId)) {
        return *decision;
    }
    return std::nullopt;
}

void TThroughputAutoscaler::AcceptPartitionCount(const TComputationId& computationId, i64 currentCount, i64 selectedCount)
{
    const auto decision = FindDecision(computationId);
    if (decision && decision->Action == EThroughputPartitioningAction::Regular &&
        decision->TargetPartitionCount < currentCount && selectedCount < currentCount)
    {
        State_.LastScaleDownBatchTime = State_.CurrentTime;
        for (auto& [_, runtime] : State_.Computations) {
            runtime.ScaleDownRecommendation.reset();
        }
    }
    GetOrCrash(State_.Policies, computationId).AcceptPartitionCount(currentCount, selectedCount, decision);
}

std::optional<TThroughputAutoscalerTelemetry> TThroughputAutoscaler::FindTelemetry(
    const TComputationId& computationId) const
{
    const auto* prepared = Prepared_.FindPtr(computationId);
    if (!prepared) {
        return std::nullopt;
    }

    TThroughputAutoscalerTelemetry result;
    result.PartitionCount = prepared->Observation.PartitionCount;
    result.Demand = prepared->Demand.CatchUpDemand;
    result.SteadyDemand = prepared->Demand.SteadyDemand;
    result.PeakSteadyDemand = prepared->PeakSteadyDemand;
    result.Repartitioning = prepared->Observation.Repartitioning;
    result.WarmupReady = prepared->WarmupReady;
    const auto& computation = GetOrCrash(Computations_, computationId);
    if (computation.PartitioningSpec) {
        result.MaxPartitionCount = computation.PartitioningSpec->GetPartitionCountBounds().second;
    }
    const auto& policy = GetOrCrash(State_.Policies, computationId);
    const auto& feedback = policy.GetState();
    result.Anchor = feedback.Anchor;
    result.Exploration = feedback.Exploration;
    if (feedback.Exploration) {
        result.ExplorationPartitionCountLimit = policy.ComputeExplorationPartitionCountLimit();
    }
    result.Phase = feedback.Phase;
    result.RetryWarmupPeriod = feedback.RetryWarmupPeriod;
    result.CapacityDegraded = feedback.CapacityDegraded;
    const auto& measurement = prepared->UseRecoveryMeasurement
        ? prepared->Observation.RecoveryMeasurement
        : prepared->Observation.Measurement;
    if (measurement) {
        result.Processed = measurement->Processed;
        result.Capacity = measurement->Capacity;
    }
    if (const auto* decision = Decisions_.FindPtr(computationId)) {
        result.TargetPartitionCount = decision->TargetPartitionCount;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
