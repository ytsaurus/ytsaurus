#include "feedback_policy.h"

#include <algorithm>
#include <cmath>
#include <limits>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

bool Exceeds(double value, double reference)
{
    return value - reference > 1e-9 * std::max({1.0, value, reference});
}

bool IsReturnPhase(EThroughputSearchPhase phase)
{
    return phase == EThroughputSearchPhase::Plateau || phase == EThroughputSearchPhase::RatePlateau ||
        phase == EThroughputSearchPhase::Recalibrating || phase == EThroughputSearchPhase::Releasing;
}

i64 ComputeRequiredCount(double value, i64 maximum)
{
    return value >= maximum ? maximum : static_cast<i64>(std::ceil(value));
}

i64 ComputeGrowthTarget(
    const TThroughputSpecPtr& config,
    const TThroughputPartitioningObservation& observation)
{
    const double demandFactor = std::max(
        observation.SteadyDemand / observation.Capacity,
        std::min(observation.Demand / observation.Capacity, config->MaximumCatchUpGrowthFactor));
    const double factor = std::clamp(demandFactor, config->MinimumGrowthFactor, config->MaximumSteadyGrowthFactor);
    return ComputeRequiredCount(observation.PartitionCount * factor, observation.MaxPartitionCount);
}

bool CanCompareCapacity(
    const TThroughputSpecPtr& config,
    const TThroughputFeedbackBaseline& anchor,
    const TThroughputPartitioningObservation& observation)
{
    return anchor.BusyPartitionShare >= config->MinimumComparablePartitionShare &&
        observation.BusyPartitionShare >= config->MinimumComparablePartitionShare;
}

bool HasSupportedCapacityGain(
    const TThroughputSpecPtr& config,
    const TThroughputFeedbackBaseline& anchor,
    const TThroughputPartitioningObservation& observation)
{
    const double parallelismGain = static_cast<double>(observation.PartitionCount) / anchor.PartitionCount - 1;
    const double capacityGain = observation.Capacity / anchor.Capacity - 1;
    return Exceeds(capacityGain, config->CapacityChangeTolerance) &&
        !Exceeds(config->MinimumScalingEffectiveness * parallelismGain, capacityGain);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TThroughputFeedbackPolicy::TThroughputFeedbackPolicy(
    TThroughputFeedbackState state,
    TThroughputSpecPtr config,
    TPartitioningSpecPtr partitioningSpec,
    NLogging::TLogger logger)
    : State_(std::move(state))
    , Logger_(std::move(logger))
{
    Reconfigure(std::move(config), std::move(partitioningSpec));
}

void TThroughputFeedbackPolicy::Reconfigure(TThroughputSpecPtr config, TPartitioningSpecPtr partitioningSpec)
{
    static const auto DefaultConfig = New<TThroughputSpec>();
    static const auto DefaultPartitioningSpec = New<TPartitioningSpec>();
    Config_ = config ? std::move(config) : TThroughputSpecPtr(DefaultConfig);
    PartitioningSpec_ = partitioningSpec ? std::move(partitioningSpec) : TPartitioningSpecPtr(DefaultPartitioningSpec);
}

const TThroughputFeedbackState& TThroughputFeedbackPolicy::GetState() const
{
    return State_;
}

i64 TThroughputFeedbackPolicy::ComputeExplorationPartitionCountLimit() const
{
    YT_VERIFY(State_.Exploration);
    const auto& exploration = *State_.Exploration;
    return ComputeRequiredCount(
        PartitioningSpec_->ThroughputMaxExplorationFactor * exploration.OriginPartitionCount *
            std::max(1.0, exploration.BestProcessed / exploration.OriginProcessed),
        std::numeric_limits<i64>::max());
}

bool TThroughputPartitioningObservation::HasShortage() const
{
    return Exceeds(Demand, Capacity);
}

bool TThroughputFeedbackPolicy::ApplyResetGeneration()
{
    const i64 pipelineGeneration = Config_->ResetGeneration;
    if (State_.PipelineResetGeneration == pipelineGeneration) {
        return false;
    }
    State_.PipelineResetGeneration = pipelineGeneration;
    if (State_.ResetRequested || State_.Phase == EThroughputSearchPhase::Recalibrating || State_.Phase == EThroughputSearchPhase::Releasing) {
        return false;
    }
    State_.ResetRequested = true;
    return true;
}

TDuration TThroughputFeedbackPolicy::GetMeasurementPeriod() const
{
    return PartitioningSpec_->ThroughputWarmupPeriod.value_or(Config_->WarmupPeriod);
}

TDuration TThroughputFeedbackPolicy::GetWarmupPeriod() const
{
    return std::max(GetMeasurementPeriod(), State_.RetryWarmupPeriod);
}

bool TThroughputFeedbackPolicy::CanRecoverWithoutBackoff(
    const TThroughputPartitioningObservation& observation) const
{
    return State_.CapacityDegraded && State_.RetryWarmupPeriod > GetMeasurementPeriod() &&
        !State_.ResetRequested && !State_.PendingPartitionCount &&
        State_.ObservedPartitionCount == observation.PartitionCount &&
        (State_.Phase == EThroughputSearchPhase::Recalibrating ||
        State_.Phase == EThroughputSearchPhase::FirstProbe || State_.Phase == EThroughputSearchPhase::SecondProbe) &&
        State_.Anchor && observation.PartitionCount > State_.Anchor->PartitionCount &&
        CanCompareCapacity(Config_, *State_.Anchor, observation) &&
        HasSupportedCapacityGain(Config_, *State_.Anchor, observation) &&
        observation.Processed > Config_->MinimumLoadShare * observation.Capacity &&
        observation.AllowScaleUp && observation.HasShortage();
}

void TThroughputFeedbackPolicy::FinishSearch()
{
    State_.Anchor.reset();
    State_.PendingPartitionCount.reset();
    State_.Phase = EThroughputSearchPhase::Idle;
    State_.RetryWarmupPeriod = TDuration::Zero();
    State_.CapacityDegraded = false;
    State_.ResetRequested = false;
}

std::optional<TThroughputAutoscalerDecision> TThroughputFeedbackPolicy::RevalidateScaleDown(
    const TThroughputPartitioningObservation& observation) const
{
    auto preview = *this;
    const auto decision = preview.SuggestPartitionCount(observation);
    if (decision.Action == EThroughputPartitioningAction::Regular &&
        decision.TargetPartitionCount < observation.PartitionCount)
    {
        return decision;
    }
    return std::nullopt;
}

void TThroughputFeedbackPolicy::AcceptPartitionCount(
    i64 currentCount,
    i64 selectedCount,
    std::optional<TThroughputAutoscalerDecision> proposal)
{
    if (proposal) {
        State_.PendingPartitionCount = proposal->TargetPartitionCount;
    }
    if (!State_.PendingPartitionCount) {
        return;
    }
    if (*State_.PendingPartitionCount < currentCount && !IsReturnPhase(State_.Phase) &&
        selectedCount >= currentCount)
    {
        State_.PendingPartitionCount.reset();
        return;
    }
    if (selectedCount == currentCount) {
        if (State_.Phase == EThroughputSearchPhase::Releasing) {
            // Demand is covered; a legacy floor or count deadband makes this the working point.
            FinishSearch();
            return;
        }
        if (*State_.PendingPartitionCount > currentCount) {
            State_.Phase = State_.Phase == EThroughputSearchPhase::SecondProbe
                ? EThroughputSearchPhase::FirstProbe
                : EThroughputSearchPhase::Idle;
            State_.PendingPartitionCount.reset();
        }
        return;
    }
    State_.PendingPartitionCount = selectedCount;
}

void TThroughputFeedbackPolicy::AcknowledgeAppliedPartitionCount(i64 count)
{
    if (!State_.PendingPartitionCount || count != *State_.PendingPartitionCount) {
        return;
    }
    const bool finished = (count < State_.ObservedPartitionCount && !IsReturnPhase(State_.Phase)) ||
        ((State_.Phase == EThroughputSearchPhase::Plateau || State_.Phase == EThroughputSearchPhase::RatePlateau) &&
            count != State_.ObservedPartitionCount) ||
        State_.Phase == EThroughputSearchPhase::Releasing;
    State_.PendingPartitionCount.reset();
    State_.ObservedPartitionCount = count;
    if (finished) {
        FinishSearch();
    }
}

TThroughputAutoscalerDecision TThroughputFeedbackPolicy::ProposePartitionCount(
    const TThroughputPartitioningObservation& observation,
    const NLogging::TLogger& logger,
    i64 target,
    EThroughputPartitioningAction action)
{
    const i64 count = observation.PartitionCount;
    const auto& Logger = logger;
    if (target > count && target > observation.MinPartitionCount && !State_.Exploration) {
        YT_VERIFY(observation.Processed > 0);
        State_.Exploration.emplace();
        State_.Exploration->OriginPartitionCount = count;
        State_.Exploration->OriginProcessed = observation.Processed;
        State_.Exploration->BestProcessed = observation.Processed;
    }
    target = std::clamp(target, observation.MinPartitionCount, observation.MaxPartitionCount);
    if (target > count && State_.Exploration) {
        target = std::max({count, observation.MinPartitionCount, std::min(target, ComputeExplorationPartitionCountLimit())});
    }
    if (target == count && State_.PendingPartitionCount && *State_.PendingPartitionCount > count) {
        State_.PendingPartitionCount.reset();
    }
    if (target != count) {
        if (action == EThroughputPartitioningAction::Regular && target < count) {
            State_.PendingPartitionCount.reset();
        } else {
            State_.PendingPartitionCount = target;
        }
        YT_TLOG_DEBUG("Throughput partition change proposed")
            .With("Phase", State_.Phase)
            .With("TargetPartitionCount", target)
            .With("Action", action)
            .With("WarmupPeriod", GetWarmupPeriod());
    }
    return TThroughputAutoscalerDecision{target, action};
}

void TThroughputFeedbackPolicy::RecordAnchor(const TThroughputPartitioningObservation& observation)
{
    State_.Anchor.emplace();
    State_.Anchor->PartitionCount = observation.PartitionCount;
    State_.Anchor->Capacity = observation.Capacity;
    State_.Anchor->BusyPartitionShare = observation.BusyPartitionShare;
    State_.Phase = EThroughputSearchPhase::Idle;
}

TThroughputAutoscalerDecision TThroughputFeedbackPolicy::ReturnToAnchor(
    const TThroughputPartitioningObservation& observation,
    const NLogging::TLogger& logger)
{
    State_.Phase = EThroughputSearchPhase::Recalibrating;
    return ProposePartitionCount(
        observation,
        logger,
        std::min(observation.PartitionCount, State_.Anchor->PartitionCount),
        EThroughputPartitioningAction::ReturnToWorkingPoint);
}

std::optional<TThroughputAutoscalerDecision> TThroughputFeedbackPolicy::ReconcilePartitionCount(
    const TThroughputPartitioningObservation& observation,
    const NLogging::TLogger& logger)
{
    const i64 count = observation.PartitionCount;
    const double capacity = observation.Capacity;
    const auto& Logger = logger;
    const auto hold = TThroughputAutoscalerDecision{count};
    const bool shortage = observation.HasShortage();
    const bool normalized = !shortage && observation.ExcessBacklog <= observation.NormalBacklog;
    const i64 demandCount = ComputeRequiredCount(
        count * (State_.CapacityDegraded ? observation.Demand : std::max(observation.Demand, Config_->ScaleDownCapacityReserve * observation.PeakSteadyDemand)) / capacity,
        count);
    AcknowledgeAppliedPartitionCount(count);
    const bool hadPendingChange = State_.PendingPartitionCount.has_value();
    if (State_.PendingPartitionCount && *State_.PendingPartitionCount < count && !IsReturnPhase(State_.Phase)) {
        State_.PendingPartitionCount.reset();
    }
    if (normalized && State_.Phase == EThroughputSearchPhase::Recalibrating) {
        State_.Phase = EThroughputSearchPhase::Releasing;
    }
    if (State_.PendingPartitionCount && *State_.PendingPartitionCount > count && (!shortage || State_.ResetRequested)) {
        State_.PendingPartitionCount.reset();
        if (!State_.ResetRequested && count == State_.Anchor->PartitionCount) {
            FinishSearch();
        } else if (!State_.ResetRequested && State_.Phase == EThroughputSearchPhase::SecondProbe) {
            State_.Phase = EThroughputSearchPhase::FirstProbe;
        }
    }
    if (State_.PendingPartitionCount && *State_.PendingPartitionCount < count &&
        (State_.Phase == EThroughputSearchPhase::Plateau || State_.Phase == EThroughputSearchPhase::RatePlateau ||
            State_.Phase == EThroughputSearchPhase::Releasing || State_.Phase == EThroughputSearchPhase::Idle))
    {
        if (normalized) {
            if (demandCount == count && State_.Phase == EThroughputSearchPhase::Releasing) {
                FinishSearch();
                return hold;
            }
            *State_.PendingPartitionCount = std::max(*State_.PendingPartitionCount, demandCount);
        } else {
            State_.PendingPartitionCount.reset();
            if (State_.Phase == EThroughputSearchPhase::Releasing) {
                return ReturnToAnchor(observation, Logger);
            }
        }
    }
    if (State_.PendingPartitionCount) {
        if (count != *State_.PendingPartitionCount) {
            if (*State_.PendingPartitionCount > count &&
                (!observation.AllowScaleUp || observation.Processed <= Config_->MinimumLoadShare * capacity))
            {
                return hold;
            }
            return ProposePartitionCount(
                observation,
                Logger,
                *State_.PendingPartitionCount,
                IsReturnPhase(State_.Phase) ? EThroughputPartitioningAction::ReturnToWorkingPoint : EThroughputPartitioningAction::Regular);
        }
        AcknowledgeAppliedPartitionCount(count);
    } else if (!hadPendingChange && State_.ObservedPartitionCount && count != State_.ObservedPartitionCount) {
        YT_TLOG_DEBUG("Throughput observation changed outside the pending probe")
            .With("PreviousPartitionCount", State_.ObservedPartitionCount);
        if (State_.CapacityDegraded && State_.Anchor) {
            State_.ObservedPartitionCount = count;
            return ReturnToAnchor(observation, Logger);
        }
        State_.Anchor.reset();
        State_.Phase = EThroughputSearchPhase::Idle;
    }
    State_.ObservedPartitionCount = count;

    if (State_.Phase == EThroughputSearchPhase::Releasing) {
        FinishSearch();
    }
    return std::nullopt;
}

std::optional<TThroughputAutoscalerDecision> TThroughputFeedbackPolicy::ObserveCapacity(
    const TThroughputPartitioningObservation& observation,
    const NLogging::TLogger& logger)
{
    const i64 count = observation.PartitionCount;
    const double capacity = observation.Capacity;
    const auto& Logger = logger;
    const auto hold = TThroughputAutoscalerDecision{count};
    const bool normalized = !observation.HasShortage() && observation.ExcessBacklog <= observation.NormalBacklog;
    const i64 demandCount = ComputeRequiredCount(count * observation.Demand / capacity, count);
    YT_TLOG_DEBUG("Throughput processing observation")
        .With("Phase", State_.Phase)
        .With("AnchorPartitionCount", State_.Anchor ? State_.Anchor->PartitionCount : 0)
        .With("AnchorCapacity", State_.Anchor ? State_.Anchor->Capacity : 0)
        .With("AnchorBusyPartitionShare", State_.Anchor ? State_.Anchor->BusyPartitionShare : 0)
        .With("Comparable", State_.Anchor && CanCompareCapacity(Config_, *State_.Anchor, observation));
    const bool capacityFell = State_.Anchor && CanCompareCapacity(Config_, *State_.Anchor, observation) && Exceeds((1 - Config_->CapacityChangeTolerance) * State_.Anchor->Capacity, capacity);
    if (capacityFell && count > State_.Anchor->PartitionCount && !State_.ResetRequested &&
        observation.Processed > Config_->MinimumLoadShare * capacity &&
        (State_.Phase == EThroughputSearchPhase::FirstProbe || State_.Phase == EThroughputSearchPhase::SecondProbe ||
            State_.Phase == EThroughputSearchPhase::Plateau || State_.Phase == EThroughputSearchPhase::RatePlateau ||
            State_.Phase == EThroughputSearchPhase::ExplorationLimited))
    {
        State_.CapacityDegraded = true;
        const auto cap = std::max(GetMeasurementPeriod(), Config_->MaxRetryWarmupPeriod);
        const auto period = GetWarmupPeriod();
        State_.RetryWarmupPeriod = period >= cap / Config_->RetryWarmupFactor ? cap : period * Config_->RetryWarmupFactor;
        YT_TLOG_WARNING("Throughput capacity fell at increased parallelism; returning for recalibration")
            .With("AnchorPartitionCount", State_.Anchor->PartitionCount)
            .With("AnchorCapacity", State_.Anchor->Capacity)
            .With("WarmupPeriod", GetWarmupPeriod());
        if (normalized) {
            const i64 target = std::min(count, std::max(State_.Anchor->PartitionCount, demandCount));
            if (target == count) {
                FinishSearch();
                return hold;
            }
            State_.Phase = EThroughputSearchPhase::Releasing;
            return ProposePartitionCount(observation, Logger, target, EThroughputPartitioningAction::ReturnToWorkingPoint);
        }
        return ReturnToAnchor(observation, Logger);
    }
    if (State_.Anchor && CanCompareCapacity(Config_, *State_.Anchor, observation) && count > State_.Anchor->PartitionCount && !State_.ResetRequested &&
        observation.Processed > Config_->MinimumLoadShare * capacity &&
        (State_.Phase == EThroughputSearchPhase::FirstProbe || State_.Phase == EThroughputSearchPhase::SecondProbe))
    {
        if (HasSupportedCapacityGain(Config_, *State_.Anchor, observation)) {
            RecordAnchor(observation);
            State_.CapacityDegraded = false;
            State_.RetryWarmupPeriod = TDuration::Zero();
        } else if (State_.Phase == EThroughputSearchPhase::SecondProbe &&
            !Exceeds((1 - Config_->CapacityChangeTolerance) * State_.Anchor->Capacity, capacity))
        {
            State_.Phase = EThroughputSearchPhase::Plateau;
            State_.CapacityDegraded = false;
            State_.RetryWarmupPeriod = TDuration::Zero();
            YT_TLOG_DEBUG("Throughput plateau retained")
                .With("AnchorPartitionCount", State_.Anchor->PartitionCount)
                .With("AnchorCapacity", State_.Anchor->Capacity);
        }
    }
    return std::nullopt;
}

TThroughputAutoscalerDecision TThroughputFeedbackPolicy::SuggestPartitionCount(
    const TThroughputPartitioningObservation& observation)
{
    const i64 count = observation.PartitionCount;
    const double capacity = observation.Capacity;
    YT_VERIFY(count > 0 && capacity > 0);
    const auto Logger = Logger_.WithTag("PartitionCount", count)
        .WithTag("Capacity", capacity)
        .WithTag("Processed", observation.Processed)
        .WithTag("ProcessedCredit", observation.ProcessedCredit)
        .WithTag("BusyPartitionShare", observation.BusyPartitionShare)
        .WithTag("SteadyDemand", observation.SteadyDemand)
        .WithTag("PeakSteadyDemand", observation.PeakSteadyDemand)
        .WithTag("Demand", observation.Demand);
    if (State_.Exploration && observation.ProcessedCredit &&
        observation.Processed >= Config_->MinimumExplorationLoadShare * capacity &&
        Exceeds(*observation.ProcessedCredit, (1 + Config_->CapacityChangeTolerance) * State_.Exploration->BestProcessed))
    {
        State_.Exploration->BestProcessed = *observation.ProcessedCredit;
    }
    const auto hold = TThroughputAutoscalerDecision{count};

    if (observation.ResumeAfterBackoff) {
        YT_VERIFY(CanRecoverWithoutBackoff(observation));
        YT_TLOG_DEBUG("Throughput recovery accepted before retry backoff")
            .With("AnchorPartitionCount", State_.Anchor->PartitionCount)
            .With("AnchorCapacity", State_.Anchor->Capacity)
            .With("RetryWarmupPeriod", State_.RetryWarmupPeriod);
        RecordAnchor(observation);
        State_.CapacityDegraded = false;
        State_.RetryWarmupPeriod = TDuration::Zero();
    }

    const bool shortage = observation.HasShortage();
    const bool normalized = !shortage && observation.ExcessBacklog <= observation.NormalBacklog;
    const i64 demandCount = ComputeRequiredCount(
        count * std::max(observation.Demand, Config_->ScaleDownCapacityReserve * observation.PeakSteadyDemand) / capacity,
        count);
    if (auto decision = ReconcilePartitionCount(observation, Logger)) {
        return *decision;
    }
    if (auto decision = ObserveCapacity(observation, Logger)) {
        return *decision;
    }
    if (normalized) {
        if (State_.Anchor && (State_.Phase == EThroughputSearchPhase::Plateau || State_.Phase == EThroughputSearchPhase::RatePlateau)) {
            const i64 target = std::min(count, std::max(State_.Anchor->PartitionCount, demandCount));
            if (target < count) {
                return ProposePartitionCount(observation, Logger, target, EThroughputPartitioningAction::ReturnToWorkingPoint);
            }
            return hold;
        }
        const i64 reserveCount = ComputeRequiredCount(
            count * (std::max(observation.Demand, Config_->ScaleDownCapacityReserve * std::max(observation.Processed, observation.PeakSteadyDemand)) / capacity),
            count);
        if ((1 + Config_->ScaleDownTolerance) * reserveCount < count) {
            const i64 minimumCount = count / Config_->MaximumScaleDownFactor + (count % Config_->MaximumScaleDownFactor != 0);
            return ProposePartitionCount(observation, Logger, std::max(minimumCount, reserveCount));
        }
        return hold;
    }

    if (shortage && State_.Exploration && State_.Phase != EThroughputSearchPhase::Plateau &&
        State_.Phase != EThroughputSearchPhase::Recalibrating && !State_.ResetRequested)
    {
        const i64 limit = ComputeExplorationPartitionCountLimit();
        if (count >= limit) {
            const auto phase = observation.Processed >= Config_->MinimumExplorationLoadShare * capacity &&
                    !Exceeds((1 - Config_->CapacityChangeTolerance) * State_.Exploration->BestProcessed, observation.Processed)
                ? EThroughputSearchPhase::RatePlateau
                : EThroughputSearchPhase::ExplorationLimited;
            if (phase != State_.Phase) {
                YT_TLOG_DEBUG("Throughput exploration stopped")
                    .With("Phase", phase)
                    .With("PartitionCountLimit", limit)
                    .With("OriginPartitionCount", State_.Exploration->OriginPartitionCount)
                    .With("OriginProcessed", State_.Exploration->OriginProcessed)
                    .With("BestProcessed", State_.Exploration->BestProcessed);
            }
            State_.Phase = phase;
            return hold;
        }
        if (State_.Phase == EThroughputSearchPhase::RatePlateau || State_.Phase == EThroughputSearchPhase::ExplorationLimited) {
            State_.Phase = EThroughputSearchPhase::FirstProbe;
        }
    }
    if (observation.Processed <= Config_->MinimumLoadShare * capacity) {
        return hold;
    }
    if (State_.ResetRequested) {
        State_.ResetRequested = false;
        if (State_.Anchor && count > State_.Anchor->PartitionCount) {
            return ReturnToAnchor(observation, Logger);
        }
        RecordAnchor(observation);
    }
    if (State_.Phase == EThroughputSearchPhase::Recalibrating) {
        // The arbiter may keep us above the requested anchor because of a legacy floor.
        RecordAnchor(observation);
    }
    if (!shortage) {
        return hold;
    }
    if (State_.Anchor && State_.Anchor->BusyPartitionShare < Config_->MinimumComparablePartitionShare &&
        observation.BusyPartitionShare >= Config_->MinimumComparablePartitionShare &&
        (State_.Phase == EThroughputSearchPhase::FirstProbe || State_.Phase == EThroughputSearchPhase::SecondProbe))
    {
        YT_TLOG_DEBUG("Throughput capacity reference initialized after broad processing activity")
            .With("PreviousAnchorPartitionCount", State_.Anchor->PartitionCount)
            .With("PreviousAnchorCapacity", State_.Anchor->Capacity);
        RecordAnchor(observation);
    }
    if (State_.Phase == EThroughputSearchPhase::Plateau) {
        if (CanCompareCapacity(Config_, *State_.Anchor, observation) &&
            HasSupportedCapacityGain(Config_, *State_.Anchor, observation))
        {
            return ReturnToAnchor(observation, Logger);
        }
        return hold;
    }
    if (!observation.AllowScaleUp) {
        return hold;
    }
    const bool comparable = !State_.Anchor || CanCompareCapacity(Config_, *State_.Anchor, observation);
    if (comparable && (!State_.Anchor || count < State_.Anchor->PartitionCount || State_.Phase == EThroughputSearchPhase::Idle)) {
        RecordAnchor(observation);
    }
    const i64 target = ComputeGrowthTarget(Config_, observation);
    if (target <= count) {
        return hold;
    }
    // Sparse work cannot distinguish a shared ceiling from underfed partitions.
    State_.Phase = comparable && count > State_.Anchor->PartitionCount && State_.Phase == EThroughputSearchPhase::FirstProbe
        ? EThroughputSearchPhase::SecondProbe
        : EThroughputSearchPhase::FirstProbe;
    return ProposePartitionCount(observation, Logger, target);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
