#include "fair_share_strategy_operation_controller.h"

#include "operation_controller.h"

#include <yt/yt/server/lib/scheduler/config.h>

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyOperationController::TFairShareStrategyOperationController(
    IOperationStrategyHost* operation,
    const TFairShareStrategyOperationControllerConfigPtr& config,
    const std::vector<IInvokerPtr>& nodeShardInvokers)
    : Controller_(operation->GetControllerStrategyHost())
    , OperationId_(operation->GetId())
    , Logger(StrategyLogger().WithTag("OperationId: %v", OperationId_))
    , Config_(config)
    , NodeShardInvokers_(nodeShardInvokers)
    , ScheduleAllocationControllerThrottlingBackoff_(
        DurationToCpuDuration(config->ControllerThrottling->ScheduleAllocationStartBackoffTime))
{
    YT_VERIFY(Controller_);

    // NB(eshcherbin): To initialize throttling options.
    UpdateConfig(config);
}

TControllerEpoch TFairShareStrategyOperationController::GetEpoch() const
{
    return Controller_->GetEpoch();
}

void TFairShareStrategyOperationController::OnScheduleAllocationStarted(const ISchedulingContextPtr& schedulingContext)
{
    auto nodeShardId = schedulingContext->GetNodeShardId();
    auto& shard = StateShards_[nodeShardId];
    ++shard.ConcurrentScheduleAllocationCalls;
    shard.ScheduleAllocationCallsSinceLastUpdate.fetch_add(1, std::memory_order::relaxed);
    shard.ConcurrentScheduleAllocationExecDuration += shard.ScheduleAllocationExecDurationEstimate;

    schedulingContext->StoreScheduleAllocationExecDurationEstimate(shard.ScheduleAllocationExecDurationEstimate);

    YT_LOG_DEBUG_IF(
        DetailedLogsEnabled_,
        "Controller schedule allocation started "
        "(ConcurrentScheduleAllocationCalls: %v, ConcurrentScheduleAllocationExecDuration: %v, "
        "ScheduleAllocationExecDurationEstimate: %v, NodeShardId: %v)",
        shard.ConcurrentScheduleAllocationCalls,
        shard.ConcurrentScheduleAllocationExecDuration,
        shard.ScheduleAllocationExecDurationEstimate,
        nodeShardId);
}

void TFairShareStrategyOperationController::OnScheduleAllocationFinished(const ISchedulingContextPtr& schedulingContext)
{
    auto nodeShardId = schedulingContext->GetNodeShardId();
    auto& shard = StateShards_[nodeShardId];
    --shard.ConcurrentScheduleAllocationCalls;
    shard.ConcurrentScheduleAllocationExecDuration -= schedulingContext->ExtractScheduleAllocationExecDurationEstimate();

    YT_LOG_DEBUG_IF(
        DetailedLogsEnabled_,
        "Controller schedule allocation finished "
        "(ConcurrentScheduleAllocationCalls: %v, ConcurrentScheduleAllocationExecDuration: %v, "
        "ScheduleAllocationExecDurationEstimate: %v, NodeShardId: %v)",
        shard.ConcurrentScheduleAllocationCalls,
        shard.ConcurrentScheduleAllocationExecDuration,
        shard.ScheduleAllocationExecDurationEstimate,
        nodeShardId);
}

TCompositeNeededResources TFairShareStrategyOperationController::GetNeededResources() const
{
    return Controller_->GetNeededResources();
}

TAllocationGroupResourcesMap TFairShareStrategyOperationController::GetGroupedNeededResources() const
{
    return Controller_->GetGroupedNeededResources();
}

TAllocationGroupResourcesMap TFairShareStrategyOperationController::GetInitialGroupedNeededResources() const
{
    return Controller_->GetInitialGroupedNeededResources();
}

TJobResources TFairShareStrategyOperationController::GetAggregatedMinNeededAllocationResources() const
{
    // Min needed resources must be less than total needed resources of operation. See YT-9363.
    auto result = GetNeededResources().DefaultResources;
    for (const auto& [_, allocationGroupResources] : GetGroupedNeededResources()) {
        result = Min(result, allocationGroupResources.MinNeededResources.ToJobResources());
    }

    return result;
}

TJobResources TFairShareStrategyOperationController::GetAggregatedInitialMinNeededAllocationResources() const
{
    auto initialGroupedNeededResources = GetInitialGroupedNeededResources();
    if (initialGroupedNeededResources.empty()) {
        // A reasonable fallback, but this should really never happen.
        return TJobResources();
    }

    auto result = initialGroupedNeededResources.begin()->second.MinNeededResources.ToJobResources();
    for (const auto& [_, allocationGroupResources] : initialGroupedNeededResources) {
        result = Min(result, allocationGroupResources.MinNeededResources.ToJobResources());
    }

    return result;
}

void TFairShareStrategyOperationController::UpdateGroupedNeededResources()
{
    Controller_->UpdateGroupedNeededResources();
}

void TFairShareStrategyOperationController::UpdateConcurrentScheduleAllocationThrottlingLimits(
    const TFairShareStrategyOperationControllerConfigPtr& config)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    int nodeShardCount = std::ssize(NodeShardInvokers_);

    auto scheduleAllocationCallsRegularizedLimit =
        config->MaxConcurrentControllerScheduleAllocationCalls *
        config->ConcurrentControllerScheduleAllocationCallsRegularization;
    auto scheduleAllocationCallsNodeShardValue = scheduleAllocationCallsRegularizedLimit / nodeShardCount;
    scheduleAllocationCallsNodeShardValue = std::max(static_cast<int>(scheduleAllocationCallsNodeShardValue), 1);

    auto scheduleAllocationExecDurationRegularizedLimit =
        config->MaxConcurrentControllerScheduleAllocationExecDuration *
        config->ConcurrentControllerScheduleAllocationCallsRegularization;
    auto scheduleAllocationExecDurationNodeShardValue =
        scheduleAllocationExecDurationRegularizedLimit /
        nodeShardCount;
    scheduleAllocationExecDurationNodeShardValue = std::max(
        scheduleAllocationExecDurationNodeShardValue,
        TDuration::FromValue(1));

    for (int nodeShardId = 0; nodeShardId < std::ssize(NodeShardInvokers_); ++nodeShardId)
    {
        NodeShardInvokers_[nodeShardId]->Invoke(BIND([
                this_ = MakeStrong(this),
                scheduleAllocationCallsNodeShardValue,
                scheduleAllocationExecDurationNodeShardValue,
                &shard = StateShards_[nodeShardId]]
            {
                shard.MaxConcurrentControllerScheduleAllocationCalls = scheduleAllocationCallsNodeShardValue;
                shard.MaxConcurrentControllerScheduleAllocationExecDuration = scheduleAllocationExecDurationNodeShardValue;
            }));
    }
}

bool TFairShareStrategyOperationController::CheckMaxScheduleAllocationCallsOverdraft(int maxScheduleAllocationCalls) const
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    for (auto& shard : StateShards_) {
        ScheduleAllocationCallsOverdraft_ += shard.ScheduleAllocationCallsSinceLastUpdate.exchange(0);
    }
    ScheduleAllocationCallsOverdraft_ = std::max(0, ScheduleAllocationCallsOverdraft_ - maxScheduleAllocationCalls);

    return ScheduleAllocationCallsOverdraft_ > 0;
}

bool TFairShareStrategyOperationController::IsMaxConcurrentScheduleAllocationCallsPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const
{
    auto nodeShardId = schedulingContext->GetNodeShardId();
    auto& shard = StateShards_[nodeShardId];
    bool limitViolated = shard.ConcurrentScheduleAllocationCalls >= shard.MaxConcurrentControllerScheduleAllocationCalls;

    YT_LOG_DEBUG_IF(
        limitViolated && DetailedLogsEnabled_,
        "Max concurrent schedule allocation calls per node shard violated (ConcurrentScheduleAllocationCalls: %v, Limit: %v, NodeShardId: %v)",
        shard.ConcurrentScheduleAllocationCalls,
        shard.MaxConcurrentControllerScheduleAllocationCalls,
        nodeShardId);

    return limitViolated;
}

bool TFairShareStrategyOperationController::IsMaxConcurrentScheduleAllocationExecDurationPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const
{
    if (!EnableConcurrentScheduleAllocationExecDurationThrottling_.load(std::memory_order_acquire)) {
        return false;
    }

    auto nodeShardId = schedulingContext->GetNodeShardId();
    auto& shard = StateShards_[nodeShardId];
    bool limitViolated = shard.ConcurrentScheduleAllocationExecDuration >= shard.MaxConcurrentControllerScheduleAllocationExecDuration;

    YT_LOG_DEBUG_IF(
        limitViolated && DetailedLogsEnabled_,
        "Max concurrent schedule allocation exec duration per node shard violated "
        "(ConcurrentScheduleAllocationExecDuration: %v, Limit: %v, "
        "ScheduleAllocationExecDurationEstimate: %v, NodeShardId: %v)",
        shard.ConcurrentScheduleAllocationExecDuration,
        shard.MaxConcurrentControllerScheduleAllocationExecDuration,
        shard.ScheduleAllocationExecDurationEstimate,
        nodeShardId);

    return limitViolated;
}

bool TFairShareStrategyOperationController::HasRecentScheduleAllocationFailure(TCpuInstant now) const
{
    return ScheduleAllocationBackoffDeadline_ > now;
}

bool TFairShareStrategyOperationController::ScheduleAllocationBackoffObserved() const
{
    return ScheduleAllocationBackoffObserved_.load();
}

void TFairShareStrategyOperationController::AbortAllocation(TAllocationId allocationId, EAbortReason abortReason, TControllerEpoch allocationEpoch)
{
    Controller_->OnNonscheduledAllocationAborted(allocationId, abortReason, allocationEpoch);
}

TControllerScheduleAllocationResultPtr TFairShareStrategyOperationController::ScheduleAllocation(
    const ISchedulingContextPtr& context,
    const TJobResources& availableResources,
    const TDiskResources& availableDiskResources,
    TDuration timeLimit,
    const TString& treeId,
    const TString& poolPath,
    const TFairShareStrategyTreeConfigPtr& treeConfig)
{
    auto scheduleAllocationResultFuture = Controller_->ScheduleAllocation(
        context,
        availableResources,
        availableDiskResources,
        treeId,
        poolPath,
        treeConfig);

    auto scheduleAllocationResultFutureWithTimeout = scheduleAllocationResultFuture
        .ToUncancelable()
        .WithTimeout(timeLimit);

    auto config = Config_.Acquire();

    auto startTime = TInstant::Now();
    scheduleAllocationResultFuture.Subscribe(
        BIND([
            this,
            this_ = MakeStrong(this),
            startTime,
            longScheduleAllocationThreshold = config->LongScheduleAllocationLoggingThreshold
        ] (const TError& /*error*/) {
            auto now = TInstant::Now();
            if (startTime + longScheduleAllocationThreshold < now) {
                YT_LOG_DEBUG(
                    "Schedule allocation takes too long (Duration: %v ms, LongScheduleAllocationThreshold: %v ms)",
                    (now - startTime).MilliSeconds(),
                    longScheduleAllocationThreshold.MilliSeconds());
            }
        }));

    auto maybeUpdateDurationEstimate = [this, nodeSharId = context->GetNodeShardId()] (const TControllerScheduleAllocationResultPtr& result) {
        if (auto estimate = result->NextDurationEstimate) {
            auto& shard = StateShards_[nodeSharId];
            shard.ScheduleAllocationExecDurationEstimate = *estimate;
        }
    };

    auto scheduleAllocationResultWithTimeoutOrError = WaitFor(scheduleAllocationResultFutureWithTimeout);
    if (!scheduleAllocationResultWithTimeoutOrError.IsOK()) {
        auto scheduleAllocationResult = New<TControllerScheduleAllocationResult>();
        if (scheduleAllocationResultWithTimeoutOrError.GetCode() == NYT::EErrorCode::Timeout) {
            scheduleAllocationResult->RecordFail(EScheduleFailReason::Timeout);
            // If ScheduleAllocation was not canceled we need to abort created allocation.
            scheduleAllocationResultFuture.Subscribe(
                BIND([
                    this,
                    this_ = MakeStrong(this),
                    maybeUpdateDurationEstimate
                ] (const TErrorOr<TControllerScheduleAllocationResultPtr>& scheduleAllocationResultOrError) {
                    if (!scheduleAllocationResultOrError.IsOK()) {
                        return;
                    }

                    const auto& scheduleAllocationResult = scheduleAllocationResultOrError.Value();
                    if (scheduleAllocationResult->StartDescriptor) {
                        auto allocationId = scheduleAllocationResult->StartDescriptor->Id;
                        YT_LOG_WARNING(
                            "Aborting late allocation (AllocationId: %v)",
                            allocationId);
                        AbortAllocation(
                            allocationId,
                            EAbortReason::SchedulingTimeout,
                            scheduleAllocationResult->ControllerEpoch);
                    }

                    maybeUpdateDurationEstimate(scheduleAllocationResult);
            }));
        }
        return scheduleAllocationResult;
    }

    const auto& scheduleAllocationResult = scheduleAllocationResultWithTimeoutOrError.Value();
    maybeUpdateDurationEstimate(scheduleAllocationResult);

    return scheduleAllocationResult;
}

void TFairShareStrategyOperationController::OnScheduleAllocationFailed(
    TCpuInstant now,
    const TString& treeId,
    const TControllerScheduleAllocationResultPtr& scheduleAllocationResult)
{
    auto config = GetConfig();

    TCpuInstant backoffDeadline = 0;
    if (scheduleAllocationResult->Failed[EScheduleFailReason::ControllerThrottling] > 0) {
        auto value = ScheduleAllocationControllerThrottlingBackoff_.load();
        backoffDeadline = now + value;

        {
            auto newValue = std::min(
                DurationToCpuDuration(config->ControllerThrottling->ScheduleAllocationMaxBackoffTime),
                TCpuDuration(value * config->ControllerThrottling->ScheduleAllocationBackoffMultiplier));
            // Nobody cares if some of concurrent updates fail.
            ScheduleAllocationControllerThrottlingBackoff_.compare_exchange_weak(value, newValue);
        }

    } else {
        ScheduleAllocationControllerThrottlingBackoff_.store(
            DurationToCpuDuration(config->ControllerThrottling->ScheduleAllocationStartBackoffTime));

        if (scheduleAllocationResult->IsBackoffNeeded()) {
            backoffDeadline = now + DurationToCpuDuration(config->ScheduleAllocationFailBackoffTime);
        }
    }

    if (backoffDeadline > 0) {
        YT_LOG_DEBUG("Failed to schedule allocation, backing off (Duration: %v, Reasons: %v)",
            backoffDeadline - now,
            scheduleAllocationResult->Failed);
        ScheduleAllocationBackoffDeadline_.store(backoffDeadline);
        ScheduleAllocationBackoffObserved_.store(true);
    }

    if (scheduleAllocationResult->Failed[EScheduleFailReason::TentativeTreeDeclined] > 0) {
        auto guard = WriterGuard(SaturatedTentativeTreesLock_);
        TentativeTreeIdToSaturationTime_[treeId] = now;
    }
}

bool TFairShareStrategyOperationController::IsSaturatedInTentativeTree(TCpuInstant now, const TString& treeId, TDuration saturationDeactivationTimeout) const
{
    auto guard = ReaderGuard(SaturatedTentativeTreesLock_);

    auto it = TentativeTreeIdToSaturationTime_.find(treeId);
    if (it == TentativeTreeIdToSaturationTime_.end()) {
        return false;
    }

    auto saturationTime = it->second;
    return saturationTime + DurationToCpuDuration(saturationDeactivationTimeout) > now;
}

void TFairShareStrategyOperationController::UpdateConfig(const TFairShareStrategyOperationControllerConfigPtr& config)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    Config_.Store(config);

    EnableConcurrentScheduleAllocationExecDurationThrottling_.store(
        config->EnableConcurrentScheduleAllocationExecDurationThrottling,
        std::memory_order_release);
    UpdateConcurrentScheduleAllocationThrottlingLimits(config);
}

TFairShareStrategyOperationControllerConfigPtr TFairShareStrategyOperationController::GetConfig()
{
    return Config_.Acquire();
}

void TFairShareStrategyOperationController::SetDetailedLogsEnabled(bool enabled)
{
    YT_ASSERT_THREAD_AFFINITY(ControlThread);

    DetailedLogsEnabled_ = enabled;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
