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
    int nodeShardCount)
    : Controller_(operation->GetControllerStrategyHost())
    , OperationId_(operation->GetId())
    , Logger(StrategyLogger.WithTag("OperationId: %v", OperationId_))
    , Config_(config)
    , NodeShardCount_(nodeShardCount)
    , ScheduleJobControllerThrottlingBackoff_(
        DurationToCpuDuration(config->ControllerThrottling->ScheduleJobStartBackoffTime))
{
    YT_VERIFY(Controller_);

    // NB(eshcherbin): To initialize throttling options.
    UpdateConfig(config);
}

TControllerEpoch TFairShareStrategyOperationController::GetEpoch() const
{
    return Controller_->GetEpoch();
}

void TFairShareStrategyOperationController::OnScheduleJobStarted(const ISchedulingContextPtr& schedulingContext)
{
    auto nodeShardId = schedulingContext->GetNodeShardId();
    auto& shard = StateShards_[nodeShardId];
    ++shard.ConcurrentScheduleJobCalls;
    shard.ScheduleJobCallsSinceLastUpdate.fetch_add(1, std::memory_order::relaxed);
    shard.ConcurrentScheduleJobExecDuration += shard.ScheduleJobExecDurationEstimate;

    schedulingContext->StoreScheduleJobExecDurationEstimate(shard.ScheduleJobExecDurationEstimate);

    YT_LOG_DEBUG_IF(DetailedLogsEnabled_,
        "Controller schedule job started "
        "(ConcurrentScheduleJobCalls: %v, ConcurrentScheduleJobExecDuration: %v, "
        "ScheduleJobExecDurationEstimate: %v, NodeShardId: %v)",
        shard.ConcurrentScheduleJobCalls,
        shard.ConcurrentScheduleJobExecDuration,
        shard.ScheduleJobExecDurationEstimate,
        nodeShardId);
}

void TFairShareStrategyOperationController::OnScheduleJobFinished(const ISchedulingContextPtr& schedulingContext)
{
    auto nodeShardId = schedulingContext->GetNodeShardId();
    auto& shard = StateShards_[nodeShardId];
    --shard.ConcurrentScheduleJobCalls;
    shard.ConcurrentScheduleJobExecDuration -= schedulingContext->ExtractScheduleJobExecDurationEstimate();

    YT_LOG_DEBUG_IF(DetailedLogsEnabled_,
        "Controller schedule job finished "
        "(ConcurrentScheduleJobCalls: %v, ConcurrentScheduleJobExecDuration: %v, "
        "ScheduleJobExecDurationEstimate: %v, NodeShardId: %v)",
        shard.ConcurrentScheduleJobCalls,
        shard.ConcurrentScheduleJobExecDuration,
        shard.ScheduleJobExecDurationEstimate,
        nodeShardId);
}

TCompositeNeededResources TFairShareStrategyOperationController::GetNeededResources() const
{
    return Controller_->GetNeededResources();
}

TJobResourcesWithQuotaList TFairShareStrategyOperationController::GetDetailedMinNeededJobResources() const
{
    return Controller_->GetMinNeededJobResources();
}

TJobResources TFairShareStrategyOperationController::GetAggregatedMinNeededJobResources() const
{
    // Min needed resources must be less than total needed resources of operation. See YT-9363.
    auto result = GetNeededResources().DefaultResources;
    for (const auto& jobResources : GetDetailedMinNeededJobResources()) {
        result = Min(result, jobResources.ToJobResources());
    }

    return result;
}

TJobResources TFairShareStrategyOperationController::GetAggregatedInitialMinNeededJobResources() const
{
    auto initialMinNeededResources = Controller_->GetInitialMinNeededJobResources();
    if (initialMinNeededResources.empty()) {
        // A reasonable fallback, but this should really never happen.
        return TJobResources();
    }

    auto result = initialMinNeededResources.front().ToJobResources();
    for (const auto& jobResources : initialMinNeededResources) {
        result = Min(result, jobResources.ToJobResources());
    }

    return result;
}

void TFairShareStrategyOperationController::UpdateMinNeededJobResources()
{
    Controller_->UpdateMinNeededJobResources();
}

void TFairShareStrategyOperationController::UpdateConcurrentScheduleJobThrottlingLimits(
    const TFairShareStrategyOperationControllerConfigPtr& config)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    {
        auto regularizedLimit = config->MaxConcurrentControllerScheduleJobCalls * config->ConcurrentControllerScheduleJobCallsRegularization;
        auto value = static_cast<int>(regularizedLimit / NodeShardCount_);
        value = std::max(value, 1);
        MaxConcurrentControllerScheduleJobCallsPerNodeShard_.store(value, std::memory_order::release);
    }

    {
        auto regularizedLimit = config->MaxConcurrentControllerScheduleJobExecDuration * config->ConcurrentControllerScheduleJobCallsRegularization;
        auto value = regularizedLimit / NodeShardCount_;
        value = std::max(value, TDuration::FromValue(1));
        MaxConcurrentControllerScheduleJobExecDurationPerNodeShard_.store(value, std::memory_order::release);
    }
}

bool TFairShareStrategyOperationController::CheckMaxScheduleJobCallsOverdraft(int maxScheduleJobCalls) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    for (auto& shard : StateShards_) {
        ScheduleJobCallsOverdraft_ += shard.ScheduleJobCallsSinceLastUpdate.exchange(0);
    }
    ScheduleJobCallsOverdraft_ = std::max(0, ScheduleJobCallsOverdraft_ - maxScheduleJobCalls);

    return ScheduleJobCallsOverdraft_ > 0;
}

bool TFairShareStrategyOperationController::IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const
{
    auto nodeShardId = schedulingContext->GetNodeShardId();
    auto& shard = StateShards_[nodeShardId];
    auto limit = MaxConcurrentControllerScheduleJobCallsPerNodeShard_.load(std::memory_order::acquire);
    bool limitViolated = shard.ConcurrentScheduleJobCalls >= limit;

    YT_LOG_DEBUG_IF(limitViolated && DetailedLogsEnabled_,
        "Max concurrent schedule job calls per node shard violated (ConcurrentScheduleJobCalls: %v, Limit: %v, NodeShardId: %v)",
        shard.ConcurrentScheduleJobCalls,
        limit,
        nodeShardId);

    return limitViolated;
}

bool TFairShareStrategyOperationController::IsMaxConcurrentScheduleJobExecDurationPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const
{
    if (!EnableConcurrentScheduleJobExecDurationThrottling_.load(std::memory_order_acquire)) {
        return false;
    }

    auto nodeShardId = schedulingContext->GetNodeShardId();
    auto& shard = StateShards_[nodeShardId];
    auto limit = MaxConcurrentControllerScheduleJobExecDurationPerNodeShard_.load(std::memory_order::acquire);
    bool limitViolated = shard.ConcurrentScheduleJobExecDuration >= limit;

    YT_LOG_DEBUG_IF(limitViolated && DetailedLogsEnabled_,
        "Max concurrent schedule job exec duration per node shard violated "
        "(ConcurrentScheduleJobExecDuration: %v, Limit: %v, "
        "ScheduleJobExecDurationEstimate: %v, NodeShardId: %v)",
        shard.ConcurrentScheduleJobExecDuration,
        limit,
        shard.ScheduleJobExecDurationEstimate,
        nodeShardId);

    return limitViolated;
}

bool TFairShareStrategyOperationController::HasRecentScheduleJobFailure(TCpuInstant now) const
{
    return ScheduleJobBackoffDeadline_ > now;
}

bool TFairShareStrategyOperationController::ScheduleJobBackoffObserved() const
{
    return ScheduleJobBackoffObserved_.load();
}

void TFairShareStrategyOperationController::AbortJob(TJobId jobId, EAbortReason abortReason, TControllerEpoch jobEpoch)
{
    Controller_->OnNonscheduledJobAborted(jobId, abortReason, jobEpoch);
}

TControllerScheduleJobResultPtr TFairShareStrategyOperationController::ScheduleJob(
    const ISchedulingContextPtr& context,
    const TJobResources& availableResources,
    const TDiskResources& availableDiskResources,
    TDuration timeLimit,
    const TString& treeId,
    const TString& poolPath,
    const TFairShareStrategyTreeConfigPtr& treeConfig)
{
    auto scheduleJobResultFuture = Controller_->ScheduleJob(context, availableResources, availableDiskResources, treeId, poolPath, treeConfig);

    auto scheduleJobResultFutureWithTimeout = scheduleJobResultFuture
        .ToUncancelable()
        .WithTimeout(timeLimit);

    auto config = Config_.Acquire();

    auto startTime = TInstant::Now();
    scheduleJobResultFuture.Subscribe(
        BIND([this, this_ = MakeStrong(this), startTime, longScheduleJobThreshold = config->LongScheduleJobLoggingThreshold] (const TError& /*error*/) {
            auto now = TInstant::Now();
            if (startTime + longScheduleJobThreshold < now) {
                YT_LOG_DEBUG("Schedule job takes too long (Duration: %v ms, LongScheduleJobThreshold: %v ms)",
                    (now - startTime).MilliSeconds(),
                    longScheduleJobThreshold.MilliSeconds());
            }
        }));

    auto maybeUpdateDurationEstimate = [this, nodeSharId = context->GetNodeShardId()] (const TControllerScheduleJobResultPtr& result) {
        if (auto estimate = result->NextDurationEstimate) {
            auto& shard = StateShards_[nodeSharId];
            shard.ScheduleJobExecDurationEstimate = *estimate;
        }
    };

    auto scheduleJobResultWithTimeoutOrError = WaitFor(scheduleJobResultFutureWithTimeout);
    if (!scheduleJobResultWithTimeoutOrError.IsOK()) {
        auto scheduleJobResult = New<TControllerScheduleJobResult>();
        if (scheduleJobResultWithTimeoutOrError.GetCode() == NYT::EErrorCode::Timeout) {
            scheduleJobResult->RecordFail(EScheduleJobFailReason::Timeout);
            // If ScheduleJob was not canceled we need to abort created job.
            scheduleJobResultFuture.Subscribe(
                BIND([this, this_ = MakeStrong(this), maybeUpdateDurationEstimate] (const TErrorOr<TControllerScheduleJobResultPtr>& scheduleJobResultOrError) {
                    if (!scheduleJobResultOrError.IsOK()) {
                        return;
                    }

                    const auto& scheduleJobResult = scheduleJobResultOrError.Value();
                    if (scheduleJobResult->StartDescriptor) {
                        auto jobId = scheduleJobResult->StartDescriptor->Id;
                        YT_LOG_WARNING("Aborting late job (JobId: %v)",
                            jobId);
                        AbortJob(
                            jobId,
                            EAbortReason::SchedulingTimeout,
                            scheduleJobResult->ControllerEpoch);
                    }

                    maybeUpdateDurationEstimate(scheduleJobResult);
            }));
        }
        return scheduleJobResult;
    }

    const auto& scheduleJobResult = scheduleJobResultWithTimeoutOrError.Value();
    maybeUpdateDurationEstimate(scheduleJobResult);

    return scheduleJobResult;
}

void TFairShareStrategyOperationController::OnScheduleJobFailed(
    TCpuInstant now,
    const TString& treeId,
    const TControllerScheduleJobResultPtr& scheduleJobResult)
{
    auto config = GetConfig();

    TCpuInstant backoffDeadline = 0;
    if (scheduleJobResult->Failed[EScheduleJobFailReason::ControllerThrottling] > 0) {
        auto value = ScheduleJobControllerThrottlingBackoff_.load();
        backoffDeadline = now + value;

        {
            auto newValue = std::min(
                DurationToCpuDuration(config->ControllerThrottling->ScheduleJobMaxBackoffTime),
                TCpuDuration(value * config->ControllerThrottling->ScheduleJobBackoffMultiplier));
            // Nobody cares if some of concurrent updates fail.
            ScheduleJobControllerThrottlingBackoff_.compare_exchange_weak(value, newValue);
        }

    } else {
        ScheduleJobControllerThrottlingBackoff_.store(
            DurationToCpuDuration(config->ControllerThrottling->ScheduleJobStartBackoffTime));

        if (scheduleJobResult->IsBackoffNeeded()) {
            backoffDeadline = now + DurationToCpuDuration(config->ScheduleJobFailBackoffTime);
        }
    }

    if (backoffDeadline > 0) {
        YT_LOG_DEBUG("Failed to schedule job, backing off (Duration: %v, Reasons: %v)",
            backoffDeadline - now,
            scheduleJobResult->Failed);
        ScheduleJobBackoffDeadline_.store(backoffDeadline);
        ScheduleJobBackoffObserved_.store(true);
    }

    if (scheduleJobResult->Failed[EScheduleJobFailReason::TentativeTreeDeclined] > 0) {
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
    VERIFY_THREAD_AFFINITY(ControlThread);

    Config_.Store(config);

    EnableConcurrentScheduleJobExecDurationThrottling_.store(config->EnableConcurrentScheduleJobExecDurationThrottling, std::memory_order_release);
    UpdateConcurrentScheduleJobThrottlingLimits(config);
}

TFairShareStrategyOperationControllerConfigPtr TFairShareStrategyOperationController::GetConfig()
{
    return Config_.Acquire();
}

void TFairShareStrategyOperationController::SetDetailedLogsEnabled(bool enabled)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    DetailedLogsEnabled_ = enabled;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
