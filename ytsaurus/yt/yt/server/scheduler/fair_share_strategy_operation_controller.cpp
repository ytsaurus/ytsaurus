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
    int NodeShardCount)
    : Controller_(operation->GetControllerStrategyHost())
    , OperationId_(operation->GetId())
    , Logger(StrategyLogger.WithTag("OperationId: %v", OperationId_))
    , Config_(config)
    , NodeShardCount_(NodeShardCount)
    , ScheduleJobControllerThrottlingBackoff_(
        DurationToCpuDuration(config->ControllerThrottling->ScheduleJobStartBackoffTime))
{
    UpdateMaxConcurrentControllerScheduleJobCallsPerNodeShard(config);
    YT_VERIFY(Controller_);
}

TControllerEpoch TFairShareStrategyOperationController::GetEpoch() const
{
    return Controller_->GetEpoch();
}

void TFairShareStrategyOperationController::IncreaseConcurrentScheduleJobCalls(const ISchedulingContextPtr& schedulingContext)
{
    auto& shard = StateShards_[schedulingContext->GetNodeShardId()];
    shard.ConcurrentScheduleJobCalls += 1;
}

void TFairShareStrategyOperationController::DecreaseConcurrentScheduleJobCalls(const ISchedulingContextPtr& schedulingContext)
{
    auto& shard = StateShards_[schedulingContext->GetNodeShardId()];
    shard.ConcurrentScheduleJobCalls -= 1;
}

void TFairShareStrategyOperationController::IncreaseScheduleJobCallsSinceLastUpdate(const ISchedulingContextPtr& schedulingContext)
{
    auto& shard = StateShards_[schedulingContext->GetNodeShardId()];
    shard.ScheduleJobCallsSinceLastUpdate.fetch_add(1, std::memory_order::relaxed);
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

void TFairShareStrategyOperationController::UpdateMaxConcurrentControllerScheduleJobCallsPerNodeShard(
    const TFairShareStrategyOperationControllerConfigPtr& config)
{
    int value = static_cast<int>(
        config->MaxConcurrentControllerScheduleJobCalls * config->ConcurrentControllerScheduleJobCallsRegularization / NodeShardCount_);
    value = std::max(value, 1);
    MaxConcurrentControllerScheduleJobCallsPerNodeShard_.store(value, std::memory_order::release);
}

void TFairShareStrategyOperationController::CheckMaxScheduleJobCallsOverdraft(
    int maxScheduleJobCalls,
    bool* isMaxScheduleJobCallsViolated) const
{
    for (auto& shard : StateShards_) {
        ScheduleJobCallsOverdraft_ += shard.ScheduleJobCallsSinceLastUpdate.exchange(0);
    }
    ScheduleJobCallsOverdraft_ = std::max(0, ScheduleJobCallsOverdraft_ - maxScheduleJobCalls);

    *isMaxScheduleJobCallsViolated = ScheduleJobCallsOverdraft_ > 0;
}

bool TFairShareStrategyOperationController::IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const
{
    auto& shard = StateShards_[schedulingContext->GetNodeShardId()];
    return shard.ConcurrentScheduleJobCalls >= MaxConcurrentControllerScheduleJobCallsPerNodeShard_.load(std::memory_order::acquire);
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
    TDuration timeLimit,
    const TString& treeId,
    const TString& poolPath,
    const TFairShareStrategyTreeConfigPtr& treeConfig)
{
    auto scheduleJobResultFuture = Controller_->ScheduleJob(context, availableResources, treeId, poolPath, treeConfig);

    auto scheduleJobResultFutureWithTimeout = scheduleJobResultFuture
        .ToUncancelable()
        .WithTimeout(timeLimit);

    auto config = Config_.Acquire();

    auto startTime = TInstant::Now();
    scheduleJobResultFuture.Subscribe(
        BIND([this, this_ = MakeStrong(this), startTime, longScheduleJobThreshold = config->LongScheduleJobLoggingThreshold] (const TError& /* error */) {
            auto now = TInstant::Now();
            if (startTime + longScheduleJobThreshold < now) {
                YT_LOG_DEBUG("Schedule job takes more than %v ms (Duration: %v ms)",
                    longScheduleJobThreshold.MilliSeconds(),
                    (now - startTime).MilliSeconds());
            }
        }));

    auto scheduleJobResultWithTimeoutOrError = WaitFor(scheduleJobResultFutureWithTimeout);

    if (!scheduleJobResultWithTimeoutOrError.IsOK()) {
        auto scheduleJobResult = New<TControllerScheduleJobResult>();
        if (scheduleJobResultWithTimeoutOrError.GetCode() == NYT::EErrorCode::Timeout) {
            scheduleJobResult->RecordFail(EScheduleJobFailReason::Timeout);
            // If ScheduleJob was not canceled we need to abort created job.
            scheduleJobResultFuture.Subscribe(
                BIND([this, this_ = MakeStrong(this), treeId] (const TErrorOr<TControllerScheduleJobResultPtr>& scheduleJobResultOrError) {
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
            }));
        }
        return scheduleJobResult;
    }

    return scheduleJobResultWithTimeoutOrError.Value();
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
    Config_.Store(config);
    UpdateMaxConcurrentControllerScheduleJobCallsPerNodeShard(config);
}

TFairShareStrategyOperationControllerConfigPtr TFairShareStrategyOperationController::GetConfig()
{
    return Config_.Acquire();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
