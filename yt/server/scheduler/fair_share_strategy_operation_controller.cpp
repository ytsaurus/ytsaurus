#include "fair_share_strategy_operation_controller.h"

#include "operation_controller.h"

namespace NYT::NScheduler {

using namespace NConcurrency;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyOperationController::TFairShareStrategyOperationController(
    IOperationStrategyHost* operation)
    : Controller_(operation->GetControllerStrategyHost())
    , OperationId_(operation->GetId())
    , Logger(NLogging::TLogger(SchedulerLogger)
        .AddTag("OperationId: %v", OperationId_))
{
    YCHECK(Controller_);
}

void TFairShareStrategyOperationController::IncreaseConcurrentScheduleJobCalls()
{
    ++ConcurrentScheduleJobCalls_;
}

void TFairShareStrategyOperationController::DecreaseConcurrentScheduleJobCalls()
{
    --ConcurrentScheduleJobCalls_;
}

void TFairShareStrategyOperationController::SetLastScheduleJobFailTime(NProfiling::TCpuInstant now)
{
    LastScheduleJobFailTime_ = now;
}

TJobResourcesWithQuotaList TFairShareStrategyOperationController::GetDetailedMinNeededJobResources() const
{
    return Controller_->GetMinNeededJobResources();
}

TJobResources TFairShareStrategyOperationController::GetAggregatedMinNeededJobResources() const
{
    // Min needed resources must be less than total needed resources of operation. See YT-9363.
    auto result = GetNeededResources();
    for (const auto& jobResources : GetDetailedMinNeededJobResources()) {
        result = Min(result, jobResources.ToJobResources());
    }
    return result;
}

void TFairShareStrategyOperationController::UpdateMinNeededJobResources()
{
    Controller_->UpdateMinNeededJobResources();
}

bool TFairShareStrategyOperationController::IsBlocked(
    NProfiling::TCpuInstant now,
    int maxConcurrentScheduleJobCalls,
    TDuration scheduleJobFailBackoffTime) const
{
    auto concurrentScheduleJobCalls = ConcurrentScheduleJobCalls_.load();
    if (concurrentScheduleJobCalls >= maxConcurrentScheduleJobCalls) {
        YT_LOG_DEBUG_UNLESS(Blocked_,
            "Operation blocked in fair share strategy due to violation of maximum concurrent schedule job calls (ConcurrentScheduleJobCalls: %v)",
            concurrentScheduleJobCalls);
        Blocked_.store(true);
        return true;
    }

    if (LastScheduleJobFailTime_ + NProfiling::DurationToCpuDuration(scheduleJobFailBackoffTime) > now) {
        YT_LOG_DEBUG_UNLESS(Blocked_, "Operation blocked in fair share strategy due to schedule job failure");
        Blocked_.store(true);
        return true;
    }

    YT_LOG_DEBUG_UNLESS(!Blocked_, "Operation unblocked in fair share strategy");
    Blocked_.store(false);
    return false;
}

void TFairShareStrategyOperationController::AbortJob(TJobId jobId, EAbortReason abortReason)
{
    Controller_->OnNonscheduledJobAborted(jobId, abortReason);
}

TScheduleJobResultPtr TFairShareStrategyOperationController::ScheduleJob(
    const ISchedulingContextPtr& context,
    const TJobResources& availableResources,
    TDuration timeLimit,
    const TString& treeId)
{
    auto scheduleJobResultFuture = Controller_->ScheduleJob(context, availableResources, treeId);

    auto scheduleJobResultFutureWithTimeout = scheduleJobResultFuture
        .WithTimeout(timeLimit);

    auto scheduleJobResultWithTimeoutOrError = WaitFor(scheduleJobResultFutureWithTimeout);

    if (!scheduleJobResultWithTimeoutOrError.IsOK()) {
        auto scheduleJobResult = New<TScheduleJobResult>();
        if (scheduleJobResultWithTimeoutOrError.GetCode() == NYT::EErrorCode::Timeout) {
            scheduleJobResult->RecordFail(EScheduleJobFailReason::Timeout);
            // If ScheduleJob was not canceled we need to abort created job.
            scheduleJobResultFuture.Subscribe(
                BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TScheduleJobResultPtr>& scheduleJobResultOrError) {
                    if (!scheduleJobResultOrError.IsOK()) {
                        return;
                    }

                    const auto& scheduleJobResult = scheduleJobResultOrError.Value();
                    if (scheduleJobResult->StartDescriptor) {
                        const auto& jobId = scheduleJobResult->StartDescriptor->Id;
                        YT_LOG_WARNING("Aborting late job (JobId: %v)",
                            jobId);
                        AbortJob(jobId, EAbortReason::SchedulingTimeout);
                    }
            }));
        }
        return scheduleJobResult;
    }

    return scheduleJobResultWithTimeoutOrError.Value();
}

int TFairShareStrategyOperationController::GetPendingJobCount() const
{
    return Controller_->GetPendingJobCount();
}

TJobResources TFairShareStrategyOperationController::GetNeededResources() const
{
    return Controller_->GetNeededResources();
}

void TFairShareStrategyOperationController::OnTentativeTreeScheduleJobFailed(NProfiling::TCpuInstant now, const TString& treeId)
{
    TWriterGuard guard(SaturatedTentativeTreesLock_);

    TentativeTreeIdToSaturationTime_[treeId] = now;
}

bool TFairShareStrategyOperationController::IsSaturatedInTentativeTree(NProfiling::TCpuInstant now, const TString& treeId, TDuration saturationDeactivationTimeout) const
{
    TReaderGuard guard(SaturatedTentativeTreesLock_);

    auto it = TentativeTreeIdToSaturationTime_.find(treeId);
    if (it == TentativeTreeIdToSaturationTime_.end()) {
        return false;
    }

    auto saturationTime = it->second;
    return saturationTime + NProfiling::DurationToCpuDuration(saturationDeactivationTimeout) > now;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
