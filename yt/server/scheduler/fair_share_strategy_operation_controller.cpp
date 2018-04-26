#include "fair_share_strategy_operation_controller.h"

#include "operation_controller.h"

namespace NYT {
namespace NScheduler {

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
    auto result = InfiniteJobResourcesWithQuota();
    for (const auto& jobResources : GetDetailedMinNeededJobResources()) {
        result = Min(result, jobResources);
    }
    return result.ToJobResources();
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
    auto controllerScheduleJobFailBackoffTime = NProfiling::DurationToCpuDuration(
        scheduleJobFailBackoffTime);

    if (ConcurrentScheduleJobCalls_ >= maxConcurrentScheduleJobCalls) {
        LOG_DEBUG_UNLESS(IsBlocked_,
            "Operation blocked in fair share strategy due to violation of maximum concurrect schedule job calls (ConcurrentScheduleJobCalls: %v)",
            ConcurrentScheduleJobCalls_.load());
        IsBlocked_.store(true);
        return true;
    }

    if (LastScheduleJobFailTime_ + controllerScheduleJobFailBackoffTime > now) {
        LOG_DEBUG_UNLESS(IsBlocked_, "Operation blocked in fair share strategy due to schedule job failure");
        IsBlocked_.store(true);
        return true;
    }

    LOG_DEBUG_UNLESS(!IsBlocked_, "Operation unblocked in fair share strategy");
    IsBlocked_.store(false);
    return false;
}

void TFairShareStrategyOperationController::AbortJob(const TJobId& jobId, EAbortReason abortReason)
{
    Controller_->OnNonscheduledJobAborted(jobId, abortReason);
}

TScheduleJobResultPtr TFairShareStrategyOperationController::ScheduleJob(
    const ISchedulingContextPtr& context,
    const TJobResources& jobLimits,
    TDuration timeLimit,
    const TString& treeId)
{
    auto scheduleJobResultFuture = Controller_->ScheduleJob(context, jobLimits, treeId);

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
                        LOG_WARNING("Aborting late job (JobId: %v)",
                            jobId,
                            OperationId_);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
