#include "fair_share_strategy_operation_controller.h"

namespace NYT {
namespace NScheduler {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

TFairShareStrategyOperationController::TFairShareStrategyOperationController(
    IOperationStrategyHost* operation)
    : Controller_(operation->GetControllerStrategyHost())
    , OperationId_(operation->GetId())
{ }

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

void TFairShareStrategyOperationController::SetMinNeededJobResources(std::vector<TJobResourcesWithQuota> jobResourcesList)
{
    TWriterGuard guard(CachedMinNeededJobResourcesLock_);
    CachedMinNeededJobResourcesList_ = std::move(jobResourcesList);

    CachedMinNeededJobResources_ = InfiniteJobResourcesWithQuota();
    for (const auto& jobResources : CachedMinNeededJobResourcesList_) {
        CachedMinNeededJobResources_ = Min(CachedMinNeededJobResources_, jobResources);
    }
}

std::vector<TJobResourcesWithQuota> TFairShareStrategyOperationController::GetMinNeededJobResourcesList() const
{
    TReaderGuard guard(CachedMinNeededJobResourcesLock_);
    return CachedMinNeededJobResourcesList_;
}

TJobResourcesWithQuota TFairShareStrategyOperationController::GetMinNeededJobResources() const
{
    TReaderGuard guard(CachedMinNeededJobResourcesLock_);
    return CachedMinNeededJobResources_;
}

void TFairShareStrategyOperationController::InvokeMinNeededJobResourcesUpdate()
{
    BIND(&NControllerAgent::IOperationControllerSchedulerHost::GetMinNeededJobResources, Controller_)
        .AsyncVia(Controller_->GetCancelableInvoker())
        .Run()
        .Subscribe(
            BIND([this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TJobResourcesWithQuota>>& resultOrError) {
                if (!resultOrError.IsOK()) {
                    LOG_WARNING(resultOrError, "Failed to update min needed resources from controller");
                    return;
                }
                SetMinNeededJobResources(std::move(resultOrError.Value()));
        }));
}

bool TFairShareStrategyOperationController::IsBlocked(
    NProfiling::TCpuInstant now,
    int maxConcurrentScheduleJobCalls,
    TDuration scheduleJobFailBackoffTime) const
{
    auto controllerScheduleJobFailBackoffTime = NProfiling::DurationToCpuDuration(
        scheduleJobFailBackoffTime);

    return ConcurrentScheduleJobCalls_ >= maxConcurrentScheduleJobCalls ||
        LastScheduleJobFailTime_ + controllerScheduleJobFailBackoffTime > now;
}

void TFairShareStrategyOperationController::AbortJob(std::unique_ptr<TAbortedJobSummary> abortedJobSummary)
{
    Controller_->GetCancelableInvoker()->Invoke(BIND(
        &NControllerAgent::IOperationControllerSchedulerHost::OnJobAborted,
        Controller_,
        Passed(std::move(abortedJobSummary))));
}

TScheduleJobResultPtr TFairShareStrategyOperationController::ScheduleJob(
    const ISchedulingContextPtr& context,
    const TJobResources& jobLimits,
    TDuration timeLimit,
    const TString& treeId)
{
    auto scheduleJobResultFuture = BIND(&NControllerAgent::IOperationControllerStrategyHost::ScheduleJob, Controller_)
        .AsyncVia(Controller_->GetCancelableInvoker())
        .Run(context, jobLimits, treeId);

    auto scheduleJobResultFutureWithTimeout = scheduleJobResultFuture
        .WithTimeout(timeLimit);

    auto scheduleJobResultWithTimeoutOrError = WaitFor(scheduleJobResultFutureWithTimeout);

    if (!scheduleJobResultWithTimeoutOrError.IsOK()) {
        auto scheduleJobResult = New<TScheduleJobResult>();
        if (scheduleJobResultWithTimeoutOrError.GetCode() == NYT::EErrorCode::Timeout) {
            ++scheduleJobResult->Failed[EScheduleJobFailReason::Timeout];
            // If ScheduleJob was not canceled we need to abort created job.
            scheduleJobResultFuture.Subscribe(
                BIND([this_ = MakeStrong(this)] (const TErrorOr<TScheduleJobResultPtr>& scheduleJobResultOrError) {
                    if (!scheduleJobResultOrError.IsOK()) {
                        return;
                    }

                    const auto& scheduleJobResult = scheduleJobResultOrError.Value();
                    if (scheduleJobResult->JobStartRequest) {
                        const auto& jobId = scheduleJobResult->JobStartRequest->Id;
                        LOG_WARNING("Aborting late job (JobId: %v, OperationId: %v)",
                            jobId,
                            this_->OperationId_);
                        this_->Controller_->OnJobAborted(
                            std::make_unique<TAbortedJobSummary>(jobId, EAbortReason::SchedulingTimeout));
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
