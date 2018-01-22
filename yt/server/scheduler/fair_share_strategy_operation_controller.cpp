#include "fair_share_strategy_operation_controller.h"
#include "scheduling_context.h"

#include <yt/server/controller_agent/scheduling_context.h>

#include "operation_controller.h"

namespace NYT {
namespace NScheduler {

using namespace NConcurrency;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

class TSchedulingContextAdapter
    : public NControllerAgent::ISchedulingContext
{
public:
    explicit TSchedulingContextAdapter(NScheduler::ISchedulingContextPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    virtual const TExecNodeDescriptor& GetNodeDescriptor() const override
    {
        return Underlying_->GetNodeDescriptor();
    }

    virtual const TJobResources& ResourceLimits() const override
    {
        return Underlying_->ResourceLimits();
    }

    virtual const NNodeTrackerClient::NProto::TDiskResources& DiskInfo() const override
    {
        return Underlying_->DiskInfo();
    }

    virtual TJobId GenerateJobId() override
    {
        return Underlying_->GenerateJobId();
    }

    virtual NProfiling::TCpuInstant GetNow() const override
    {
        return NProfiling::GetCpuInstant();
    }

private:
    const NScheduler::ISchedulingContextPtr Underlying_;
};

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

    return ConcurrentScheduleJobCalls_ >= maxConcurrentScheduleJobCalls ||
        LastScheduleJobFailTime_ + controllerScheduleJobFailBackoffTime > now;
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
    auto scheduleJobResultFuture =
        BIND([controller = Controller_, context, jobLimits, treeId] {
            TSchedulingContextAdapter adapter(context);
            return controller->ScheduleJob(&adapter, jobLimits, treeId);
        })
        .AsyncVia(Controller_->GetCancelableInvoker())
        .Run();

    auto scheduleJobResultFutureWithTimeout = scheduleJobResultFuture
        .WithTimeout(timeLimit);

    auto scheduleJobResultWithTimeoutOrError = WaitFor(scheduleJobResultFutureWithTimeout);

    if (!scheduleJobResultWithTimeoutOrError.IsOK()) {
        auto scheduleJobResult = New<TScheduleJobResult>();
        if (scheduleJobResultWithTimeoutOrError.GetCode() == NYT::EErrorCode::Timeout) {
            ++scheduleJobResult->Failed[EScheduleJobFailReason::Timeout];
            // If ScheduleJob was not canceled we need to abort created job.
            scheduleJobResultFuture.Subscribe(
                BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TScheduleJobResultPtr>& scheduleJobResultOrError) {
                    if (!scheduleJobResultOrError.IsOK()) {
                        return;
                    }

                    const auto& scheduleJobResult = scheduleJobResultOrError.Value();
                    if (scheduleJobResult->JobStartRequest) {
                        const auto& jobId = scheduleJobResult->JobStartRequest->Id;
                        LOG_WARNING("Aborting late job (JobId: %v, OperationId: %v)",
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
