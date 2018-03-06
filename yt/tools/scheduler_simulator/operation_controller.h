#pragma once

#include "private.h"
#include "operation_description.h"
#include "scheduling_context.h"

#include <yt/server/scheduler/operation.h>
#include <yt/server/scheduler/operation_controller.h>

#include <yt/core/logging/log_manager.h>

#include <deque>
#include <map>

namespace NYT {
namespace NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

class TOperationController
    : public NScheduler::IOperationControllerStrategyHost
{
public:
    TOperationController(const TOperation* operation, const TOperationDescription* operationDescription);

    //! Returns the number of jobs the controller still needs to start right away.
    virtual int GetPendingJobCount() const override;

    //! Returns the total resources that are additionally needed.
    virtual NScheduler::TJobResources GetNeededResources() const override;

    void OnJobCompleted(std::unique_ptr<NControllerAgent::TCompletedJobSummary> jobSummary);

    virtual void OnNonscheduledJobAborted(const NScheduler::TJobId&, NScheduler::EAbortReason) override;

    bool IsOperationCompleted() const;

    //! Called during heartbeat processing to request actions the node must perform.
    virtual TFuture<NControllerAgent::TScheduleJobResultPtr> ScheduleJob(
        const NScheduler::ISchedulingContextPtr& context,
        const NScheduler::TJobResourcesWithQuota& nodeLimits,
        const TString& /* treeId */) override;

    virtual void UpdateMinNeededJobResources() override;
    virtual NScheduler::TJobResourcesWithQuotaList GetMinNeededJobResources() const override;

    TString GetLoggingProgress() const;

private:
    const TOperation* Operation_;
    const TOperationDescription* OperationDescription_;
    bool OperationCompleted_ = false;
    int StagePendingJobCount_ = 0;
    int CompletedJobCount_ = 0;
    int StageCompletedJobCount_ = 0;
    int RunningJobCount_ = 0;
    int CurrentStage_ = 0;
    int AbortedJobCount_ = 0;
    NScheduler::TJobResources NeededResources_;

    std::vector<std::deque<TJobDescription>> PendingJobs_;
    std::vector<int> StageJobCounts_;
    std::map<TGuid, TJobDescription> IdToDescription_;

    NLogging::TLogger Logger;

    void SetStage(int stage);
};

DEFINE_REFCOUNTED_TYPE(TOperationController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSchedulerSimulator
} // namespace NYT
