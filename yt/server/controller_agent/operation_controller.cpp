#include "operation_controller.h"

#include "helpers.h"
#include "ordered_controller.h"
#include "sort_controller.h"
#include "sorted_controller.h"
#include "unordered_controller.h"

#include <yt/server/scheduler/operation.h>

#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/yson/consumer.h>
#include <yt/core/yson/string.h>

namespace NYT {
namespace NControllerAgent {

using namespace NApi;
using namespace NScheduler;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

//! Ensures that operation controllers are being destroyed in a
//! dedicated invoker.
class TOperationControllerWrapper
    : public IOperationController
{
public:
    TOperationControllerWrapper(
        const TOperationId& id,
        IOperationControllerPtr underlying,
        IInvokerPtr dtorInvoker)
        : Id_(id)
        , Underlying_(std::move(underlying))
        , DtorInvoker_(std::move(dtorInvoker))
    { }

    virtual ~TOperationControllerWrapper()
    {
        DtorInvoker_->Invoke(BIND([underlying = std::move(Underlying_), id = Id_] () mutable {
            auto Logger = OperationLogger;
            Logger.AddTag("OperationId: %v", id);
            NProfiling::TWallTimer timer;
            LOG_INFO("Started destroying operation controller");
            underlying.Reset();
            LOG_INFO("Finished destroying operation controller (Elapsed: %v)",
                timer.GetElapsedTime());
        }));
    }

    virtual void Initialize() override
    {
        Underlying_->Initialize();
    }

    virtual void InitializeReviving(TControllerTransactionsPtr operationTransactions) override
    {
        Underlying_->InitializeReviving(std::move(operationTransactions));
    }

    virtual void Prepare() override
    {
        Underlying_->Prepare();
    }

    virtual void Materialize() override
    {
        Underlying_->Materialize();
    }

    virtual void Commit() override
    {
        Underlying_->Commit();
    }

    virtual void SaveSnapshot(IOutputStream* stream) override
    {
        Underlying_->SaveSnapshot(stream);
    }

    virtual void Revive() override
    {
        Underlying_->Revive();
    }

    virtual void Abort() override
    {
        Underlying_->Abort();
    }

    virtual void Forget() override
    {
        Underlying_->Forget();
    }

    virtual TOperationControllerInitializationAttributes GetInitializationAttributes() const override
    {
        return Underlying_->GetInitializationAttributes();
    }

    virtual NYson::TYsonString GetAttributes() const override
    {
        return Underlying_->GetAttributes();
    }

    virtual void OnTransactionAborted(const TTransactionId& transactionId) override
    {
        Underlying_->OnTransactionAborted(transactionId);
    }

    virtual std::vector<ITransactionPtr> GetTransactions() override
    {
        return Underlying_->GetTransactions();
    }

    virtual void Complete() override
    {
        Underlying_->Complete();
    }

    virtual TCancelableContextPtr GetCancelableContext() const override
    {
        return Underlying_->GetCancelableContext();
    }

    virtual IInvokerPtr GetCancelableInvoker() const override
    {
        return Underlying_->GetCancelableInvoker();
    }

    virtual IInvokerPtr GetInvoker() const override
    {
        return Underlying_->GetInvoker();
    }

    virtual TFuture<void> Suspend() override
    {
        return Underlying_->Suspend();
    }

    virtual void Resume() override
    {
        Underlying_->Resume();
    }

    virtual int GetPendingJobCount() const override
    {
        return Underlying_->GetPendingJobCount();
    }

    virtual int GetTotalJobCount() const override
    {
        return Underlying_->GetTotalJobCount();
    }

    virtual bool IsRunning() const override
    {
        return Underlying_->IsRunning();
    }

    virtual bool IsRevivedFromSnapshot() const override
    {
        return Underlying_->IsRevivedFromSnapshot();
    }

    virtual TJobResources GetNeededResources() const override
    {
        return Underlying_->GetNeededResources();
    }

    virtual std::vector<NScheduler::TJobResourcesWithQuota> GetMinNeededJobResources() const
    {
        return Underlying_->GetMinNeededJobResources();
    }

    virtual void OnJobStarted(const TJobId& jobId, TInstant startTime) override
    {
        Underlying_->OnJobStarted(jobId, startTime);
    }

    virtual void OnJobCompleted(std::unique_ptr<TCompletedJobSummary> jobSummary) override
    {
        Underlying_->OnJobCompleted(std::move(jobSummary));
    }

    virtual void OnJobFailed(std::unique_ptr<TFailedJobSummary> jobSummary) override
    {
        Underlying_->OnJobFailed(std::move(jobSummary));
    }

    virtual void OnJobAborted(std::unique_ptr<TAbortedJobSummary> jobSummary) override
    {
        Underlying_->OnJobAborted(std::move(jobSummary));
    }

    virtual void OnJobRunning(std::unique_ptr<TRunningJobSummary> jobSummary) override
    {
        Underlying_->OnJobRunning(std::move(jobSummary));
    }

    virtual TScheduleJobResultPtr ScheduleJob(
        ISchedulingContextPtr context,
        const TJobResourcesWithQuota& jobLimits,
        const TString& treeId) override
    {
        return Underlying_->ScheduleJob(std::move(context), jobLimits, treeId);
    }

    virtual void UpdateConfig(TSchedulerConfigPtr config) override
    {
        Underlying_->UpdateConfig(std::move(config));
    }

    virtual bool ShouldUpdateProgress() const override
    {
        return Underlying_->ShouldUpdateProgress();
    }

    virtual void SetProgressUpdated() override
    {
        Underlying_->SetProgressUpdated();
    }

    virtual bool HasProgress() const override
    {
        return Underlying_->HasProgress();
    }

    virtual TString GetLoggingProgress() const override
    {
        return Underlying_->GetLoggingProgress();
    }

    virtual TYsonString GetProgress() const override
    {
        return Underlying_->GetProgress();
    }

    virtual TYsonString GetBriefProgress() const override
    {
        return Underlying_->GetBriefProgress();
    }

    virtual TYsonString BuildJobYson(const TJobId& jobId, bool outputStatistics) const override
    {
        return Underlying_->BuildJobYson(jobId, outputStatistics);
    }

    virtual TSharedRef ExtractJobSpec(const TJobId& jobId) const override
    {
        return Underlying_->ExtractJobSpec(jobId);
    }

    virtual TOperationJobMetrics ExtractJobMetricsDelta() override
    {
        return Underlying_->ExtractJobMetricsDelta();
    }

    virtual TOperationAlertsMap GetAlerts() override
    {
        return Underlying_->GetAlerts();
    }

    virtual void BuildOperationInfo(NScheduler::NProto::TRspGetOperationInfo* response) override
    {
        Underlying_->BuildOperationInfo(response);
    }

    virtual TYsonString GetSuspiciousJobsYson() const override
    {
        return Underlying_->GetSuspiciousJobsYson();
    }

    virtual bool IsCompleteFinished() const override
    {
        return Underlying_->IsCompleteFinished();
    }

    virtual TError GetFailureError() const override
    {
        return Underlying_->GetFailureError();
    }

    virtual TError GetAbortError() const override
    {
        return Underlying_->GetAbortError();
    }

    virtual TError GetSuspensionError() const override
    {
        return Underlying_->GetSuspensionError();
    }

    virtual void ResetSuspensionError() override
    {
        Underlying_->ResetSuspensionError();
    }

    virtual int OnSnapshotStarted() override
    {
        return Underlying_->OnSnapshotStarted();
    }

    virtual void OnSnapshotCompleted(int snapshotIndex) override
    {
        return Underlying_->OnSnapshotCompleted(snapshotIndex);
    }

    virtual void OnBeforeDisposal() override
    {
        return Underlying_->OnBeforeDisposal();
    }

    virtual void SetPoolTreeSchedulingTagFilters(const std::vector<NScheduler::TSchedulingTagFilter>& filters) override
    {
        Underlying_->SetPoolTreeSchedulingTagFilters(filters);
    }

    virtual std::vector<NScheduler::TJobPtr> BuildJobsFromJoblets() const override
    {
        return Underlying_->BuildJobsFromJoblets();
    }

private:
    const TOperationId Id_;
    const IOperationControllerPtr Underlying_;
    const IInvokerPtr DtorInvoker_;
};

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateControllerForOperation(
    TControllerAgentPtr controllerAgent,
    TOperation* operation)
{
    IOperationControllerPtr controller;
    switch (operation->GetType()) {
        case EOperationType::Map: {
            auto baseSpec = ParseOperationSpec<TMapOperationSpec>(operation->GetSpec());
            controller = baseSpec->Ordered
                ? CreateOrderedMapController(controllerAgent, operation)
                : CreateUnorderedMapController(controllerAgent, operation);
            break;
        }
        case EOperationType::Merge: {
            auto baseSpec = ParseOperationSpec<TMergeOperationSpec>(operation->GetSpec());
            switch (baseSpec->Mode) {
                case EMergeMode::Ordered: {
                    controller = CreateOrderedMergeController(controllerAgent, operation);
                    break;
                }
                case EMergeMode::Sorted: {
                    controller = CreateSortedMergeController(controllerAgent, operation);
                    break;
                }
                case EMergeMode::Unordered: {
                    controller = CreateUnorderedMergeController(controllerAgent, operation);
                    break;
                }
            }
            break;
        }
        case EOperationType::Erase: {
            controller = CreateEraseController(controllerAgent, operation);
            break;
        }
        case EOperationType::Sort: {
            controller = CreateSortController(controllerAgent, operation);
            break;
        }
        case EOperationType::Reduce: {
            controller = CreateSortedReduceController(controllerAgent, operation);
            break;
        }
        case EOperationType::JoinReduce: {
            controller = CreateJoinReduceController(controllerAgent, operation);
            break;
        }
        case EOperationType::MapReduce: {
            controller = CreateMapReduceController(controllerAgent, operation);
            break;
        }
        case EOperationType::RemoteCopy: {
            controller = CreateRemoteCopyController(controllerAgent, operation);
            break;
        }
        default:
            Y_UNREACHABLE();
    }

    return New<TOperationControllerWrapper>(
        operation->GetId(),
        controller,
        controller->GetInvoker());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT

