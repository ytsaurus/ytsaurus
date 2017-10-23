#include "operation_controller.h"

#include "helpers.h"
#include "legacy_merge_controller.h"
#include "map_controller.h"
#include "ordered_controller.h"
#include "remote_copy_controller.h"
#include "sort_controller.h"
#include "sorted_controller.h"

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

    virtual TOperationControllerInitializeResult GetInitializeResult() const override
    {
        return Underlying_->GetInitializeResult();
    }

    virtual void InitializeReviving(TControllerTransactionsPtr controllerTransactions) override
    {
        Underlying_->InitializeReviving(controllerTransactions);
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

    virtual bool IsForgotten() const override
    {
        return Underlying_->IsForgotten();
    }

    virtual bool IsRevivedFromSnapshot() const override
    {
        return Underlying_->IsRevivedFromSnapshot();
    }

    virtual TJobResources GetNeededResources() const override
    {
        return Underlying_->GetNeededResources();
    }

    virtual std::vector<TJobResources> GetMinNeededJobResources() const
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
        const TJobResources& jobLimits) override
    {
        return Underlying_->ScheduleJob(std::move(context), jobLimits);
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

    virtual bool HasJobSplitterInfo() const override
    {
        return Underlying_->HasJobSplitterInfo();
    }

    virtual void BuildSpec(NYson::IYsonConsumer* consumer) const override
    {
        Underlying_->BuildSpec(consumer);
    }

    virtual void BuildOperationAttributes(NYson::IYsonConsumer* consumer) const override
    {
        Underlying_->BuildOperationAttributes(consumer);
    }

    virtual void BuildProgress(IYsonConsumer* consumer) const override
    {
        Underlying_->BuildProgress(consumer);
    }

    virtual void BuildBriefProgress(IYsonConsumer* consumer) const override
    {
        Underlying_->BuildBriefProgress(consumer);
    }

    virtual TString GetLoggingProgress() const override
    {
        return Underlying_->GetLoggingProgress();
    }

    virtual void BuildMemoryDigestStatistics(IYsonConsumer* consumer) const override
    {
        Underlying_->BuildMemoryDigestStatistics(consumer);
    }

    virtual void BuildJobSplitterInfo(IYsonConsumer* consumer) const override
    {
        Underlying_->BuildJobSplitterInfo(consumer);
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

    virtual TYsonString BuildJobsYson() const override
    {
        return Underlying_->BuildJobsYson();
    }

    virtual TSharedRef ExtractJobSpec(const TJobId& jobId) const override
    {
        return Underlying_->ExtractJobSpec(jobId);
    }

    virtual TYsonString BuildSuspiciousJobsYson() const override
    {
        return Underlying_->BuildSuspiciousJobsYson();
    }

    virtual int GetRecentlyCompletedJobCount() const override
    {
        return Underlying_->GetRecentlyCompletedJobCount();
    }

    virtual TFuture<void> ReleaseJobs(int jobCount) override
    {
        return Underlying_->ReleaseJobs(jobCount);
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
    IOperationHost* host,
    TOperation* operation)
{
    IOperationControllerPtr controller;
    switch (operation->GetType()) {
        case EOperationType::Map: {
            auto baseSpec = ParseOperationSpec<TMapOperationSpec>(operation->GetSpec());
            if (baseSpec->Ordered) {
                auto legacySpec = ParseOperationSpec<TOperationWithLegacyControllerSpec>(operation->GetSpec());
                controller = legacySpec->UseLegacyController
                    ? CreateLegacyOrderedMapController(host, operation)
                    : CreateOrderedMapController(host, operation);
            } else {
                controller = CreateMapController(host, operation);
            }
            break;
        }
        case EOperationType::Merge: {
            auto baseSpec = ParseOperationSpec<TMergeOperationSpec>(operation->GetSpec());
            switch (baseSpec->Mode) {
                case EMergeMode::Ordered: {
                    auto legacySpec = ParseOperationSpec<TOperationWithLegacyControllerSpec>(operation->GetSpec());
                    controller = legacySpec->UseLegacyController
                        ? CreateLegacyOrderedMergeController(host, operation)
                        : CreateOrderedMergeController(host, operation);
                    break;
                }
                case EMergeMode::Sorted: {
                    auto legacySpec = ParseOperationSpec<TOperationWithLegacyControllerSpec>(operation->GetSpec());
                    controller = legacySpec->UseLegacyController
                        ? CreateLegacySortedMergeController(host, operation)
                        : CreateSortedMergeController(host, operation);
                    break;
                }
                case EMergeMode::Unordered:
                    controller = CreateUnorderedMergeController(host, operation);
            }
            break;
        }
        case EOperationType::Erase: {
            auto legacySpec = ParseOperationSpec<TOperationWithLegacyControllerSpec>(operation->GetSpec());
            controller = legacySpec->UseLegacyController
                ? CreateLegacyEraseController(host, operation)
                : CreateEraseController(host, operation);
            break;
        }
        case EOperationType::Sort:
            controller = CreateSortController(host, operation);
            break;
        case EOperationType::Reduce: {
            auto legacySpec = ParseOperationSpec<TOperationWithLegacyControllerSpec>(operation->GetSpec());
            controller = legacySpec->UseLegacyController
                ? CreateLegacyReduceController(host, operation)
                : CreateSortedReduceController(host, operation);
            break;
        }
        case EOperationType::JoinReduce: {
            auto legacySpec = ParseOperationSpec<TOperationWithLegacyControllerSpec>(operation->GetSpec());
            controller = legacySpec->UseLegacyController
                ? CreateLegacyJoinReduceController(host, operation)
                : CreateJoinReduceController(host, operation);
            break;
        }
        case EOperationType::MapReduce:
            controller = CreateMapReduceController(host, operation);
            break;
        case EOperationType::RemoteCopy: {
            auto legacySpec = ParseOperationSpec<TOperationWithLegacyControllerSpec>(operation->GetSpec());
            controller = legacySpec->UseLegacyController
                ? CreateLegacyRemoteCopyController(host, operation)
                : CreateRemoteCopyController(host, operation);
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

