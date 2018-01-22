#include "operation_controller.h"
#include "helpers.h"
#include "operation.h"
#include "ordered_controller.h"
#include "sort_controller.h"
#include "sorted_controller.h"
#include "unordered_controller.h"
#include "operation_controller_host.h"
#include "vanilla_controller.h"

#include <yt/server/scheduler/operation.h>

#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/scheduler/config.h>
#include <yt/ytlib/scheduler/proto/job.pb.h>

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

using NScheduler::NProto::TSchedulerJobResultExt;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TStartedJobSummary::TStartedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event)
    : Id(FromProto<TJobId>(event->status().job_id()))
    , StartTime(FromProto<TInstant>(event->start_time()))
{
    YCHECK(event->has_start_time());
}

////////////////////////////////////////////////////////////////////////////////

TJobSummary::TJobSummary(const TJobId& id, EJobState state)
    : Result()
    , Id(id)
    , State(state)
    , LogAndProfile(false)
{ }

TJobSummary::TJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event)
    : Id(FromProto<TJobId>(event->status().job_id()))
    , State(static_cast<EJobState>(event->status().state()))
    , FinishTime(event->has_finish_time() ? MakeNullable(FromProto<TInstant>(event->finish_time())) : Null)
    , LogAndProfile(event->log_and_profile())
{
    auto* status = event->mutable_status();
    Result.Swap(status->mutable_result());
    if (status->has_prepare_duration()) {
        PrepareDuration = FromProto<TDuration>(status->prepare_duration());
    }
    if (status->has_download_duration()) {
        DownloadDuration = FromProto<TDuration>(status->download_duration());
    }
    if (status->has_exec_duration()) {
        ExecDuration = FromProto<TDuration>(status->exec_duration());
    }
    if (status->has_statistics()) {
        StatisticsYson = TYsonString(status->statistics());
    }
}

void TJobSummary::Persist(const NPhoenix::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Result);
    Persist(context, Id);
    Persist(context, State);
    Persist(context, FinishTime);
    Persist(context, PrepareDuration);
    Persist(context, DownloadDuration);
    Persist(context, ExecDuration);
    Persist(context, Statistics);
    Persist(context, StatisticsYson);
    Persist(context, LogAndProfile);
}

////////////////////////////////////////////////////////////////////////////////

TCompletedJobSummary::TCompletedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event)
    : TJobSummary(event)
    , Abandoned(event->abandoned())
    , InterruptReason(static_cast<EInterruptReason>(event->interrupt_reason()))
{
    YCHECK(event->has_abandoned());
    YCHECK(event->has_interrupt_reason());
    const auto& schedulerResultExt = Result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
    YCHECK(
        (InterruptReason == EInterruptReason::None && schedulerResultExt.unread_chunk_specs_size() == 0) ||
        (InterruptReason != EInterruptReason::None && schedulerResultExt.unread_chunk_specs_size() != 0));
}

void TCompletedJobSummary::Persist(const NPhoenix::TPersistenceContext& context)
{
    TJobSummary::Persist(context);

    using NYT::Persist;

    Persist(context, Abandoned);
    Persist(context, InterruptReason);
    // TODO(max42): now we persist only those completed job summaries that correspond
    // to non-interrupted jobs, because Persist(context, UnreadInputDataSlices) produces
    // lots of ugly template resolution errors. I wasn't able to fix it :(
    YCHECK(InterruptReason == EInterruptReason::None);
    Persist(context, SplitJobCount);
}

////////////////////////////////////////////////////////////////////////////////

TAbortedJobSummary::TAbortedJobSummary(const TJobId& id, EAbortReason abortReason)
    : TJobSummary(id, EJobState::Aborted)
    , AbortReason(abortReason)
{ }

TAbortedJobSummary::TAbortedJobSummary(const TJobSummary& other, EAbortReason abortReason)
    : TJobSummary(other)
    , AbortReason(abortReason)
{ }

TAbortedJobSummary::TAbortedJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event)
    : TJobSummary(event)
    , AbortReason(static_cast<EAbortReason>(event->abort_reason()))
{ }

////////////////////////////////////////////////////////////////////////////////

TRunningJobSummary::TRunningJobSummary(NScheduler::NProto::TSchedulerToAgentJobEvent* event)
    : TJobSummary(event)
    , Progress(event->status().progress())
    , StderrSize(event->status().stderr_size())
{ }

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

    virtual void Cancel() override
    {
        Underlying_->Cancel();
    }

    virtual TOperationControllerInitializationResult GetInitializationResult() override
    {
        return Underlying_->GetInitializationResult();
    }

    virtual TOperationControllerReviveResult GetReviveResult() override
    {
        return Underlying_->GetReviveResult();
    }

    virtual NYson::TYsonString GetAttributes() const override
    {
        return Underlying_->GetAttributes();
    }

    virtual void OnTransactionAborted(const TTransactionId& transactionId) override
    {
        Underlying_->OnTransactionAborted(transactionId);
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

    virtual bool IsRunning() const override
    {
        return Underlying_->IsRunning();
    }

    virtual TJobResources GetNeededResources() const override
    {
        return Underlying_->GetNeededResources();
    }

    virtual void UpdateMinNeededJobResources() override
    {
        Underlying_->UpdateMinNeededJobResources();
    }

    virtual std::vector<NScheduler::TJobResourcesWithQuota> GetMinNeededJobResources() const override
    {
        return Underlying_->GetMinNeededJobResources();
    }

    virtual void OnJobStarted(std::unique_ptr<TStartedJobSummary> jobSummary) override
    {
        Underlying_->OnJobStarted(std::move(jobSummary));
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

    virtual void UpdateConfig(const TControllerAgentConfigPtr& config) override
    {
        Underlying_->UpdateConfig(config);
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

    virtual TOperationJobMetrics PullJobMetricsDelta() override
    {
        return Underlying_->PullJobMetricsDelta();
    }

    virtual TOperationAlertMap GetAlerts() override
    {
        return Underlying_->GetAlerts();
    }

    virtual TOperationInfo BuildOperationInfo() override
    {
        return Underlying_->BuildOperationInfo();
    }

    virtual TYsonString GetSuspiciousJobsYson() const override
    {
        return Underlying_->GetSuspiciousJobsYson();
    }

    virtual TSnapshotCookie OnSnapshotStarted() override
    {
        return Underlying_->OnSnapshotStarted();
    }

    virtual void OnSnapshotCompleted(const TSnapshotCookie& cookie) override
    {
        return Underlying_->OnSnapshotCompleted(cookie);
    }

    virtual void OnBeforeDisposal() override
    {
        return Underlying_->OnBeforeDisposal();
    }

    // TODO(babenko)
    virtual void OnNonscheduledJobAborted(
        const TJobId& jobid,
        EAbortReason abortReason) override
    {
        Y_UNREACHABLE();
    }

private:
    const TOperationId Id_;
    const IOperationControllerPtr Underlying_;
    const IInvokerPtr DtorInvoker_;
};

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateControllerForOperation(
    TControllerAgentConfigPtr config,
    TOperation* operation)
{
    IOperationControllerPtr controller;
    auto host = operation->GetHost();
    switch (operation->GetType()) {
        case EOperationType::Map: {
            auto baseSpec = ParseOperationSpec<TMapOperationSpec>(operation->GetSpec());
            controller = baseSpec->Ordered
                ? CreateOrderedMapController(config, host, operation)
                : CreateUnorderedMapController(config, host, operation);
            break;
        }
        case EOperationType::Merge: {
            auto baseSpec = ParseOperationSpec<TMergeOperationSpec>(operation->GetSpec());
            switch (baseSpec->Mode) {
                case EMergeMode::Ordered: {
                    controller = CreateOrderedMergeController(config, host, operation);
                    break;
                }
                case EMergeMode::Sorted: {
                    controller = CreateSortedMergeController(config, host, operation);
                    break;
                }
                case EMergeMode::Unordered: {
                    controller = CreateUnorderedMergeController(config, host, operation);
                    break;
                }
            }
            break;
        }
        case EOperationType::Erase: {
            controller = CreateEraseController(config, host, operation);
            break;
        }
        case EOperationType::Sort: {
            controller = CreateSortController(config, host, operation);
            break;
        }
        case EOperationType::Reduce: {
            controller = CreateSortedReduceController(config, host, operation);
            break;
        }
        case EOperationType::JoinReduce: {
            controller = CreateJoinReduceController(config, host, operation);
            break;
        }
        case EOperationType::MapReduce: {
            controller = CreateMapReduceController(config, host, operation);
            break;
        }
        case EOperationType::RemoteCopy: {
            controller = CreateRemoteCopyController(config, host, operation);
            break;
        }
        case EOperationType::Vanilla: {
            controller = CreateVanillaController(config, host, operation);
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

