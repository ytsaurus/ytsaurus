#include "operation_controller.h"
#include "helpers.h"
#include "operation.h"
#include "ordered_controller.h"
#include "sort_controller.h"
#include "sorted_controller.h"
#include "unordered_controller.h"
#include "operation_controller_host.h"
#include "vanilla_controller.h"
#include "memory_tag_queue.h"

#include <yt/client/api/transaction.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/scheduler/config.h>
#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/yson/consumer.h>
#include <yt/core/yson/string.h>

namespace NYT::NControllerAgent {

using namespace NApi;
using namespace NScheduler;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NYson;
using namespace NYTree;
using namespace NYTAlloc;

using NScheduler::NProto::TSchedulerJobResultExt;
using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TControllerTransactionIds* transactionIdsProto, const NControllerAgent::TControllerTransactionIds& transactionIds)
{
    ToProto(transactionIdsProto->mutable_async_id(), transactionIds.AsyncId);
    ToProto(transactionIdsProto->mutable_input_id(), transactionIds.InputId);
    ToProto(transactionIdsProto->mutable_output_id(), transactionIds.OutputId);
    ToProto(transactionIdsProto->mutable_debug_id(), transactionIds.DebugId);
    ToProto(transactionIdsProto->mutable_output_completion_id(), transactionIds.OutputCompletionId);
    ToProto(transactionIdsProto->mutable_debug_completion_id(), transactionIds.DebugCompletionId);
    ToProto(transactionIdsProto->mutable_nested_input_ids(), transactionIds.NestedInputIds);
}

void FromProto(NControllerAgent::TControllerTransactionIds* transactionIds, const NProto::TControllerTransactionIds& transactionIdsProto)
{
    transactionIds->AsyncId = FromProto<TTransactionId>(transactionIdsProto.async_id());
    transactionIds->InputId = FromProto<TTransactionId>(transactionIdsProto.input_id());
    transactionIds->OutputId = FromProto<TTransactionId>(transactionIdsProto.output_id());
    transactionIds->DebugId  = FromProto<TTransactionId>(transactionIdsProto.debug_id());
    transactionIds->OutputCompletionId = FromProto<TTransactionId>(transactionIdsProto.output_completion_id());
    transactionIds->DebugCompletionId = FromProto<TTransactionId>(transactionIdsProto.debug_completion_id());
    transactionIds->NestedInputIds = FromProto<std::vector<TTransactionId>>(transactionIdsProto.nested_input_ids());
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TInitializeOperationResult* resultProto, const TOperationControllerInitializeResult& result)
{
    resultProto->set_mutable_attributes(result.Attributes.Mutable.GetData());
    resultProto->set_brief_spec(result.Attributes.BriefSpec.GetData());
    resultProto->set_full_spec(result.Attributes.FullSpec.GetData());
    resultProto->set_unrecognized_spec(result.Attributes.UnrecognizedSpec.GetData());
    ToProto(resultProto->mutable_transaction_ids(), result.TransactionIds);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TPrepareOperationResult* resultProto, const TOperationControllerPrepareResult& result)
{
    if (result.Attributes) {
        resultProto->set_attributes(result.Attributes.GetData());
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TMaterializeOperationResult* resultProto, const TOperationControllerMaterializeResult& result)
{
    resultProto->set_suspend(result.Suspend);
    ToProto(resultProto->mutable_initial_needed_resources(), result.InitialNeededResources);
}


////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TReviveOperationResult* resultProto, const TOperationControllerReviveResult& result)
{
    resultProto->set_attributes(result.Attributes.GetData());
    resultProto->set_revived_from_snapshot(result.RevivedFromSnapshot);
    for (const auto& job : result.RevivedJobs) {
        auto* jobProto = resultProto->add_revived_jobs();
        ToProto(jobProto->mutable_job_id(), job.JobId);
        jobProto->set_job_type(static_cast<int>(job.JobType));
        jobProto->set_start_time(ToProto<ui64>(job.StartTime));
        ToProto(jobProto->mutable_resource_limits(), job.ResourceLimits);
        jobProto->set_interruptible(job.Interruptible);
        jobProto->set_tree_id(job.TreeId);
        jobProto->set_node_id(job.NodeId);
        jobProto->set_node_address(job.NodeAddress);
    }
    ToProto(resultProto->mutable_revived_banned_tree_ids(), result.RevivedBannedTreeIds);
    ToProto(resultProto->mutable_needed_resources(), result.NeededResources);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TCommitOperationResult* /* resultProto */, const TOperationControllerCommitResult& /* result */)
{ }

////////////////////////////////////////////////////////////////////////////////

//! Ensures that operation controllers are being destroyed in a
//! dedicated invoker and releases memory tag when controller is destroyed.
class TOperationControllerWrapper
    : public IOperationController
{
public:
    TOperationControllerWrapper(
        TOperationId id,
        IOperationControllerPtr underlying,
        IInvokerPtr dtorInvoker,
        TMemoryTag memoryTag,
        TMemoryTagQueue* memoryTagQueue)
        : Id_(id)
        , Underlying_(std::move(underlying))
        , DtorInvoker_(std::move(dtorInvoker))
        , MemoryTag_(memoryTag)
        , MemoryTagQueue_(memoryTagQueue)
    { }

    virtual ~TOperationControllerWrapper()
    {
        auto Logger = NLogging::TLogger(ControllerLogger)
            .AddTag("OperationId: %v", Id_);
        YT_LOG_INFO("Controller wrapper destructed, controller destruction scheduled (MemoryUsage: %v)",
            GetMemoryUsageForTag(MemoryTag_));
        DtorInvoker_->Invoke(BIND([
                underlying = std::move(Underlying_),
                memoryTagQueue = MemoryTagQueue_,
                memoryTag = MemoryTag_,
                Logger] () mutable
        {
            NProfiling::TWallTimer timer;
            auto memoryUsageBefore = GetMemoryUsageForTag(memoryTag);
            YT_LOG_INFO("Started destructing operation controller (MemoryUsageBefore: %v)", memoryUsageBefore);
            if (auto refCount = ResetAndGetResidualRefCount(underlying)) {
                YT_LOG_WARNING(
                    "Controller is going to be removed, but it has residual reference count; memory leak is possible "
                    "(RefCount: %v)",
                    refCount);
            }
            auto memoryUsageAfter = GetMemoryUsageForTag(memoryTag);
            YT_LOG_INFO("Finished destructing operation controller (Elapsed: %v, MemoryUsageAfter: %v, MemoryUsageDecrease: %v)",
                timer.GetElapsedTime(),
                memoryUsageAfter,
                memoryUsageBefore - memoryUsageAfter);
            if (memoryTagQueue) {
                memoryTagQueue->ReclaimTag(memoryTag);
            }
        }));
    }

    virtual TOperationControllerInitializeResult InitializeClean() override
    {
        return Underlying_->InitializeClean();
    }

    virtual TOperationControllerInitializeResult InitializeReviving(const TControllerTransactionIds& transactions) override
    {
        return Underlying_->InitializeReviving(transactions);
    }

    virtual TOperationControllerPrepareResult Prepare() override
    {
        return Underlying_->Prepare();
    }

    virtual TOperationControllerMaterializeResult Materialize() override
    {
        return Underlying_->Materialize();
    }

    virtual void Commit() override
    {
        Underlying_->Commit();
    }

    virtual void SaveSnapshot(IOutputStream* stream) override
    {
        Underlying_->SaveSnapshot(stream);
    }

    virtual TOperationControllerReviveResult Revive() override
    {
        return Underlying_->Revive();
    }

    virtual void Terminate(EControllerState finalState) override
    {
        Underlying_->Terminate(finalState);
    }

    virtual void Cancel() override
    {
        Underlying_->Cancel();
    }

    virtual void Complete() override
    {
        Underlying_->Complete();
    }

    virtual void Dispose() override
    {
        Underlying_->Dispose();
    }

    virtual void UpdateRuntimeParameters(const TOperationRuntimeParametersUpdatePtr& update) override
    {
        Underlying_->UpdateRuntimeParameters(update);
    }

    virtual void OnTransactionsAborted(const std::vector<TTransactionId>& transactionIds) override
    {
        Underlying_->OnTransactionsAborted(transactionIds);
    }

    virtual TCancelableContextPtr GetCancelableContext() const override
    {
        return Underlying_->GetCancelableContext();
    }

    virtual IInvokerPtr GetInvoker(EOperationControllerQueue queue) const override
    {
        return Underlying_->GetInvoker(queue);
    }

    virtual IInvokerPtr GetCancelableInvoker(EOperationControllerQueue queue) const override
    {
        return Underlying_->GetCancelableInvoker(queue);
    }

    virtual IDiagnosableInvokerPool::TInvokerStatistics GetInvokerStatistics(EOperationControllerQueue queue) const override
    {
        return Underlying_->GetInvokerStatistics(queue);
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

    virtual TJobResourcesWithQuotaList GetMinNeededJobResources() const override
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

    virtual void OnJobAborted(std::unique_ptr<TAbortedJobSummary> jobSummary, bool byScheduler) override
    {
        Underlying_->OnJobAborted(std::move(jobSummary), byScheduler);
    }

    virtual void OnJobRunning(std::unique_ptr<TRunningJobSummary> jobSummary) override
    {
        Underlying_->OnJobRunning(std::move(jobSummary));
    }

    virtual TControllerScheduleJobResultPtr ScheduleJob(
        ISchedulingContext* context,
        const TJobResourcesWithQuota& jobLimits,
        const TString& treeId) override
    {
        return Underlying_->ScheduleJob(context, jobLimits, treeId);
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

    virtual TYsonString BuildJobYson(TJobId jobId, bool outputStatistics) const override
    {
        return Underlying_->BuildJobYson(jobId, outputStatistics);
    }

    virtual TSharedRef ExtractJobSpec(TJobId jobId) const override
    {
        return Underlying_->ExtractJobSpec(jobId);
    }

    virtual TOperationJobMetrics PullJobMetricsDelta(bool force) override
    {
        return Underlying_->PullJobMetricsDelta(force);
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

    virtual IYPathServicePtr GetOrchid() const override
    {
        return Underlying_->GetOrchid();
    }

    virtual TString WriteCoreDump() const override
    {
        return Underlying_->WriteCoreDump();
    }

    virtual void RegisterOutputRows(i64 count, int tableIndex) override
    {
        return Underlying_->RegisterOutputRows(count, tableIndex);
    }

    virtual std::optional<int> GetRowCountLimitTableIndex() override
    {
        return Underlying_->GetRowCountLimitTableIndex();
    }

    virtual void LoadSnapshot(const TOperationSnapshot& snapshot) override
    {
        return Underlying_->LoadSnapshot(snapshot);
    }

private:
    const TOperationId Id_;
    const IOperationControllerPtr Underlying_;
    const IInvokerPtr DtorInvoker_;
    const TMemoryTag MemoryTag_;
    TMemoryTagQueue* const MemoryTagQueue_;
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
            controller = CreateAppropriateReduceController(config, host, operation, /* isJoinReduce */ false);
            break;
        }
        case EOperationType::JoinReduce: {
            controller = CreateAppropriateReduceController(config, host, operation, /* isJoinReduce */ true);
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
            YT_ABORT();
    }

    return New<TOperationControllerWrapper>(
        operation->GetId(),
        controller,
        controller->GetInvoker(),
        operation->GetMemoryTag(),
        host->GetMemoryTagQueue());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

