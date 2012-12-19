#include "stdafx.h"
#include "operation_controller_detail.h"
#include "private.h"
#include "chunk_list_pool.h"
#include "chunk_pool.h"
#include "job_resources.h"

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/chunk_client/chunk_list_ypath_proxy.h>
#include <ytlib/table_client/key.h>

#include <ytlib/object_client/object_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/convert.h>

#include <ytlib/formats/format.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>
#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/scheduler/config.h>

#include <ytlib/table_client/helpers.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NFileClient;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NFormats;
using namespace NJobProxy;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////

TOperationControllerBase::TTask::TTask(TOperationControllerBase* controller)
    : Controller(controller)
    , CachedPendingJobCount(0)
    , CachedTotalNeededResources(ZeroNodeResources())
    , Logger(Controller->Logger)
{ }

int TOperationControllerBase::TTask::GetPendingJobCount() const
{
    return GetChunkPoolOutput()->GetPendingJobCount();
}

int TOperationControllerBase::TTask::GetPendingJobCountDelta()
{
    int oldValue = CachedPendingJobCount;
    int newValue = GetPendingJobCount();
    CachedPendingJobCount = newValue;
    return newValue - oldValue;
}

TNodeResources TOperationControllerBase::TTask::GetTotalNeededResourcesDelta()
{
    auto oldValue = CachedTotalNeededResources;
    auto newValue = GetTotalNeededResources();
    CachedTotalNeededResources = newValue;
    newValue -= oldValue;
    return newValue;
}

TNodeResources TOperationControllerBase::TTask::GetTotalNeededResources() const
{
    i64 count = GetPendingJobCount();
    // NB: Don't call GetAvgNeededResources if there are no pending jobs.
    return count == 0 ? ZeroNodeResources() : GetAvgNeededResources() * count;
}

i64 TOperationControllerBase::TTask::GetLocality(const Stroka& address) const
{
    return GetChunkPoolOutput()->GetLocality(address);
}

bool TOperationControllerBase::TTask::IsStrictlyLocal() const
{
    return false;
}

int TOperationControllerBase::TTask::GetPriority() const
{
    return 0;
}

void TOperationControllerBase::TTask::AddInput(TChunkStripePtr stripe)
{
    GetChunkPoolInput()->Add(stripe);
    AddInputLocalityHint(stripe);
    AddPendingHint();
}

void TOperationControllerBase::TTask::AddInput(const std::vector<TChunkStripePtr>& stripes)
{
    FOREACH (auto stripe, stripes) {
        AddInput(stripe);
    }
}

void TOperationControllerBase::TTask::FinishInput()
{
    LOG_DEBUG("Task input finished (Task: %s)", ~GetId());

    GetChunkPoolInput()->Finish();
    AddPendingHint();
}

TJobPtr TOperationControllerBase::TTask::ScheduleJob(ISchedulingContext* context)
{
    int chunkListCount = GetChunkListCountPerJob();
    if (!Controller->HasEnoughChunkLists(chunkListCount)) {
        return NULL;
    }

    int jobIndex = Controller->JobIndexGenerator.Next();
    auto joblet = New<TJoblet>(this, jobIndex);

    auto address = context->GetNode()->GetAddress();
    auto* chunkPoolOutput = GetChunkPoolOutput();
    joblet->OutputCookie = chunkPoolOutput->Extract(address);
    if (joblet->OutputCookie == IChunkPoolOutput::NullCookie) {
        return NULL;
    }

    joblet->InputStripeList = chunkPoolOutput->GetStripeList(joblet->OutputCookie);

    // Compute the actual usage for this joblet and check it
    // against the the limits. This is the last chance to give up.
    auto resourceLimits = GetNeededResources(joblet);
    auto node = context->GetNode();
    if (!node->HasEnoughResources(resourceLimits)) {
        chunkPoolOutput->Failed(joblet->OutputCookie);
        return NULL;
    }

    auto jobType = GetJobType();

    // Async part.
    auto this_ = MakeStrong(this);
    auto jobSpecBuilder = BIND([=] (TJobSpec* jobSpec) -> TVoid {
        this_->BuildJobSpec(joblet, jobSpec);
        this_->Controller->CustomizeJobSpec(joblet, jobSpec);
        return TVoid();
    });

    auto job = context->StartJob(
        Controller->Operation,
        jobType,
        resourceLimits,
        jobSpecBuilder);
    joblet->Job = job;

    LOG_DEBUG("Job scheduled (JobId: %s, OperationId: %s, JobType: %s, Address: %s, Task: %s, JobIndex: %d, ChunkCount: %d (%d local), DataSize: %" PRId64 ", RowCount: %" PRId64 ", ResourceLimilts: {%s})",
        ~ToString(job->GetId()),
        ~ToString(Controller->Operation->GetOperationId()),
        ~jobType.ToString(),
        ~node->GetAddress(),
        ~GetId(),
        jobIndex,
        joblet->InputStripeList->TotalChunkCount,
        joblet->InputStripeList->LocalChunkCount,
        joblet->InputStripeList->TotalDataSize,
        joblet->InputStripeList->TotalRowCount,
        ~FormatResources(resourceLimits));

    // Prepare chunk lists.
    for (int index = 0; index < chunkListCount; ++index) {
        auto id = Controller->ExtractChunkList();
        joblet->ChunkListIds.push_back(id);
    }

    // Sync part.
    PrepareJoblet(joblet);
    Controller->CustomizeJoblet(joblet);

    Controller->RegisterJoblet(joblet);

    OnJobStarted(joblet);

    return joblet->Job;
}

bool TOperationControllerBase::TTask::IsPending() const
{
    return GetChunkPoolOutput()->GetPendingJobCount() > 0;
}

bool TOperationControllerBase::TTask::IsCompleted() const
{
    return GetChunkPoolOutput()->IsCompleted();
}

i64 TOperationControllerBase::TTask::GetTotalDataSize() const
{
    return GetChunkPoolOutput()->GetTotalDataSize();
}

i64 TOperationControllerBase::TTask::GetCompletedDataSize() const
{
    return GetChunkPoolOutput()->GetCompletedDataSize();
}

i64 TOperationControllerBase::TTask::GetPendingDataSize() const
{
    return GetChunkPoolOutput()->GetPendingDataSize();
}

void TOperationControllerBase::TTask::PrepareJoblet(TJobletPtr joblet)
{
    UNUSED(joblet);
}

void TOperationControllerBase::TTask::OnJobStarted(TJobletPtr joblet)
{
    UNUSED(joblet);
}

void TOperationControllerBase::TTask::OnJobCompleted(TJobletPtr joblet)
{
    GetChunkPoolOutput()->Completed(joblet->OutputCookie);
}

void TOperationControllerBase::TTask::ReleaseFailedJobResources(TJobletPtr joblet)
{
    auto* chunkPoolOutput = GetChunkPoolOutput();

    Controller->ReleaseChunkLists(joblet->ChunkListIds);

    auto list = chunkPoolOutput->GetStripeList(joblet->OutputCookie);
    FOREACH (const auto& stripe, list->Stripes) {
        AddInputLocalityHint(stripe);
    }

    chunkPoolOutput->Failed(joblet->OutputCookie);

    AddPendingHint();
}

void TOperationControllerBase::TTask::OnJobFailed(TJobletPtr joblet)
{
    ReleaseFailedJobResources(joblet);
}

void TOperationControllerBase::TTask::OnJobAborted(TJobletPtr joblet)
{
    ReleaseFailedJobResources(joblet);
}

void TOperationControllerBase::TTask::OnTaskCompleted()
{
    LOG_DEBUG("Task completed (Task: %s)", ~GetId());
}

void TOperationControllerBase::TTask::AddPendingHint()
{
    Controller->AddTaskPendingHint(this);
}

void TOperationControllerBase::TTask::AddInputLocalityHint(TChunkStripePtr stripe)
{
    Controller->AddTaskLocalityHint(this, stripe);
}

void TOperationControllerBase::TTask::AddSequentialInputSpec(
    NScheduler::NProto::TJobSpec* jobSpec,
    TJobletPtr joblet,
    bool enableTableIndex)
{
    auto* inputSpec = jobSpec->add_input_specs();
    auto list = joblet->InputStripeList;
    FOREACH (const auto& stripe, list->Stripes) {
        AddInputChunks(inputSpec, stripe, list->PartitionTag, enableTableIndex);
    }
    UpdateInputSpecTotals(jobSpec, joblet);
}

void TOperationControllerBase::TTask::AddParallelInputSpec(
    NScheduler::NProto::TJobSpec* jobSpec,
    TJobletPtr joblet,
    bool enableTableIndex)
{
    auto list = joblet->InputStripeList;
    FOREACH (const auto& stripe, list->Stripes) {
        auto* inputSpec = jobSpec->add_input_specs();
        AddInputChunks(inputSpec, stripe, list->PartitionTag, enableTableIndex);
    }
    UpdateInputSpecTotals(jobSpec, joblet);
}

void TOperationControllerBase::TTask::UpdateInputSpecTotals(
    NScheduler::NProto::TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    auto list = joblet->InputStripeList;
    jobSpec->set_input_uncompressed_data_size(
        jobSpec->input_uncompressed_data_size() +
        list->TotalDataSize);
    jobSpec->set_input_row_count(
        jobSpec->input_row_count() +
        list->TotalRowCount);
}

void TOperationControllerBase::TTask::AddFinalOutputSpecs(
    NScheduler::NProto::TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    YCHECK(joblet->ChunkListIds.size() == Controller->OutputTables.size());
    for (int index = 0; index < static_cast<int>(Controller->OutputTables.size()); ++index) {
        const auto& table = Controller->OutputTables[index];
        auto* outputSpec = jobSpec->add_output_specs();
        outputSpec->set_channels(table.Channels.Data());
        outputSpec->set_replication_factor(table.ReplicationFactor);
        if (table.KeyColumns) {
            ToProto(outputSpec->mutable_key_columns(), *table.KeyColumns);
        }
        *outputSpec->mutable_chunk_list_id() = joblet->ChunkListIds[index].ToProto();
    }
}

void TOperationControllerBase::TTask::AddIntermediateOutputSpec(
    NScheduler::NProto::TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    YCHECK(joblet->ChunkListIds.size() == 1);
    auto* outputSpec = jobSpec->add_output_specs();
    outputSpec->set_channels("[]");
    *outputSpec->mutable_chunk_list_id() = joblet->ChunkListIds[0].ToProto();
}

void TOperationControllerBase::TTask::AddInputChunks(
    NScheduler::NProto::TTableInputSpec* inputSpec,
    TChunkStripePtr stripe,
    TNullable<int> partitionTag,
    bool enableTableIndex)
{
    FOREACH (const auto& stripeChunk, stripe->Chunks) {
        auto* inputChunk = inputSpec->add_chunks();
        *inputChunk = *stripeChunk;
        if (!enableTableIndex) {
            inputChunk->clear_table_index();
        }
        if (partitionTag) {
            inputChunk->set_partition_tag(partitionTag.Get());
        }
    }
}

TNodeResources TOperationControllerBase::TTask::GetAvgNeededResources() const
{
    return GetMinNeededResources();
}

TNodeResources TOperationControllerBase::TTask::GetNeededResources(TJobletPtr joblet) const
{
    UNUSED(joblet);
    return GetMinNeededResources();
}

////////////////////////////////////////////////////////////////////

TOperationControllerBase::TOperationControllerBase(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
    : Config(config)
    , Host(host)
    , Operation(operation)
    , ObjectProxy(host->GetMasterChannel())
    , Logger(OperationLogger)
    , CancelableContext(New<TCancelableContext>())
    , CancelableControlInvoker(CancelableContext->CreateInvoker(Host->GetControlInvoker()))
    , CancelableBackgroundInvoker(CancelableContext->CreateInvoker(Host->GetBackgroundInvoker()))
    , Active(false)
    , Running(false)
    , TotalInputChunkCount(0)
    , TotalInputDataSize(0)
    , TotalInputRowCount(0)
    , TotalInputValueCount(0)
    , UsedResources(ZeroNodeResources())
    , PendingTaskInfos(MaxTaskPriority + 1)
    , CachedPendingJobCount(0)
    , CachedNeededResources(ZeroNodeResources())
{
    Logger.AddTag(Sprintf("OperationId: %s", ~operation->GetOperationId().ToString()));
}

void TOperationControllerBase::Initialize()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Initializing operation");

    FOREACH (const auto& path, GetInputTablePaths()) {
        TInputTable table;
        table.Path = path;
        InputTables.push_back(table);
    }

    FOREACH (const auto& path, GetOutputTablePaths()) {
        TOutputTable table;
        table.Path = path;
        if (path.Attributes().Get<bool>("overwrite", false)) {
            table.Clear = true;
            table.Overwrite = true;
            table.LockMode = ELockMode::Exclusive;
        }

        table.KeyColumns = path.Attributes().Find< std::vector<Stroka> >("sorted_by");
        if (table.KeyColumns) {
            if (!IsSortedOutputSupported()) {
                THROW_ERROR_EXCEPTION("Sorted outputs are not supported");
            } else {
                table.Clear = true;
                table.LockMode = ELockMode::Exclusive;
            }
        }

        OutputTables.push_back(table);
    }

    try {
        if (Host->GetExecNodes().empty()) {
            THROW_ERROR_EXCEPTION("No online exec nodes to start operation");
        }
        DoInitialize();
    } catch (const std::exception& ex) {
        LOG_INFO(ex, "Operation has failed to initialize");
        Active = false;
        throw;
    }

    Active = true;

    LOG_INFO("Operation initialized");
}

void TOperationControllerBase::DoInitialize()
{ }

TFuture<void> TOperationControllerBase::Prepare()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto this_ = MakeStrong(this);
    auto pipeline = StartAsyncPipeline(CancelableBackgroundInvoker)
        ->Add(BIND(&TThis::StartIOTransactions, MakeStrong(this)))
        ->Add(BIND(&TThis::OnIOTransactionsStarted, MakeStrong(this)), CancelableControlInvoker)
        ->Add(BIND(&TThis::GetObjectIds, MakeStrong(this)))
        ->Add(BIND(&TThis::OnObjectIdsReceived, MakeStrong(this)))
        ->Add(BIND(&TThis::GetInputTypes, MakeStrong(this)))
        ->Add(BIND(&TThis::OnInputTypesReceived, MakeStrong(this)))
        ->Add(BIND(&TThis::RequestInputs, MakeStrong(this)))
        ->Add(BIND(&TThis::OnInputsReceived, MakeStrong(this)))
        ->Add(BIND(&TThis::CompletePreparation, MakeStrong(this)));
     pipeline = CustomizePreparationPipeline(pipeline);
     return pipeline
        ->Add(BIND(&TThis::OnPreparationCompleted, MakeStrong(this)))
        ->Run()
        .Apply(BIND([=] (TValueOrError<void> result) -> TFuture<void> {
            if (result.IsOK()) {
                if (this_->Active) {
                    this_->Running = true;
                }
                return MakeFuture();
            } else {
                LOG_WARNING(result, "Operation has failed to prepare");
                this_->Active = false;
                this_->Host->OnOperationFailed(this_->Operation, result);
                // This promise is never fulfilled.
                return NewPromise<void>();
            }
        }));
}

TFuture<void> TOperationControllerBase::Revive()
{
    try {
        Initialize();
    } catch (const std::exception& ex) {
        OnOperationFailed(TError("Operation has failed to initialize")
            << ex);
        // This promise is never fulfilled.
        return NewPromise<void>();
    }
    return Prepare();
}

TFuture<void> TOperationControllerBase::Commit()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YCHECK(Active);

    LOG_INFO("Committing operation");

    auto this_ = MakeStrong(this);
    return StartAsyncPipeline(CancelableBackgroundInvoker)
        ->Add(BIND(&TThis::CommitOutputs, MakeStrong(this)))
        ->Add(BIND(&TThis::OnOutputsCommitted, MakeStrong(this)))
        ->Run()
        .Apply(BIND([=] (TValueOrError<void> result) -> TFuture<void> {
            Active = false;
            if (result.IsOK()) {
                LOG_INFO("Operation committed");
                return MakeFuture();
            } else {
                LOG_WARNING(result, "Operation has failed to commit");
                this_->Host->OnOperationFailed(this_->Operation, result);
                return NewPromise<void>();
            }
        }));
}

void TOperationControllerBase::OnJobStarted(TJobPtr job)
{
    UsedResources += job->ResourceUsage();
}

void TOperationControllerBase::OnJobRunning(TJobPtr job, const NProto::TJobStatus& status)
{
    UsedResources -= job->ResourceUsage();
    job->ResourceUsage() = status.resource_usage();
    UsedResources += job->ResourceUsage();
}

void TOperationControllerBase::OnJobCompleted(TJobPtr job)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    JobCounter.Completed(1);

    UsedResources -= job->ResourceUsage();

    auto joblet = GetJoblet(job);
    joblet->Task->OnJobCompleted(joblet);

    RemoveJoblet(job);

    LogProgress();

    if (joblet->Task->IsCompleted()) {
        joblet->Task->OnTaskCompleted();
    }

    if (JobCounter.GetRunning() == 0 && GetPendingJobCount() == 0) {
        OnOperationCompleted();
    }
}

void TOperationControllerBase::OnJobFailed(TJobPtr job)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    JobCounter.Failed(1);

    UsedResources -= job->ResourceUsage();

    auto joblet = GetJoblet(job);
    joblet->Task->OnJobFailed(joblet);

    RemoveJoblet(job);

    LogProgress();

    if (JobCounter.GetFailed() >= Config->FailedJobsLimit) {
        OnOperationFailed(TError("Failed jobs limit %d has been reached",
            Config->FailedJobsLimit));
    }

    FOREACH (const auto& chunkId, job->Result().failed_chunk_ids()) {
        OnChunkFailed(TChunkId::FromProto(chunkId));
    }
}

void TOperationControllerBase::OnJobAborted(TJobPtr job)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    JobCounter.Aborted(1);

    UsedResources -= job->ResourceUsage();

    auto joblet = GetJoblet(job);
    joblet->Task->OnJobAborted(joblet);

    RemoveJoblet(job);

    LogProgress();
}

void TOperationControllerBase::OnChunkFailed(const TChunkId& chunkId)
{
    if (InputChunkIds.find(chunkId) == InputChunkIds.end()) {
        LOG_WARNING("Intermediate chunk %s has failed", ~chunkId.ToString());
        OnIntermediateChunkFailed(chunkId);
    } else {
        LOG_WARNING("Input chunk %s has failed", ~chunkId.ToString());
        OnInputChunkFailed(chunkId);
    }
}

void TOperationControllerBase::OnInputChunkFailed(const TChunkId& chunkId)
{
    OnOperationFailed(TError("Unable to read input chunk %s", ~chunkId.ToString()));
}

void TOperationControllerBase::OnIntermediateChunkFailed(const TChunkId& chunkId)
{
    OnOperationFailed(TError("Unable to read intermediate chunk %s", ~chunkId.ToString()));
}

void TOperationControllerBase::Abort()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Aborting operation");

    Running = false;
    Active = false;
    CancelableContext->Cancel();

    AbortTransactions();

    LOG_INFO("Operation aborted");
}

void TOperationControllerBase::OnNodeOnline(TExecNodePtr node)
{
    UNUSED(node);
}

void TOperationControllerBase::OnNodeOffline(TExecNodePtr node)
{
    UNUSED(node);
}

TJobPtr TOperationControllerBase::ScheduleJob(
    ISchedulingContext* context,
    bool isStarving)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!Running) {
        LOG_TRACE("Operation is not running, scheduling request ignored");
        return NULL;
    }

    if (GetPendingJobCount() == 0) {
        LOG_TRACE("No pending jobs left, scheduling request ignored");
        return NULL;
    }

    // Make a course check to see if the node has enough resources.
    auto node = context->GetNode();
    if (!HasEnoughResources(node)) {
        return NULL;
    }

    auto job = DoScheduleJob(context, isStarving);
    if (!job) {
        return NULL;
    }

    JobCounter.Start(1);
    LogProgress();
    return job;
}

void TOperationControllerBase::CustomizeJoblet(TJobletPtr joblet)
{
    UNUSED(joblet);
}

void TOperationControllerBase::CustomizeJobSpec(TJobletPtr joblet, TJobSpec* jobSpec)
{
    UNUSED(joblet);
    UNUSED(jobSpec);
}

void TOperationControllerBase::OnTaskUpdated(TTaskPtr task)
{
    int oldJobCount = CachedPendingJobCount;
    int newJobCount = CachedPendingJobCount + task->GetPendingJobCountDelta();
    CachedPendingJobCount = newJobCount;

    CachedNeededResources += task->GetTotalNeededResourcesDelta();

    LOG_DEBUG_IF(newJobCount != oldJobCount, "Pending job count updated: %d -> %d (Task: %s, NeededResources: {%s})",
        oldJobCount,
        newJobCount,
        ~task->GetId(),
        ~FormatResources(CachedNeededResources));
}

void TOperationControllerBase::AddTaskPendingHint(TTaskPtr task)
{
    if (!task->IsStrictlyLocal() && task->GetPendingJobCount() > 0) {
        auto* info = GetPendingTaskInfo(task);
        if (info->GlobalTasks.insert(task).second) {
            LOG_DEBUG("Task pending hint added (Task: %s)",
                ~task->GetId());
        }
    }
    OnTaskUpdated(task);
}

void TOperationControllerBase::DoAddTaskLocalityHint(TTaskPtr task, const Stroka& address)
{
    auto* info = GetPendingTaskInfo(task);
    if (info->AddressToLocalTasks[address].insert(task).second) {
        LOG_TRACE("Task locality hint added (Task: %s, Address: %s)",
            ~task->GetId(),
            ~address);
    }
}

TOperationControllerBase::TPendingTaskInfo* TOperationControllerBase::GetPendingTaskInfo(TTaskPtr task)
{
    int priority = task->GetPriority();
    YASSERT(priority >= 0 && priority <= MaxTaskPriority);
    return &PendingTaskInfos[priority];
}

void TOperationControllerBase::AddTaskLocalityHint(TTaskPtr task, const Stroka& address)
{
    DoAddTaskLocalityHint(task, address);
    OnTaskUpdated(task);
}

void TOperationControllerBase::AddTaskLocalityHint(TTaskPtr task, TChunkStripePtr stripe)
{
    if (!task->IsStrictlyLocal()) {
        FOREACH (const auto& chunk, stripe->Chunks) {
            FOREACH (const auto& address, chunk->node_addresses()) {
                DoAddTaskLocalityHint(task, address);
            }
        }
    }
    OnTaskUpdated(task);
}

bool TOperationControllerBase::HasEnoughResources(TExecNodePtr node)
{
    return Dominates(
        node->ResourceLimits() + node->ResourceUsageDiscount(),
        node->ResourceUsage() + GetMinNeededResources());
}

bool TOperationControllerBase::HasEnoughResources(TTaskPtr task, TExecNodePtr node)
{
    return node->HasEnoughResources(task->GetMinNeededResources());
}

TJobPtr TOperationControllerBase::DoScheduleJob(
    ISchedulingContext* context,
    bool isStarving)
{
    // First try to find a local task for this node.
    auto now = TInstant::Now();
    auto node = context->GetNode();
    auto address = node->GetAddress();
    for (int priority = static_cast<int>(PendingTaskInfos.size()) - 1; priority >= 0; --priority) {
        auto& info = PendingTaskInfos[priority];
        auto localTasksIt = info.AddressToLocalTasks.find(address);
        if (localTasksIt == info.AddressToLocalTasks.end()) {
            continue;
        }

        i64 bestLocality = 0;
        TTaskPtr bestTask = NULL;

        auto& localTasks = localTasksIt->second;
        auto it = localTasks.begin();
        while (it != localTasks.end()) {
            auto jt = it++;
            auto task = *jt;

            // Make sure that the task is ready to launch jobs.
            // Remove pending hint if not.
            i64 locality = task->GetLocality(address);
            if (locality <= 0) {
                localTasks.erase(jt);
                LOG_TRACE("Task locality hint removed (Task: %s, Address: %s)",
                    ~task->GetId(),
                    ~address);
                continue;
            }

            if (locality <= bestLocality) {
                continue;
            }

            if (!HasEnoughResources(task, node)) {
                continue;
            }

            if (task->GetPendingJobCount() == 0) {
                OnTaskUpdated(task);
                continue;
            }

            bestLocality = locality;
            bestTask = task;
        }

        if (bestTask) {
            auto job = bestTask->ScheduleJob(context);
            if (job) {
                auto delayedTime = bestTask->GetDelayedTime();
                LOG_DEBUG("Scheduled a local job (Task: %s, Address: %s, Priority: %d, Locality: %" PRId64 ", Delay: %s)",
                    ~bestTask->GetId(),
                    ~address,
                    priority,
                    bestLocality,
                    delayedTime ? ~ToString(now - delayedTime.Get()) : "Null");
                bestTask->SetDelayedTime(Null);
                OnTaskUpdated(bestTask);
                OnJobStarted(job);
                return job;
            }
        }
    }

    // Next look for other (global) tasks.
    for (int priority = static_cast<int>(PendingTaskInfos.size()) - 1; priority >= 0; --priority) {
        auto& info = PendingTaskInfos[priority];
        auto& globalTasks = info.GlobalTasks;
        auto it = globalTasks.begin();
        while (it != globalTasks.end()) {
            auto jt = it++;
            auto task = *jt;

            // Make sure that the task is ready to launch jobs.
            // Remove pending hint if not.
            if (task->GetPendingJobCount() == 0) {
                LOG_DEBUG("Task pending hint removed (Task: %s)", ~task->GetId());
                globalTasks.erase(jt);
                OnTaskUpdated(task);
                continue;
            }

            if (!HasEnoughResources(task, node)) {
                continue;
            }

            // Use delayed execution unless starving.
            bool mustWait = false;
            auto delayedTime = task->GetDelayedTime();
            if (delayedTime) {
                mustWait = delayedTime.Get() + task->GetLocalityTimeout() > now;
            } else {
                task->SetDelayedTime(now);
                mustWait = true;
            }
            if (!isStarving && mustWait) {
                continue;
            }

            auto job = task->ScheduleJob(context);
            if (job) {
                LOG_DEBUG("Scheduled a non-local job (Task: %s, Address: %s, Priority: %d, Delay: %s)",
                    ~task->GetId(),
                    ~address,
                    priority,
                    delayedTime ? ~ToString(now - delayedTime.Get()) : "Null");
                OnTaskUpdated(task);
                OnJobStarted(job);
                return job;
            }
        }
    }

    return NULL;
}

TCancelableContextPtr TOperationControllerBase::GetCancelableContext()
{
    return CancelableContext;
}

IInvokerPtr TOperationControllerBase::GetCancelableControlInvoker()
{
    return CancelableControlInvoker;
}

IInvokerPtr TOperationControllerBase::GetCancelableBackgroundInvoker()
{
    return CancelableBackgroundInvoker;
}

int TOperationControllerBase::GetPendingJobCount()
{
    return CachedPendingJobCount;
}

NProto::TNodeResources TOperationControllerBase::GetUsedResources()
{
    return UsedResources;
}

NProto::TNodeResources TOperationControllerBase::GetNeededResources()
{
    return CachedNeededResources;
}

void TOperationControllerBase::OnOperationCompleted()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YCHECK(Active);
    LOG_INFO("Operation completed");

    JobCounter.Finalize();

    Running = false;

    Host->OnOperationCompleted(Operation);
}

void TOperationControllerBase::OnOperationFailed(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!Active)
        return;

    LOG_WARNING(error, "Operation failed");

    Running = false;
    Active = false;

    Host->OnOperationFailed(Operation, error);
}

void TOperationControllerBase::AbortTransactions()
{
    LOG_INFO("Aborting transactions");

    Operation->GetSchedulerTransaction()->Abort();

    // No need to abort the others.
}

TObjectServiceProxy::TInvExecuteBatch TOperationControllerBase::CommitOutputs()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Committing outputs");

    auto batchReq = ObjectProxy.ExecuteBatch();

    FOREACH (auto& table, OutputTables) {
        auto path = FromObjectId(table.ObjectId);
        // Split large outputs into separate requests.
        {
            TChunkListYPathProxy::TReqAttachPtr req;
            int reqSize = 0;
            auto flushReq = [&] () {
                if (req) {
                    batchReq->AddRequest(req, "attach_out");
                    reqSize = 0;
                    req.Reset();
                }
            };

            auto addChunkTree = [&] (const NChunkServer::TChunkTreeId chunkTreeId) {
                if (!req) {
                    req = TChunkListYPathProxy::Attach(FromObjectId(table.OutputChunkListId));
                    NMetaState::GenerateRpcMutationId(req);
                }
                *req->add_children_ids() = chunkTreeId.ToProto();
                ++reqSize;
                if (reqSize >= Config->MaxChildrenPerAttachRequest) {
                    flushReq();
                }
            };

            if (table.KeyColumns && IsSortedOutputSupported()) {
                // Sorted output generated by user operation requires to rearrange job outputs.
                YCHECK(table.Endpoints.size() % 2 == 0);

                LOG_DEBUG("Sorting %d endpoints", static_cast<int>(table.Endpoints.size()));
                std::sort(
                    table.Endpoints.begin(),
                    table.Endpoints.end(),
                    [=] (const TOutputTable::TEndpoint& lhs, const TOutputTable::TEndpoint& rhs) -> bool {
                        // First sort by keys.
                        // Then sort by ChunkTreeKeys.
                        auto keysResult = NTableClient::NProto::CompareKeys(lhs.Key, rhs.Key);
                        if (keysResult != 0) {
                            return keysResult < 0;
                        }
                        return (lhs.ChunkTreeKey - rhs.ChunkTreeKey) < 0;
                });

                auto outputCount = static_cast<int>(table.Endpoints.size()) / 2;
                for (int outputIndex = 0; outputIndex < outputCount; ++outputIndex) {
                    auto& leftEndpoint = table.Endpoints[2 * outputIndex];
                    auto& rightEndpoint = table.Endpoints[2 * outputIndex + 1];
                    if (leftEndpoint.ChunkTreeKey != rightEndpoint.ChunkTreeKey) {
                        auto error = TError(
                            "Ouput table %s is not sorted: overlapping job outputs.",
                            ~table.Path.GetPath());

                        LOG_DEBUG(error);
                        THROW_ERROR error;
                    }

                    auto res = table.OutputChunkTreeIds.equal_range(leftEndpoint.ChunkTreeKey);
                    auto it = res.first;
                    addChunkTree(it->second);
                    // In user operations each ChunkTreeKey corresponds to a single OutputChunkTreeId.
                    // Let's check it.
                    YCHECK(++it == res.second);
                }
            } else {
                FOREACH (const auto& pair, table.OutputChunkTreeIds) {
                    addChunkTree(pair.second);
                }
            }

            flushReq();
        }

        if (table.KeyColumns) {
            LOG_INFO("Table %s will be marked as sorted by %s",
                ~table.Path.GetPath(),
                ~ConvertToYsonString(table.KeyColumns.Get(), EYsonFormat::Text).Data());
            auto req = TTableYPathProxy::SetSorted(path);
            SetTransactionId(req, OutputTransaction);
            ToProto(req->mutable_key_columns(), table.KeyColumns.Get());
            NMetaState::GenerateRpcMutationId(req);
            batchReq->AddRequest(req, "set_out_sorted");
        }
    }

    {
        auto req = TTransactionYPathProxy::Commit(FromObjectId(InputTransaction->GetId()));
        NMetaState::GenerateRpcMutationId(req);
        batchReq->AddRequest(req, "commit_in_tx");
    }

    {
        auto req = TTransactionYPathProxy::Commit(FromObjectId(OutputTransaction->GetId()));
        NMetaState::GenerateRpcMutationId(req);
        batchReq->AddRequest(req, "commit_out_tx");
    }

    // NB: Scheduler transaction is committed by TScheduler.
    // We don't need pings any longer, detach the transactions.
    InputTransaction->Detach();
    OutputTransaction->Detach();

    return batchReq->Invoke();
}

void TOperationControllerBase::OnOutputsCommitted(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error committing outputs");

    {
        auto rsps = batchRsp->GetResponses("attach_out");
        FOREACH (auto rsp, rsps) {
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error attaching chunk trees");
        }
    }

    {
        auto rsps = batchRsp->GetResponses("set_out_sorted");
        FOREACH (auto rsp, rsps) {
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error marking output table as sorted");
        }
    }

    {
        auto rsp = batchRsp->GetResponse("commit_in_tx");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error committing input transaction");
    }

    {
        auto rsp = batchRsp->GetResponse("commit_out_tx");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error committing output transaction");
    }

    LOG_INFO("Outputs committed");
}

TObjectServiceProxy::TInvExecuteBatch TOperationControllerBase::StartIOTransactions()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Starting IO transactions");

    auto batchReq = ObjectProxy.ExecuteBatch();
    const auto& schedulerTransactionId = Operation->GetSchedulerTransaction()->GetId();

    {
        auto req = TTransactionYPathProxy::CreateObject(FromObjectId(schedulerTransactionId));
        req->set_type(EObjectType::Transaction);
        NMetaState::GenerateRpcMutationId(req);
        batchReq->AddRequest(req, "start_in_tx");
    }

    {
        auto req = TTransactionYPathProxy::CreateObject(FromObjectId(schedulerTransactionId));
        req->set_type(EObjectType::Transaction);
        NMetaState::GenerateRpcMutationId(req);
        batchReq->AddRequest(req, "start_out_tx");
    }

    return batchReq->Invoke();
}

void TOperationControllerBase::OnIOTransactionsStarted(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error starting IO transactions");

    {
        auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_in_tx");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error starting input transaction");
        auto id = TTransactionId::FromProto(rsp->object_id());
        LOG_INFO("Input transaction is %s", ~id.ToString());
        InputTransaction = Host->GetTransactionManager()->Attach(id, true);
    }

    {
        auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_out_tx");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error starting output transaction");
        auto id = TTransactionId::FromProto(rsp->object_id());
        LOG_INFO("Output transaction is %s", ~id.ToString());
        OutputTransaction = Host->GetTransactionManager()->Attach(id, true);
    }
}

TObjectServiceProxy::TInvExecuteBatch TOperationControllerBase::GetObjectIds()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Getting object ids");

    auto batchReq = ObjectProxy.ExecuteBatch();

    FOREACH (const auto& table, InputTables) {
        auto req = TObjectYPathProxy::GetId(table.Path.GetPath());
        SetTransactionId(req, InputTransaction);
        req->set_allow_nonempty_path_suffix(true);
        batchReq->AddRequest(req, "get_in_id");
    }

    FOREACH (const auto& table, OutputTables) {
        auto req = TObjectYPathProxy::GetId(table.Path.GetPath());
        SetTransactionId(req, InputTransaction);
        // TODO(babenko): should we allow this?
        req->set_allow_nonempty_path_suffix(true);
        batchReq->AddRequest(req, "get_out_id");
    }

    return batchReq->Invoke();
}

void TOperationControllerBase::OnObjectIdsReceived(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error getting object ids");

    {
        auto getInIdRsps = batchRsp->GetResponses<TObjectYPathProxy::TRspGetId>("get_in_id");
        for (int index = 0; index < static_cast<int>(InputTables.size()); ++index) {
            auto& table = InputTables[index];
            {
                auto rsp = getInIdRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting id for input table %s",
                    ~table.Path.GetPath());
                table.ObjectId = TObjectId::FromProto(rsp->object_id());
            }
        }
    }

    {
        auto getOutIdRsps = batchRsp->GetResponses<TObjectYPathProxy::TRspGetId>("get_out_id");
        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            auto& table = OutputTables[index];
            {
                auto rsp = getOutIdRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting id for output table %s",
                    ~table.Path.GetPath());
                table.ObjectId = TObjectId::FromProto(rsp->object_id());
            }
        }
    }

    LOG_INFO("Object ids received");
}

TObjectServiceProxy::TInvExecuteBatch TOperationControllerBase::GetInputTypes()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Getting input types");

    auto batchReq = ObjectProxy.ExecuteBatch();


    FOREACH (const auto& table, InputTables) {
        auto req = TObjectYPathProxy::Get(FromObjectId(table.ObjectId) + "/@type");
        SetTransactionId(req, InputTransaction);
        batchReq->AddRequest(req, "get_input_types");
    }

    FOREACH (const auto& table, OutputTables) {
        auto req = TObjectYPathProxy::Get(FromObjectId(table.ObjectId) + "/@type");
        SetTransactionId(req, InputTransaction);
        batchReq->AddRequest(req, "get_output_types");
    }

    FOREACH (const auto& pair, GetFilePaths()) {
        const auto& path = pair.first;
        auto req = TObjectYPathProxy::Get(path.GetPath() + "/@type");
        SetTransactionId(req, InputTransaction);
        batchReq->AddRequest(req, "get_file_types");
    }

    return batchReq->Invoke();
}

void TOperationControllerBase::OnInputTypesReceived(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error getting input types");

    {
        auto getInputTypes = batchRsp->GetResponses<TObjectYPathProxy::TRspGet>("get_input_types");
        for (int index = 0; index < static_cast<int>(InputTables.size()); ++index) {
            auto& table = InputTables[index];
            auto rsp = getInputTypes[index];
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting type for input %s",
                ~table.Path.GetPath());

            auto type = ConvertTo<Stroka>(TYsonString(rsp->value()));
            if (type != "table") {
                THROW_ERROR_EXCEPTION("Input %s should be table", ~table.Path.GetPath());
            }
        }
    }

    {
        auto getOutputTypes = batchRsp->GetResponses<TObjectYPathProxy::TRspGet>("get_output_types");
        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            auto& table = OutputTables[index];
            auto rsp = getOutputTypes[index];
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting type for output %s",
                ~table.Path.GetPath());

            auto type = ConvertTo<Stroka>(TYsonString(rsp->value()));
            if (type != "table") {
                THROW_ERROR_EXCEPTION("Output %s should be table", ~table.Path.GetPath());
            }
        }
    }

    {
        auto paths = GetFilePaths();
        auto getFileTypes = batchRsp->GetResponses<TObjectYPathProxy::TRspGet>("get_file_types");
        for (int index = 0; index < static_cast<int>(paths.size()); ++index) {
            auto path = paths[index].first;
            auto stage = paths[index].second;
            auto rsp = getFileTypes[index];
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting type for file %s", ~path.GetPath());

            auto type = ConvertTo<Stroka>(TYsonString(rsp->value()));
            if (type == "file") {
                Files.push_back(TUserFile());
                Files.back().Path = path;
                Files.back().Stage = stage;
            } else if (type == "table") {
                TableFiles.push_back(TUserTableFile());
                TableFiles.back().Path = path;
                TableFiles.back().Stage = stage;
            }
            else {
                THROW_ERROR_EXCEPTION("Incorrect type %s of file %s", ~rsp->value(), ~path.GetPath());
            }
        }
    }

    LOG_INFO("Input types received");
}

TObjectServiceProxy::TInvExecuteBatch TOperationControllerBase::RequestInputs()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Requesting inputs");

    auto batchReq = ObjectProxy.ExecuteBatch();

    FOREACH (const auto& table, InputTables) {
        auto path = FromObjectId(table.ObjectId);
        {
            auto req = TCypressYPathProxy::Lock(path);
            SetTransactionId(req, InputTransaction);
            req->set_mode(ELockMode::Snapshot);
            NMetaState::GenerateRpcMutationId(req);
            batchReq->AddRequest(req, "lock_in");
        }
        {
            // NB: Use table.Path here, otherwise path suffix is ignored.
            auto req = TTableYPathProxy::Fetch(table.Path.GetPath());
            SetTransactionId(req, InputTransaction);
            req->set_fetch_node_addresses(true);
            req->set_fetch_all_meta_extensions(true);
            req->set_negate(table.NegateFetch);
            batchReq->AddRequest(req, "fetch_in");
        }
        {
            auto req = TYPathProxy::Get(path);
            SetTransactionId(req, InputTransaction);
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("sorted");
            attributeFilter.Keys.push_back("sorted_by");
            *req->mutable_attribute_filter() = ToProto(attributeFilter);
            batchReq->AddRequest(req, "get_in_attributes");
        }
    }

    FOREACH (const auto& table, OutputTables) {
        auto path = FromObjectId(table.ObjectId);
        {
            auto req = TCypressYPathProxy::Lock(path);
            SetTransactionId(req, OutputTransaction);
            req->set_mode(table.LockMode);
            NMetaState::GenerateRpcMutationId(req);
            batchReq->AddRequest(req, "lock_out");
        }
        {
            auto req = TYPathProxy::Get(path);
            SetTransactionId(req, OutputTransaction);
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("channels");
            attributeFilter.Keys.push_back("row_count");
            attributeFilter.Keys.push_back("replication_factor");
            *req->mutable_attribute_filter() = ToProto(attributeFilter);
            batchReq->AddRequest(req, "get_out_attributes");
        }
        if (table.Clear) {
            LOG_INFO("Output table %s will be cleared", ~table.Path.GetPath());
            auto req = TTableYPathProxy::Clear(path);
            SetTransactionId(req, OutputTransaction);
            NMetaState::GenerateRpcMutationId(req);
            batchReq->AddRequest(req, "clear_out");
        } else {
            // Even if |Clear| is False we still add a dummy request
            // to keep "clear_out" requests aligned with output tables.
            batchReq->AddRequest(NULL, "clear_out");
        }
        {
            auto req = TTableYPathProxy::GetChunkListForUpdate(path);
            SetTransactionId(req, OutputTransaction);
            batchReq->AddRequest(req, "get_out_chunk_list");
        }
    }

    FOREACH (const auto& file, Files) {
        auto path = file.Path.GetPath();
        {
            auto req = TFileYPathProxy::FetchFile(path);
            SetTransactionId(req, InputTransaction->GetId());
            batchReq->AddRequest(req, "fetch_files");
        }
    }

    FOREACH (const auto& file, TableFiles) {
        auto path = file.Path.GetPath();
        {
            {
                auto req = TTableYPathProxy::Fetch(path);
                req->set_fetch_all_meta_extensions(true);
                SetTransactionId(req, InputTransaction->GetId());
                batchReq->AddRequest(req, "fetch_table_files_chunks");
            }

            {
                auto req = TYPathProxy::GetKey(path);
                SetTransactionId(req, InputTransaction->GetId());
                batchReq->AddRequest(req, "fetch_table_files_names");
            }

            {
                auto req = TYPathProxy::Get(path + "/@uncompressed_data_size");
                SetTransactionId(req, InputTransaction->GetId());
                batchReq->AddRequest(req, "fetch_table_files_sizes");
            }
        }
    }

    RequestCustomInputs(batchReq);

    return batchReq->Invoke();
}

void TOperationControllerBase::OnInputsReceived(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error requesting inputs");

    {
        auto fetchInRsps = batchRsp->GetResponses<TTableYPathProxy::TRspFetch>("fetch_in");
        auto lockInRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_in");
        auto getInAttributesRsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_in_attributes");
        for (int index = 0; index < static_cast<int>(InputTables.size()); ++index) {
            auto& table = InputTables[index];
            {
                auto rsp = lockInRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error locking input table %s",
                    ~table.Path.GetPath());

                LOG_INFO("Input table %s locked",
                    ~table.Path.GetPath());
            }
            {
                auto rsp = fetchInRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error fetching input input table %s",
                    ~table.Path.GetPath());

                table.FetchResponse = rsp;
                FOREACH (const auto& chunk, rsp->chunks()) {
                    auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
                    if (chunk.node_addresses_size() == 0) {
                        THROW_ERROR_EXCEPTION("Chunk %s in input table %s is lost",
                            ~chunkId.ToString(),
                            ~table.Path.GetPath());
                    }
                    InputChunkIds.insert(chunkId);
                }
                LOG_INFO("Input table %s has %d chunks",
                    ~table.Path.GetPath(),
                    rsp->chunks_size());
            }
            {
                auto rsp = getInAttributesRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting attributes for input table %s",
                    ~table.Path.GetPath());

                auto node = ConvertToNode(TYsonString(rsp->value()));
                const auto& attributes = node->Attributes();

                if (attributes.Get<bool>("sorted")) {
                    table.KeyColumns = attributes.Get< std::vector<Stroka> >("sorted_by");
                    LOG_INFO("Input table %s is sorted by %s",
                        ~table.Path.GetPath(),
                        ~ConvertToYsonString(table.KeyColumns.Get(), EYsonFormat::Text).Data());
                } else {
                    LOG_INFO("Input table %s is not sorted",
                        ~table.Path.GetPath());
                }
            }
        }
    }

    {
        auto lockOutRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_out");
        auto clearOutRsps = batchRsp->GetResponses<TTableYPathProxy::TRspClear>("clear_out");
        auto getOutChunkListRsps = batchRsp->GetResponses<TTableYPathProxy::TRspGetChunkListForUpdate>("get_out_chunk_list");
        auto getOutAttributesRsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_out_attributes");
        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            auto& table = OutputTables[index];
            {
                auto rsp = lockOutRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error locking output table %s",
                    ~table.Path.GetPath());

                LOG_INFO("Output table %s locked",
                    ~table.Path.GetPath());
            }
            {
                auto rsp = getOutAttributesRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting attributes for output table %s",
                    ~table.Path.GetPath());

                auto node = ConvertToNode(TYsonString(rsp->value()));
                const auto& attributes = node->Attributes();

                table.Channels = attributes.GetYson("channels");
                LOG_INFO("Output table %s has channels %s",
                    ~table.Path.GetPath(),
                    ~ConvertToYsonString(table.Channels, EYsonFormat::Text).Data());

                i64 initialRowCount = attributes.Get<i64>("row_count");
                if (initialRowCount > 0 && table.Clear && !table.Overwrite) {
                    THROW_ERROR_EXCEPTION("Output table %s must be empty (use \"overwrite\" attribute to force clearing it)",
                        ~table.Path.GetPath());
                }

                table.ReplicationFactor = attributes.Get<int>("replication_factor");
            }
            if (table.Clear) {
                auto rsp = clearOutRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error clearing output table %s",
                    ~table.Path.GetPath());

                LOG_INFO("Output table %s cleared",
                    ~table.Path.GetPath());
            }
            {
                auto rsp = getOutChunkListRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting output chunk list for table %s",
                    ~table.Path.GetPath());

                table.OutputChunkListId = TChunkListId::FromProto(rsp->chunk_list_id());
                LOG_INFO("Output table %s has output chunk list %s",
                    ~table.Path.GetPath(),
                    ~table.OutputChunkListId.ToString());
            }
        }
    }

    {
        auto fetchFilesRsps = batchRsp->GetResponses<TFileYPathProxy::TRspFetchFile>("fetch_files");
        for (int index = 0; index < static_cast<int>(Files.size()); ++index) {
            auto& file = Files[index];
            {
                auto rsp = fetchFilesRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error fetching files");

                if (file.Path.Attributes().Contains("file_name")) {
                    rsp->set_file_name(file.Path.Attributes().Get<Stroka>("file_name"));
                }
                file.FetchResponse = rsp;
                LOG_INFO("File %s consists of chunk %s",
                    ~file.Path.GetPath(),
                    ~TChunkId::FromProto(rsp->chunk_id()).ToString());
            }
        }
    }

    {
        auto fetchTableFilesSizesRsps = batchRsp->GetResponses<TTableYPathProxy::TRspGet>("fetch_table_files_sizes");
        auto fetchTableFilesRsps = batchRsp->GetResponses<TTableYPathProxy::TRspFetch>("fetch_table_files_chunks");
        auto fetchTableFilesNamesRsps = batchRsp->GetResponses<TYPathProxy::TRspGetKey>("fetch_table_files_names");
        for (int index = 0; index < static_cast<int>(TableFiles.size()); ++index) {
            auto& file = TableFiles[index];

            auto sizeRsp = fetchTableFilesSizesRsps[index];
            THROW_ERROR_EXCEPTION_IF_FAILED(*sizeRsp, "Error fetching size of table files");
            auto tableSize = ConvertTo<i64>(TYsonString(sizeRsp->value()));
            if (tableSize > Config->TableFileSizeLimit) {
                THROW_ERROR_EXCEPTION(
                    "Table file %s is too large: " PRIu64 " > " PRIu64,
                    ~file.Path.GetPath(),
                    tableSize,
                    Config->TableFileSizeLimit);
            }

            auto fetchRsp = fetchTableFilesRsps[index];
            THROW_ERROR_EXCEPTION_IF_FAILED(*fetchRsp, "Error fetching chunks of table files");
            file.FetchResponse = fetchRsp;

            auto getKeyRsp = fetchTableFilesNamesRsps[index];
            THROW_ERROR_EXCEPTION_IF_FAILED(*getKeyRsp, "Error fetching names of table files");
            file.FileName = getKeyRsp->value();
            if (file.Path.Attributes().Contains("file_name")) {
                file.FileName = file.Path.Attributes().Get<Stroka>("file_name");
            }
            file.Format = file.Path.Attributes().GetYson("format");

            std::vector<TChunkId> chunkIds;
            FOREACH (const auto chunk, file.FetchResponse->chunks()) {
                chunkIds.push_back(TChunkId::FromProto(chunk.slice().chunk_id()));
            }

            LOG_INFO(
                "Table file %s has file_name %s, format %s and consists of chunks %s",
                ~file.Path.GetPath(),
                ~file.FileName,
                ~file.Format.Data(),
                ~JoinToString(chunkIds));
        }
    }

    OnCustomInputsRecieved(batchRsp);

    LOG_INFO("Inputs received");
}

void TOperationControllerBase::RequestCustomInputs(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
{
    UNUSED(batchReq);
}

void TOperationControllerBase::OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    UNUSED(batchRsp);
}

TFuture<void> TOperationControllerBase::CompletePreparation()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    FOREACH (const auto& table, InputTables) {
        FOREACH (const auto& chunk, table.FetchResponse->chunks()) {
            i64 chunkDataSize;
            i64 chunkRowCount;
            i64 chunkValueCount;
            NTableClient::GetStatistics(chunk, &chunkDataSize, &chunkRowCount, &chunkValueCount);

            TotalInputDataSize += chunkDataSize;
            TotalInputRowCount += chunkRowCount;
            TotalInputValueCount += chunkValueCount;
            ++TotalInputChunkCount;
        }
    }

    LOG_INFO("Input totals collected (ChunkCount: %d, DataSize: %" PRId64 ", RowCount: % " PRId64 ", ValueCount: %" PRId64 ")",
        TotalInputChunkCount,
        TotalInputDataSize,
        TotalInputRowCount,
        TotalInputValueCount);

    // Check for empty inputs.
    if (TotalInputChunkCount == 0) {
        LOG_INFO("Empty input");
        OnOperationCompleted();
        return NewPromise<void>();
    }

    ChunkListPool = New<TChunkListPool>(
        Config,
        Host->GetMasterChannel(),
        CancelableControlInvoker,
        Operation);

    return MakeFuture();
}

void TOperationControllerBase::OnPreparationCompleted()
{
    if (!Active)
        return;

    LOG_INFO("Preparation completed");
}

TAsyncPipeline<void>::TPtr TOperationControllerBase::CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline)
{
    return pipeline;
}

void TOperationControllerBase::ReleaseChunkList(const TChunkListId& id)
{
    std::vector<TChunkListId> ids;
    ids.push_back(id);
    ReleaseChunkLists(ids);
}

void TOperationControllerBase::ReleaseChunkLists(const std::vector<TChunkListId>& ids)
{
    auto batchReq = ObjectProxy.ExecuteBatch();
    FOREACH (const auto& id, ids) {
        auto req = TTransactionYPathProxy::ReleaseObject();
        *req->mutable_object_id() = id.ToProto();
        NMetaState::GenerateRpcMutationId(req);
        batchReq->AddRequest(req);
    }

    // Fire-and-forget.
    // The subscriber is only needed to log the outcome.
    batchReq->Invoke().Subscribe(
        BIND(&TThis::OnChunkListsReleased, MakeStrong(this)));
}

void TOperationControllerBase::OnChunkListsReleased(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    if (!batchRsp->IsOK()) {
        LOG_WARNING(*batchRsp, "Error releasing chunk lists");
    }
}

std::vector<TRefCountedInputChunkPtr> TOperationControllerBase::CollectInputChunks()
{
    // TODO(babenko): set row_attributes
    std::vector<TRefCountedInputChunkPtr> result;
    for (int tableIndex = 0; tableIndex < InputTables.size(); ++tableIndex) {
        const auto& table = InputTables[tableIndex];
        FOREACH (const auto& inputChunk, table.FetchResponse->chunks()) {
            result.push_back(New<TRefCountedInputChunk>(inputChunk, tableIndex));
        }
    }
    return result;
}

std::vector<TChunkStripePtr> TOperationControllerBase::SliceInputChunks(
    TNullable<int> jobCount,
    i64 jobSliceDataSize)
{
    auto inputChunks = CollectInputChunks();

    i64 sliceDataSize =
        jobCount
        ? std::min(jobSliceDataSize, TotalInputDataSize / jobCount.Get() + 1)
        : jobSliceDataSize;

    YCHECK(sliceDataSize > 0);

    // Ensure that no input chunk has size larger than sliceSize.
    std::vector<TChunkStripePtr> stripes;
    FOREACH (auto inputChunk, inputChunks) {
        auto chunkId = TChunkId::FromProto(inputChunk->slice().chunk_id());

        i64 dataSize;
        GetStatistics(*inputChunk, &dataSize);

        if (dataSize > sliceDataSize) {
            int sliceCount = (int) std::ceil((double) dataSize / (double) sliceDataSize);
            auto slicedInputChunks = SliceChunkEvenly(inputChunk, sliceCount);
            FOREACH (auto slicedInputChunk, slicedInputChunks) {
                auto stripe = New<TChunkStripe>(slicedInputChunk);
                stripes.push_back(stripe);
            }
            LOG_TRACE("Slicing chunk (ChunkId: %s, SliceCount: %d)",
                ~chunkId.ToString(),
                sliceCount);
        } else {
            auto stripe = New<TChunkStripe>(inputChunk);
            stripes.push_back(stripe);
            LOG_TRACE("Taking whole chunk (ChunkId: %s)",
                ~chunkId.ToString());
        }
    }


    LOG_DEBUG("Sliced chunks prepared (InputChunkCount: %d, SlicedChunkCount: %d, JobCount: %s, JobSliceDataSize: %" PRId64 ", SliceDataSize: %" PRId64 ")",
        static_cast<int>(inputChunks.size()),
        static_cast<int>(stripes.size()),
        ~ToString(jobCount),
        jobSliceDataSize,
        sliceDataSize);

    return stripes;
}

std::vector<Stroka> TOperationControllerBase::CheckInputTablesSorted(const TNullable< std::vector<Stroka> >& keyColumns)
{
    YCHECK(!InputTables.empty());

    FOREACH (const auto& table, InputTables) {
        if (!table.KeyColumns) {
            THROW_ERROR_EXCEPTION("Input table %s is not sorted",
                ~table.Path.GetPath());
        }
    }

    if (keyColumns) {
        FOREACH (const auto& table, InputTables) {
            if (!CheckKeyColumnsCompatible(table.KeyColumns.Get(), keyColumns.Get())) {
                THROW_ERROR_EXCEPTION("Input table %s is sorted by columns %s that are not compatible with the requested columns %s",
                    ~table.Path.GetPath(),
                    ~ConvertToYsonString(table.KeyColumns.Get(), EYsonFormat::Text).Data(),
                    ~ConvertToYsonString(keyColumns.Get(), EYsonFormat::Text).Data());
            }
        }
        return keyColumns.Get();
    } else {
        const auto& referenceTable = InputTables[0];
        FOREACH (const auto& table, InputTables) {
            if (table.KeyColumns != referenceTable.KeyColumns) {
                THROW_ERROR_EXCEPTION("Key columns do not match: input table %s is sorted by columns %s while input table %s is sorted by columns %s",
                    ~table.Path.GetPath(),
                    ~ConvertToYsonString(table.KeyColumns.Get(), EYsonFormat::Text).Data(),
                    ~referenceTable.Path.GetPath(),
                    ~ConvertToYsonString(referenceTable.KeyColumns.Get(), EYsonFormat::Text).Data());
            }
        }
        return referenceTable.KeyColumns.Get();
    }
}

bool TOperationControllerBase::CheckKeyColumnsCompatible(
    const std::vector<Stroka>& fullColumns,
    const std::vector<Stroka>& prefixColumns)
{
    if (fullColumns.size() < prefixColumns.size()) {
        return false;
    }

    for (int index = 0; index < static_cast<int>(prefixColumns.size()); ++index) {
        if (fullColumns[index] != prefixColumns[index]) {
            return false;
        }
    }

    return true;
}

bool TOperationControllerBase::IsSortedOutputSupported() const
{
    return false;
}

void TOperationControllerBase::RegisterOutputChunkTree(
    const NChunkServer::TChunkTreeId& chunkTreeId,
    int key,
    int tableIndex,
    TOutputTable& table)
{
    table.OutputChunkTreeIds.insert(std::make_pair(key, chunkTreeId));

    LOG_DEBUG("Output chunk tree registered (Table: %d, ChunkTreeId: %s, Key: %d)",
        tableIndex,
        ~chunkTreeId.ToString(),
        key);
}

void TOperationControllerBase::RegisterOutputChunkTree(
    const NChunkServer::TChunkTreeId& chunkTreeId,
    int key,
    int tableIndex)
{
    auto& table = OutputTables[tableIndex];
    RegisterOutputChunkTree(chunkTreeId, key, tableIndex, table);
}

void TOperationControllerBase::RegisterOutputChunkTrees(
    TJobletPtr joblet,
    int key,
    const NProto::TUserJobResult* userJobResult)
{
    for (int tableIndex = 0; tableIndex < static_cast<int>(OutputTables.size()); ++tableIndex) {
        auto& table = OutputTables[tableIndex];
        RegisterOutputChunkTree(joblet->ChunkListIds[tableIndex], key, tableIndex, table);

        if (table.KeyColumns && IsSortedOutputSupported()) {
            YCHECK(userJobResult);
            auto& boundaryKeys = userJobResult->output_boundary_keys(tableIndex);
            YCHECK(boundaryKeys.start() <= boundaryKeys.end());
            {
                TOutputTable::TEndpoint endpoint;
                endpoint.Key = boundaryKeys.start();
                endpoint.Left = true;
                endpoint.ChunkTreeKey = key;
                table.Endpoints.push_back(endpoint);
            }
            {
                TOutputTable::TEndpoint endpoint;
                endpoint.Key = boundaryKeys.end();
                endpoint.Left = false;
                endpoint.ChunkTreeKey = key;
                table.Endpoints.push_back(endpoint);
            }
        }
    }
}

TChunkStripePtr TOperationControllerBase::BuildIntermediateChunkStripe(
    google::protobuf::RepeatedPtrField<NTableClient::NProto::TInputChunk>* inputChunks)
{
    auto stripe = New<TChunkStripe>();
    FOREACH (auto& inputChunk, *inputChunks) {
        stripe->Chunks.push_back(New<TRefCountedInputChunk>(MoveRV(inputChunk)));
    }
    return stripe;
}

bool TOperationControllerBase::HasEnoughChunkLists(int requestedCount)
{
    return ChunkListPool->HasEnough(requestedCount);
}

TChunkListId TOperationControllerBase::ExtractChunkList()
{
    return ChunkListPool->Extract();
}

void TOperationControllerBase::RegisterJoblet(TJobletPtr joblet)
{
    YCHECK(JobsInProgress.insert(std::make_pair(joblet->Job, joblet)).second);
}

TOperationControllerBase::TJobletPtr TOperationControllerBase::GetJoblet(TJobPtr job)
{
    auto it = JobsInProgress.find(job);
    YCHECK(it != JobsInProgress.end());
    return it->second;
}

void TOperationControllerBase::RemoveJoblet(TJobPtr job)
{
    YCHECK(JobsInProgress.erase(job) == 1);
}

void TOperationControllerBase::BuildProgressYson(IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("jobs").BeginMap()
            .Item("total").Scalar(JobCounter.GetCompleted() + JobCounter.GetRunning() + GetPendingJobCount())
            .Item("pending").Scalar(GetPendingJobCount())
            .Item("running").Scalar(JobCounter.GetRunning())
            .Item("completed").Scalar(JobCounter.GetCompleted())
            .Item("failed").Scalar(JobCounter.GetFailed())
            .Item("aborted").Scalar(JobCounter.GetAborted())
            .Item("lost").Scalar(JobCounter.GetLost())
        .EndMap();
}

void TOperationControllerBase::BuildResultYson(IYsonConsumer* consumer)
{
    auto error = FromProto(Operation->Result().error());
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("error").Scalar(error)
        .EndMap();
}

std::vector<TOperationControllerBase::TPathWithStage> TOperationControllerBase::GetFilePaths() const
{
    return std::vector<TPathWithStage>();
}

int TOperationControllerBase::SuggestJobCount(
    i64 totalDataSize,
    i64 minDataSizePerJob,
    i64 maxDataSizePerJob,
    TNullable<int> configJobCount,
    int chunkCount)
{
    i64 minSuggestion = static_cast<i64>(std::ceil((double) totalDataSize / maxDataSizePerJob));
    i64 maxSuggestion = static_cast<i64>(std::ceil((double) totalDataSize / minDataSizePerJob));
    i64 result = configJobCount.Get(minSuggestion);
    result = std::min(result, static_cast<i64>(chunkCount));
    result = std::min(result, maxSuggestion);
    result = std::max(result, static_cast<i64>(1));
    result = std::min(result, static_cast<i64>(Config->MaxJobCount));
    return static_cast<int>(result);
}

void TOperationControllerBase::InitUserJobSpec(
    NScheduler::NProto::TUserJobSpec* proto,
    TUserJobSpecPtr config,
    const std::vector<TUserFile>& files,
    const std::vector<TUserTableFile>& tableFiles)
{
    proto->set_shell_command(config->Command);

    {
        // Set input and output format.
        TFormat inputFormat(EFormatType::Yson);
        TFormat outputFormat(EFormatType::Yson);

        if (config->Format) {
            inputFormat = outputFormat = config->Format.Get();
        }

        if (config->InputFormat) {
            inputFormat = config->InputFormat.Get();
        }

        if (config->OutputFormat) {
            outputFormat = config->OutputFormat.Get();
        }

        proto->set_input_format(ConvertToYsonString(inputFormat).Data());
        proto->set_output_format(ConvertToYsonString(outputFormat).Data());
    }

    auto fillEnvironment = [&] (yhash_map<Stroka, Stroka>& env) {
        FOREACH(const auto& pair, env) {
            proto->add_environment(Sprintf("%s=%s", ~pair.first, ~pair.second));
        }
    };

    // Global environment.
    fillEnvironment(Config->Environment);

    // Local environment.
    fillEnvironment(config->Environment);

    proto->add_environment(Sprintf("YT_OPERATION_ID=%s",
        ~Operation->GetOperationId().ToString()));

    // TODO(babenko): think about per-job files
    FOREACH (const auto& file, files) {
        *proto->add_files() = *file.FetchResponse;
    }

    FOREACH (const auto& file, tableFiles) {
        auto table_file = proto->add_table_files();
        *table_file->mutable_table() = *file.FetchResponse;
        table_file->set_file_name(file.FileName);
        table_file->set_format(file.Format.Data());
    }
}

void TOperationControllerBase::AddUserJobEnvironment(
    NScheduler::NProto::TUserJobSpec* proto,
    TJobletPtr joblet)
{
    proto->add_environment(Sprintf("YT_JOB_INDEX=%d", joblet->JobIndex));
    proto->add_environment(Sprintf("YT_JOB_ID=%s", ~joblet->Job->GetId().ToString()));
    if (joblet->StartRowIndex >= 0) {
        proto->add_environment(Sprintf("YT_START_ROW_INDEX=%" PRId64, joblet->StartRowIndex));
    }
}

void TOperationControllerBase::InitIntermediateInputConfig(TJobIOConfigPtr config)
{
    // Disable master requests.
    config->TableReader->AllowFetchingSeedsFromMaster = false;
}

void TOperationControllerBase::InitIntermediateOutputConfig(TJobIOConfigPtr config)
{
    // Don't replicate intermediate output.
    config->TableWriter->ReplicationFactor = 1;
    config->TableWriter->UploadReplicationFactor = 1;

    // Cache blocks on nodes.
    config->TableWriter->EnableNodeCaching = true;

    // Don't move intermediate chunks.
    config->TableWriter->ChunksMovable = false;
    config->TableWriter->ChunksVital = false;
}

void TOperationControllerBase::InitFinalOutputConfig(TJobIOConfigPtr config)
{
    UNUSED(config);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

