#include "stdafx.h"
#include "operation_controller_detail.h"
#include "private.h"
#include "chunk_list_pool.h"
#include "chunk_pool.h"
#include "helpers.h"
#include "master_connector.h"
#include "chunk_teleporter.h"

#include <ytlib/transaction_client/helpers.h>

#include <ytlib/node_tracker_client/node_directory_builder.h>

#include <ytlib/chunk_client/chunk_list_ypath_proxy.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_slice.h>
#include <ytlib/chunk_client/data_statistics.h>
#include <ytlib/chunk_client/helpers.h>

#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>

#include <ytlib/scheduler/helpers.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/udf_descriptor.h>

#include <core/erasure/codec.h>

#include <core/misc/fs.h>

#include <functional>

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NFileClient;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NFormats;
using namespace NJobProxy;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NNodeTrackerClient::NProto;
using namespace NConcurrency;
using namespace NApi;
using namespace NRpc;
using namespace NTableClient;
using namespace NQueryClient;

using NTableClient::NProto::TBoundaryKeysExt;

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TUserObjectBase::Persist(TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Path);
    Persist(context, ObjectId);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TLivePreviewTableBase::Persist(TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, LivePreviewTableId);
    Persist(context, LivePreviewChunkListId);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TInputTable::Persist(TPersistenceContext& context)
{
    TUserObjectBase::Persist(context);

    using NYT::Persist;
    Persist(context, ChunkCount);
    Persist(context, Chunks);
    Persist(context, SortedBy);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TEndpoint::Persist(TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Key);
    Persist(context, Left);
    Persist(context, ChunkTreeKey);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TOutputTable::Persist(TPersistenceContext& context)
{
    TUserObjectBase::Persist(context);
    TLivePreviewTableBase::Persist(context);

    using NYT::Persist;
    Persist(context, AppendRequested);
    Persist(context, UpdateMode);
    Persist(context, LockMode);
    Persist(context, Options);
    Persist(context, KeyColumns);
    Persist(context, OutputChunkListId);
    // NB: Scheduler snapshots need not be stable.
    Persist<
        TMultiMapSerializer<
            TDefaultSerializer,
            TDefaultSerializer,
            TUnsortedTag
        >
    >(context, OutputChunkTreeIds);
    Persist(context, Endpoints);
    Persist(context, EffectiveAcl);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TIntermediateTable::Persist(TPersistenceContext& context)
{
    TLivePreviewTableBase::Persist(context);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TUserFile::Persist(TPersistenceContext& context)
{
    TUserObjectBase::Persist(context);

    using NYT::Persist;
    Persist<TAttributeDictionaryRefSerializer>(context, Attributes);
    Persist(context, Stage);
    Persist(context, FileName);
    Persist(context, ChunkSpecs);
    Persist(context, Type);
    Persist(context, Executable);
    Persist(context, Format);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TCompletedJob::Persist(TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, IsLost);
    Persist(context, JobId);
    Persist(context, SourceTask);
    Persist(context, OutputCookie);
    Persist(context, DestinationPool);
    Persist(context, InputCookie);
    Persist(context, Address);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TJoblet::Persist(TPersistenceContext& context)
{
    // NB: Every joblet is aborted after snapshot is loaded.
    // Here we only serialize a subset of members required for ReinstallJob to work
    // properly.
    using NYT::Persist;
    Persist(context, Task);
    Persist(context, InputStripeList);
    Persist(context, OutputCookie);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TTaskGroup::Persist(TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, MinNeededResources);
    // NB: Scheduler snapshots need not be stable.
    Persist<
        TSetSerializer<
            TDefaultSerializer,
            TUnsortedTag
        >
    >(context, NonLocalTasks);
    Persist<
        TMultiMapSerializer<
            TDefaultSerializer,
            TDefaultSerializer,
            TUnsortedTag
        >
    >(context, CandidateTasks);
    Persist<
        TMultiMapSerializer<
            TDefaultSerializer,
            TDefaultSerializer,
            TUnsortedTag
        >
    >(context, DelayedTasks);
    Persist<
        TMapSerializer<
            TDefaultSerializer,
            TSetSerializer<
                TDefaultSerializer,
                TUnsortedTag
            >,
            TUnsortedTag
        >
    >(context, LocalTasks);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TStripeDescriptor::Persist(TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Stripe);
    Persist(context, Cookie);
    Persist(context, Task);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TInputChunkDescriptor::Persist(TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, InputStripes);
    Persist(context, ChunkSpecs);
    Persist(context, State);
}

////////////////////////////////////////////////////////////////////

TOperationControllerBase::TInputChunkScratcher::TInputChunkScratcher(
    TOperationControllerBase* controller,
    NRpc::IChannelPtr masterChannel)
    : Controller(controller)
    , PeriodicExecutor(New<TPeriodicExecutor>(
        controller->GetCancelableControlInvoker(),
        BIND(&TInputChunkScratcher::LocateChunks, MakeWeak(this)),
        controller->Config->ChunkScratchPeriod))
    , Proxy(masterChannel)
    , Started(false)
    , Logger(controller->Logger)
{ }

void TOperationControllerBase::TInputChunkScratcher::Start()
{
    if (Started)
        return;

    auto controller = Controller.Lock();
    if (!controller) {
        return;
    }

    Started = true;

    LOG_DEBUG("Starting input chunk scratcher");

    NextChunkIterator = controller->InputChunkMap.begin();
    PeriodicExecutor->Start();
}

void TOperationControllerBase::TInputChunkScratcher::LocateChunks()
{
    auto controller = Controller.Lock();
    if (!controller) {
        return;
    }

    VERIFY_THREAD_AFFINITY(controller->ControlThread);

    auto startIterator = NextChunkIterator;
    // XXX(babenko): multicell
    auto req = Proxy.LocateChunks();

    for (int chunkCount = 0; chunkCount < controller->Config->MaxChunksPerScratch; ++chunkCount) {
        ToProto(req->add_chunk_ids(), NextChunkIterator->first);

        ++NextChunkIterator;
        if (NextChunkIterator == controller->InputChunkMap.end()) {
            NextChunkIterator = controller->InputChunkMap.begin();
        }

        if (NextChunkIterator == startIterator) {
            // Total number of chunks is less than MaxChunksPerScratch.
            break;
        }
    }

    WaitFor(controller->Host->GetChunkLocationThrottler()->Throttle(
        req->chunk_ids_size()));

    LOG_DEBUG("Locating input chunks (Count: %v)", req->chunk_ids_size());

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        LOG_WARNING(rspOrError, "Failed to locate input chunks");
        return;
    }

    const auto& rsp = rspOrError.Value();
    controller->InputNodeDirectory->MergeFrom(rsp->node_directory());

    int availableCount = 0;
    int unavailableCount = 0;
    for (const auto& chunkInfo : rsp->chunks()) {
        auto chunkId = FromProto<TChunkId>(chunkInfo.chunk_id());
        auto it = controller->InputChunkMap.find(chunkId);
        YCHECK(it != controller->InputChunkMap.end());

        auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(chunkInfo.replicas());

        auto& descriptor = it->second;
        YCHECK(!descriptor.ChunkSpecs.empty());
        auto& chunkSpec = descriptor.ChunkSpecs.front();
        auto codecId = NErasure::ECodec(chunkSpec->erasure_codec());

        if (IsUnavailable(replicas, codecId, controller->NeedsAllChunkParts())) {
            ++unavailableCount;
            controller->OnInputChunkUnavailable(chunkId, descriptor);
        } else {
            ++availableCount;
            controller->OnInputChunkAvailable(chunkId, descriptor, replicas);
        }
    }

    LOG_DEBUG("Input chunks located (AvailableCount: %v, UnavailableCount: %v)",
        availableCount,
        unavailableCount);
}

////////////////////////////////////////////////////////////////////

TOperationControllerBase::TTask::TTask()
    : CachedPendingJobCount(-1)
    , CachedTotalJobCount(-1)
    , LastDemandSanityCheckTime(TInstant::Zero())
    , CompletedFired(false)
    , Logger(OperationLogger)
{ }

TOperationControllerBase::TTask::TTask(TOperationControllerBase* controller)
    : Controller(controller)
    , CachedPendingJobCount(0)
    , CachedTotalJobCount(0)
    , LastDemandSanityCheckTime(TInstant::Zero())
    , CompletedFired(false)
    , Logger(OperationLogger)
{ }

void TOperationControllerBase::TTask::Initialize()
{
    Logger = Controller->Logger;
    Logger.AddTag("Task: %v", GetId());
}

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

int TOperationControllerBase::TTask::GetTotalJobCount() const
{
    return GetChunkPoolOutput()->GetTotalJobCount();
}

int TOperationControllerBase::TTask::GetTotalJobCountDelta()
{
    int oldValue = CachedTotalJobCount;
    int newValue = GetTotalJobCount();
    CachedTotalJobCount = newValue;
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
    // NB: Don't call GetMinNeededResources if there are no pending jobs.
    return count == 0 ? ZeroNodeResources() : GetMinNeededResources() * count;
}

bool TOperationControllerBase::TTask::IsIntermediateOutput() const
{
    return false;
}

i64 TOperationControllerBase::TTask::GetLocality(const Stroka& address) const
{
    return GetChunkPoolOutput()->GetLocality(address);
}

bool TOperationControllerBase::TTask::HasInputLocality() const
{
    return true;
}

void TOperationControllerBase::TTask::AddInput(TChunkStripePtr stripe)
{
    Controller->RegisterInputStripe(stripe, this);
    if (HasInputLocality()) {
        Controller->AddTaskLocalityHint(this, stripe);
    }
    AddPendingHint();
}

void TOperationControllerBase::TTask::AddInput(const std::vector<TChunkStripePtr>& stripes)
{
    for (auto stripe : stripes) {
        if (stripe) {
            AddInput(stripe);
        }
    }
}

void TOperationControllerBase::TTask::FinishInput()
{
    LOG_DEBUG("Task input finished");

    GetChunkPoolInput()->Finish();
    AddPendingHint();
    CheckCompleted();
}

void TOperationControllerBase::TTask::CheckCompleted()
{
    if (!CompletedFired && IsCompleted()) {
        CompletedFired = true;
        OnTaskCompleted();
    }
}

TJobId TOperationControllerBase::TTask::ScheduleJob(
    ISchedulingContext* context,
    const TNodeResources& jobLimits)
{
    bool intermediateOutput = IsIntermediateOutput();
    if (!Controller->HasEnoughChunkLists(intermediateOutput)) {
        LOG_DEBUG("Job chunk list demand is not met");
        return NullJobId;
    }

    int jobIndex = Controller->JobIndexGenerator.Next();
    auto joblet = New<TJoblet>(this, jobIndex);

    const auto& nodeResourceLimits = context->ResourceLimits();
    const auto& address = context->GetAddress();
    auto* chunkPoolOutput = GetChunkPoolOutput();
    joblet->OutputCookie = chunkPoolOutput->Extract(address);
    if (joblet->OutputCookie == IChunkPoolOutput::NullCookie) {
        LOG_DEBUG("Job input is empty");
        return NullJobId;
    }

    joblet->InputStripeList = chunkPoolOutput->GetStripeList(joblet->OutputCookie);
    joblet->MemoryReserveEnabled = IsMemoryReserveEnabled();

    auto neededResources = GetNeededResources(joblet);

    // Check the usage against the limits. This is the last chance to give up.
    if (!Dominates(jobLimits, neededResources)) {
        LOG_DEBUG("Job actual resource demand is not met (Limits: {%v}, Demand: {%v})",
            FormatResources(jobLimits),
            FormatResources(neededResources));
        CheckResourceDemandSanity(nodeResourceLimits, neededResources);
        chunkPoolOutput->Aborted(joblet->OutputCookie);
        // Seems like cached min needed resources are too optimistic.
        ResetCachedMinNeededResources();
        return NullJobId;
    }

    auto jobType = GetJobType();

    // Async part.
    auto controller = MakeStrong(Controller); // hold the controller
    auto jobSpecBuilder = BIND([=, this_ = MakeStrong(this)] (TJobSpec* jobSpec) {
        BuildJobSpec(joblet, jobSpec);
        controller->CustomizeJobSpec(joblet, jobSpec);

        auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        schedulerJobSpecExt->set_enable_job_proxy_memory_control(controller->Spec->EnableJobProxyMemoryControl);
        schedulerJobSpecExt->set_enable_sort_verification(controller->Spec->EnableSortVerification);

        // Adjust sizes if approximation flag is set.
        if (joblet->InputStripeList->IsApproximate) {
            schedulerJobSpecExt->set_input_uncompressed_data_size(static_cast<i64>(
                schedulerJobSpecExt->input_uncompressed_data_size() *
                ApproximateSizesBoostFactor));
            schedulerJobSpecExt->set_input_row_count(static_cast<i64>(
                schedulerJobSpecExt->input_row_count() *
                ApproximateSizesBoostFactor));
        }

        if (schedulerJobSpecExt->input_uncompressed_data_size() > Controller->Spec->MaxDataSizePerJob) {
            Controller->OnOperationFailed(TError(
                "Maximum allowed data size per job violated: %v > %v",
                schedulerJobSpecExt->input_uncompressed_data_size(),
                Controller->Spec->MaxDataSizePerJob));
        }
    });

    auto restarted = LostJobCookieMap.find(joblet->OutputCookie) != LostJobCookieMap.end();
    joblet->JobId = context->StartJob(
        Controller->Operation,
        jobType,
        neededResources,
        restarted,
        jobSpecBuilder);

    joblet->JobType = jobType;
    joblet->Address = context->GetAddress();

    LOG_INFO(
        "Job scheduled (JobId: %v, OperationId: %v, JobType: %v, Address: %v, JobIndex: %v, ChunkCount: %v (%v local), "
        "Approximate: %v, DataSize: %v (%v local), RowCount: %v, Restarted: %v, ResourceLimits: {%v})",
        joblet->JobId,
        Controller->OperationId,
        jobType,
        joblet->Address,
        jobIndex,
        joblet->InputStripeList->TotalChunkCount,
        joblet->InputStripeList->LocalChunkCount,
        joblet->InputStripeList->IsApproximate,
        joblet->InputStripeList->TotalDataSize,
        joblet->InputStripeList->LocalDataSize,
        joblet->InputStripeList->TotalRowCount,
        restarted,
        FormatResources(neededResources));

    // Prepare chunk lists.
    if (intermediateOutput) {
        joblet->ChunkListIds.push_back(Controller->ExtractChunkList(Controller->IntermediateOutputCellTag));
    } else {
        for (const auto& table : Controller->OutputTables) {
            joblet->ChunkListIds.push_back(Controller->ExtractChunkList(table.CellTag));
        }
    }

    // Sync part.
    PrepareJoblet(joblet);
    Controller->CustomizeJoblet(joblet);

    Controller->RegisterJoblet(joblet);

    OnJobStarted(joblet);

    return joblet->JobId;
}

bool TOperationControllerBase::TTask::IsPending() const
{
    return GetChunkPoolOutput()->GetPendingJobCount() > 0;
}

bool TOperationControllerBase::TTask::IsCompleted() const
{
    return IsActive() && GetChunkPoolOutput()->IsCompleted();
}

bool TOperationControllerBase::TTask::IsActive() const
{
    return true;
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

void TOperationControllerBase::TTask::Persist(TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, DelayedTime_);

    Persist(context, Controller);

    Persist(context, CachedPendingJobCount);
    Persist(context, CachedTotalJobCount);

    Persist(context, CachedTotalNeededResources);
    Persist(context, CachedMinNeededResources);

    Persist(context, LastDemandSanityCheckTime);

    Persist(context, CompletedFired);

    Persist(context, LostJobCookieMap);
}

void TOperationControllerBase::TTask::PrepareJoblet(TJobletPtr /* joblet */)
{ }

void TOperationControllerBase::TTask::OnJobStarted(TJobletPtr /* joblet */)
{ }

void TOperationControllerBase::TTask::OnJobCompleted(TJobletPtr joblet, const TCompletedJobSummary& jobSummary)
{
    if (Controller->IsRowCountPreserved()) {
        const auto& statistics = jobSummary.Statistics;
        auto inputStatistics = GetTotalInputDataStatistics(statistics);
        auto outputStatistics = GetTotalOutputDataStatistics(statistics);
        if (inputStatistics.row_count() != outputStatistics.row_count()) {
            Controller->OnOperationFailed(TError(
                "Input/output row count mismatch in completed job: %v != %v",
                inputStatistics.row_count(),
                outputStatistics.row_count())
                << TErrorAttribute("task", GetId()));
        }
    }
    GetChunkPoolOutput()->Completed(joblet->OutputCookie);
}

void TOperationControllerBase::TTask::ReinstallJob(TJobletPtr joblet, EJobReinstallReason reason)
{
    Controller->ReleaseChunkLists(joblet->ChunkListIds);

    auto* chunkPoolOutput = GetChunkPoolOutput();

    auto list =
        HasInputLocality()
        ? chunkPoolOutput->GetStripeList(joblet->OutputCookie)
        : nullptr;

    switch (reason) {
        case EJobReinstallReason::Failed:
            chunkPoolOutput->Failed(joblet->OutputCookie);
            break;
        case EJobReinstallReason::Aborted:
            chunkPoolOutput->Aborted(joblet->OutputCookie);
            break;
        default:
            YUNREACHABLE();
    }

    if (HasInputLocality()) {
        for (const auto& stripe : list->Stripes) {
            Controller->AddTaskLocalityHint(this, stripe);
        }
    }

    AddPendingHint();
}

void TOperationControllerBase::TTask::OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& /* jobSummary */)
{
    ReinstallJob(joblet, EJobReinstallReason::Failed);
}

void TOperationControllerBase::TTask::OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& /* jobSummary */)
{
    ReinstallJob(joblet, EJobReinstallReason::Aborted);
}

void TOperationControllerBase::TTask::OnJobLost(TCompletedJobPtr completedJob)
{
    YCHECK(LostJobCookieMap.insert(std::make_pair(
        completedJob->OutputCookie,
        completedJob->InputCookie)).second);
}

void TOperationControllerBase::TTask::OnTaskCompleted()
{
    LOG_DEBUG("Task completed");
}

void TOperationControllerBase::TTask::DoCheckResourceDemandSanity(
    const TNodeResources& neededResources)
{
    auto nodes = Controller->Host->GetExecNodes();
    if (nodes.size() < Controller->Config->SafeOnlineNodeCount)
        return;

    for (auto node : nodes) {
        if (node->CanSchedule(Controller->Operation->GetSchedulingTag()) &&
            Dominates(node->ResourceLimits(), neededResources))
        {
            return;
        }
    }

    // It seems nobody can satisfy the demand.
    Controller->OnOperationFailed(
        TError("No online node can satisfy the resource demand")
            << TErrorAttribute("task", GetId())
            << TErrorAttribute("needed_resources", neededResources));
}

void TOperationControllerBase::TTask::CheckResourceDemandSanity(
    const TNodeResources& neededResources)
{
    // Run sanity check to see if any node can provide enough resources.
    // Don't run these checks too often to avoid jeopardizing performance.
    auto now = TInstant::Now();
    if (now < LastDemandSanityCheckTime + Controller->Config->ResourceDemandSanityCheckPeriod)
        return;
    LastDemandSanityCheckTime = now;

    // Schedule check in control thread.
    Controller->GetCancelableControlInvoker()->Invoke(BIND(
        &TTask::DoCheckResourceDemandSanity,
        MakeWeak(this),
        neededResources));
}

void TOperationControllerBase::TTask::CheckResourceDemandSanity(
    const TNodeResources& nodeResourceLimits,
    const TNodeResources& neededResources)
{
    // The task is requesting more than some node is willing to provide it.
    // Maybe it's OK and we should wait for some time.
    // Or maybe it's not and the task is requesting something no one is able to provide.

    // First check if this very node has enough resources (including those currently
    // allocated by other jobs).
    if (Dominates(nodeResourceLimits, neededResources))
        return;

    CheckResourceDemandSanity(neededResources);
}

void TOperationControllerBase::TTask::AddPendingHint()
{
    Controller->AddTaskPendingHint(this);
}

void TOperationControllerBase::TTask::AddLocalityHint(const Stroka& address)
{
    Controller->AddTaskLocalityHint(this, address);
}

void TOperationControllerBase::TTask::AddSequentialInputSpec(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    TNodeDirectoryBuilder directoryBuilder(
        Controller->InputNodeDirectory,
        schedulerJobSpecExt->mutable_input_node_directory());
    auto* inputSpec = schedulerJobSpecExt->add_input_specs();
    auto list = joblet->InputStripeList;
    for (const auto& stripe : list->Stripes) {
        AddChunksToInputSpec(&directoryBuilder, inputSpec, stripe, list->PartitionTag);
    }
    UpdateInputSpecTotals(jobSpec, joblet);
}

void TOperationControllerBase::TTask::AddParallelInputSpec(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    TNodeDirectoryBuilder directoryBuilder(
        Controller->InputNodeDirectory,
        schedulerJobSpecExt->mutable_input_node_directory());
    auto list = joblet->InputStripeList;
    for (const auto& stripe : list->Stripes) {
        auto* inputSpec = schedulerJobSpecExt->add_input_specs();
        AddChunksToInputSpec(&directoryBuilder, inputSpec, stripe, list->PartitionTag);
    }
    UpdateInputSpecTotals(jobSpec, joblet);
}

void TOperationControllerBase::TTask::AddChunksToInputSpec(
    TNodeDirectoryBuilder* directoryBuilder,
    TTableInputSpec* inputSpec,
    TChunkStripePtr stripe,
    TNullable<int> partitionTag)
{
    for (const auto& chunkSlice : stripe->ChunkSlices) {
        auto* chunkSpec = inputSpec->add_chunks();
        ToProto(chunkSpec, *chunkSlice);
        for (ui32 protoReplica : chunkSlice->GetChunkSpec()->replicas()) {
            auto replica = FromProto<TChunkReplica>(protoReplica);
            directoryBuilder->Add(replica);
        }
        if (partitionTag) {
            chunkSpec->set_partition_tag(*partitionTag);
        }
    }
}

void TOperationControllerBase::TTask::UpdateInputSpecTotals(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    auto list = joblet->InputStripeList;
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    schedulerJobSpecExt->set_input_uncompressed_data_size(
        schedulerJobSpecExt->input_uncompressed_data_size() +
        list->TotalDataSize);
    schedulerJobSpecExt->set_input_row_count(
        schedulerJobSpecExt->input_row_count() +
        list->TotalRowCount);
}

void TOperationControllerBase::TTask::AddFinalOutputSpecs(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    YCHECK(joblet->ChunkListIds.size() == Controller->OutputTables.size());
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    for (int index = 0; index < Controller->OutputTables.size(); ++index) {
        const auto& table = Controller->OutputTables[index];
        auto* outputSpec = schedulerJobSpecExt->add_output_specs();
        outputSpec->set_table_writer_options(ConvertToYsonString(table.Options).Data());
        if (table.KeyColumns) {
            ToProto(outputSpec->mutable_key_columns(), *table.KeyColumns);
        }
        ToProto(outputSpec->mutable_chunk_list_id(), joblet->ChunkListIds[index]);
    }
}

void TOperationControllerBase::TTask::AddIntermediateOutputSpec(
    TJobSpec* jobSpec,
    TJobletPtr joblet,
    TNullable<TKeyColumns> keyColumns)
{
    YCHECK(joblet->ChunkListIds.size() == 1);
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    auto* outputSpec = schedulerJobSpecExt->add_output_specs();
    auto options = New<TTableWriterOptions>();
    options->Account = Controller->Spec->IntermediateDataAccount;
    options->ChunksVital = false;
    options->ReplicationFactor = 1;
    options->CompressionCodec = Controller->Spec->IntermediateCompressionCodec;
    outputSpec->set_table_writer_options(ConvertToYsonString(options).Data());

    if (keyColumns) {
        ToProto(outputSpec->mutable_key_columns(), *keyColumns);
    }
    ToProto(outputSpec->mutable_chunk_list_id(), joblet->ChunkListIds[0]);
}

void TOperationControllerBase::TTask::ResetCachedMinNeededResources()
{
    CachedMinNeededResources.Reset();
}

const TNodeResources& TOperationControllerBase::TTask::GetMinNeededResources() const
{
    if (!CachedMinNeededResources) {
        YCHECK(GetPendingJobCount() > 0);
        CachedMinNeededResources = GetMinNeededResourcesHeavy();
    }
    return *CachedMinNeededResources;
}

TNodeResources TOperationControllerBase::TTask::GetNeededResources(TJobletPtr /* joblet */) const
{
    return GetMinNeededResources();
}

void TOperationControllerBase::TTask::RegisterIntermediate(
    TJobletPtr joblet,
    TChunkStripePtr stripe,
    TTaskPtr destinationTask)
{
    RegisterIntermediate(joblet, stripe, destinationTask->GetChunkPoolInput());

    if (destinationTask->HasInputLocality()) {
        Controller->AddTaskLocalityHint(destinationTask, stripe);
    }
    destinationTask->AddPendingHint();
}

void TOperationControllerBase::TTask::RegisterIntermediate(
    TJobletPtr joblet,
    TChunkStripePtr stripe,
    IChunkPoolInput* destinationPool)
{
    IChunkPoolInput::TCookie inputCookie;

    auto lostIt = LostJobCookieMap.find(joblet->OutputCookie);
    if (lostIt == LostJobCookieMap.end()) {
        inputCookie = destinationPool->Add(stripe);
    } else {
        inputCookie = lostIt->second;
        destinationPool->Resume(inputCookie, stripe);
        LostJobCookieMap.erase(lostIt);
    }

    // Store recovery info.
    auto completedJob = New<TCompletedJob>(
        joblet->JobId,
        this,
        joblet->OutputCookie,
        destinationPool,
        inputCookie,
        joblet->Address);

    Controller->RegisterIntermediate(
        joblet,
        completedJob,
        stripe);
}

TChunkStripePtr TOperationControllerBase::TTask::BuildIntermediateChunkStripe(
    google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs)
{
    auto stripe = New<TChunkStripe>();
    for (auto& chunkSpec : *chunkSpecs) {
        auto chunkSlice = CreateChunkSlice(New<TRefCountedChunkSpec>(std::move(chunkSpec)));
        stripe->ChunkSlices.push_back(chunkSlice);
    }
    return stripe;
}

void TOperationControllerBase::TTask::RegisterOutput(
    TJobletPtr joblet,
    int key,
    const TCompletedJobSummary& jobSummary)
{
    Controller->RegisterOutput(joblet, key, jobSummary);
}

////////////////////////////////////////////////////////////////////

TOperationControllerBase::TOperationControllerBase(
    TSchedulerConfigPtr config,
    TOperationSpecBasePtr spec,
    IOperationHost* host,
    TOperation* operation)
    : Config(config)
    , Host(host)
    , Operation(operation)
    , OperationId(Operation->GetId())
    , AuthenticatedMasterClient(CreateClient())
    , AuthenticatedInputMasterClient(AuthenticatedMasterClient)
    , AuthenticatedOutputMasterClient(AuthenticatedMasterClient)
    , Logger(OperationLogger)
    , CancelableContext(New<TCancelableContext>())
    , CancelableControlInvoker(CancelableContext->CreateInvoker(Host->GetControlInvoker()))
    , CancelableBackgroundInvoker(CancelableContext->CreateInvoker(Host->GetBackgroundInvoker()))
    , Prepared(false)
    , Running(false)
    , TotalEstimatedInputChunkCount(0)
    , TotalEstimatedInputDataSize(0)
    , TotalEstimatedInputRowCount(0)
    , TotalEstimatedInputValueCount(0)
    , TotalEstimatedCompressedDataSize(0)
    , UnavailableInputChunkCount(0)
    , JobCounter(0)
    , Spec(spec)
    , CachedPendingJobCount(0)
    , CachedNeededResources(ZeroNodeResources())
{
    Logger.AddTag("OperationId: %v", operation->GetId());
}

void TOperationControllerBase::Initialize()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Initializing operation (Title: %v)",
        Spec->Title);

    InputNodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    AuxNodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();

    for (const auto& path : GetInputTablePaths()) {
        TInputTable table;
        table.Path = path;
        InputTables.push_back(table);
    }

    for (const auto& path : GetOutputTablePaths()) {
        TOutputTable table;
        table.Path = path;

        if (path.GetAppend()) {
            table.AppendRequested = true;
            table.UpdateMode = EUpdateMode::Append;
            table.LockMode = ELockMode::Shared;
        }

        table.KeyColumns = path.Attributes().Find<TKeyColumns>("sorted_by");
        if (table.KeyColumns) {
            if (!IsSortedOutputSupported()) {
                THROW_ERROR_EXCEPTION("Sorted outputs are not supported");
            }
            table.UpdateMode = EUpdateMode::Overwrite;
            table.LockMode = ELockMode::Exclusive;
        }

        OutputTables.push_back(table);
    }

    for (const auto& pair : GetFilePaths()) {
        TUserFile file;
        file.Path = pair.first;
        file.Stage = pair.second;
        Files.push_back(file);
    }

    if (InputTables.size() > Config->MaxInputTableCount) {
        THROW_ERROR_EXCEPTION(
            "Too many input tables: maximum allowed %v, actual %v",
            Config->MaxInputTableCount,
            InputTables.size());
    }

    if (OutputTables.size() > Config->MaxOutputTableCount) {
        THROW_ERROR_EXCEPTION(
            "Too many output tables: maximum allowed %v, actual %v",
            Config->MaxOutputTableCount,
            OutputTables.size());
    }

    DoInitialize();

    LOG_INFO("Operation initialized");
}

void TOperationControllerBase::Essentiate()
{
    Operation->SetMaxStderrCount(Spec->MaxStderrCount.Get(Config->MaxStderrCount));
    Operation->SetSchedulingTag(Spec->SchedulingTag);

    InitializeTransactions();

    InputChunkScratcher = New<TInputChunkScratcher>(
        this,
        AuthenticatedInputMasterClient->GetMasterChannel(EMasterChannelKind::Leader));
}

void TOperationControllerBase::DoInitialize()
{ }

TFuture<void> TOperationControllerBase::Prepare()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return BIND(&TThis::DoPrepare, MakeStrong(this))
        .AsyncVia(CancelableBackgroundInvoker)
        .Run()
        .Apply(BIND([=, this_ = MakeStrong(this)] () {
            Prepared = true;
            Running = true;
        }).AsyncVia(CancelableControlInvoker));
}

void TOperationControllerBase::DoPrepare()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    GetInputTablesBasicAttributes();
    GetOutputTablesBasicAttributes();
    GetFilesBasicAttributes(&Files);

    LockInputTables();
    LockUserFiles(&Files, {});

    FetchInputTables();
    FetchUserFiles(&Files);

    BeginUploadOutputTables();
    GetOutputTablesUploadParams();

    PickIntermediateDataCell();
    InitCells();

    CreateLivePreviewTables();

    PrepareLivePreviewTablesForUpdate();

    CollectTotals();

    CustomPrepare();

    if (InputChunkMap.empty()) {
        // Possible reasons:
        // - All input chunks are unavailable && Strategy == Skip
        // - Merge decided to passthrough all input chunks
        // - Anything else?
        LOG_INFO("Empty input");
        OnOperationCompleted();
        return;
    }

    SuspendUnavailableInputStripes();

    AddAllTaskPendingHints();

    // Input chunk scratcher initialization should be the last step to avoid races,
    // because input chunk scratcher works in control thread.
    InitInputChunkScratcher();
}

void TOperationControllerBase::SaveSnapshot(TOutputStream* output)
{
    DoSaveSnapshot(output);
}

void TOperationControllerBase::DoSaveSnapshot(TOutputStream* output)
{
    TSaveContext context;
    context.SetOutput(output);

    Save(context, this);
}

TFuture<void> TOperationControllerBase::Revive()
{
    return BIND(&TOperationControllerBase::DoRevive, MakeStrong(this))
        .AsyncVia(CancelableBackgroundInvoker)
        .Run()
        .Apply(BIND([=, this_ = MakeStrong(this)] () {
            ReinstallLivePreview();
            Prepared = true;
            Running = true;
        }).AsyncVia(CancelableControlInvoker));
}

void TOperationControllerBase::DoRevive()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    InitCells();

    DoLoadSnapshot();

    PrepareLivePreviewTablesForUpdate();

    AbortAllJoblets();

    AddAllTaskPendingHints();

    // Input chunk scratcher initialization should be the last step to avoid races.
    InitInputChunkScratcher();
}

void TOperationControllerBase::InitializeTransactions()
{
    StartAsyncSchedulerTransaction();
    if (Operation->GetCleanStart()) {
        StartSyncSchedulerTransaction();
        StartInputTransaction(Operation->GetSyncSchedulerTransaction()->GetId());
        StartOutputTransaction(Operation->GetSyncSchedulerTransaction()->GetId());
    }
}

void TOperationControllerBase::StartAsyncSchedulerTransaction()
{
    LOG_INFO("Starting async scheduler transaction");

    auto channel = AuthenticatedMasterClient->GetMasterChannel(EMasterChannelKind::Leader);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TMasterYPathProxy::CreateObject();
        req->set_type(static_cast<int>(EObjectType::Transaction));

        auto* reqExt = req->mutable_extensions()->MutableExtension(NTransactionClient::NProto::TTransactionCreationExt::transaction_creation_ext);
        reqExt->set_timeout(Config->OperationTransactionTimeout.MilliSeconds());
        reqExt->set_enable_uncommitted_accounting(false);
        reqExt->set_enable_staged_accounting(false);

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Scheduler async for operation %v", OperationId));
        ToProto(req->mutable_object_attributes(), *attributes);

        GenerateMutationId(req);
        batchReq->AddRequest(req, "start_async_tx");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error starting async scheduler transaction");
    if (Operation->GetState() != EOperationState::Initializing &&
        Operation->GetState() != EOperationState::Reviving)
        throw TFiberCanceledException();


    {
        const auto& batchRsp = batchRspOrError.Value();
        auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObject>("start_async_tx").Value();
        auto transactionId = FromProto<TObjectId>(rsp->object_id());
        auto transactionManager = AuthenticatedMasterClient->GetTransactionManager();
        Operation->SetAsyncSchedulerTransaction(transactionManager->Attach(transactionId));
    }

    LOG_INFO("Scheduler async transaction started (AsyncTranasctionId: %v)",
        Operation->GetAsyncSchedulerTransaction()->GetId());
}

void TOperationControllerBase::StartSyncSchedulerTransaction()
{
    LOG_INFO("Starting sync scheduler transaction");

    auto channel = AuthenticatedMasterClient->GetMasterChannel(EMasterChannelKind::Leader);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    {
        auto userTransaction = Operation->GetUserTransaction();
        auto req = TMasterYPathProxy::CreateObject();
        if (userTransaction) {
            ToProto(req->mutable_transaction_id(), userTransaction->GetId());
        }
        req->set_type(static_cast<int>(EObjectType::Transaction));

        auto* reqExt = req->mutable_extensions()->MutableExtension(NTransactionClient::NProto::TTransactionCreationExt::transaction_creation_ext);
        reqExt->set_timeout(Config->OperationTransactionTimeout.MilliSeconds());

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Scheduler sync for operation %v", OperationId));
        ToProto(req->mutable_object_attributes(), *attributes);

        GenerateMutationId(req);
        batchReq->AddRequest(req, "start_sync_tx");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error starting sync scheduler transaction");
    const auto& batchRsp = batchRspOrError.Value();
    if (Operation->GetState() != EOperationState::Initializing &&
        Operation->GetState() != EOperationState::Reviving)
        throw TFiberCanceledException();

    {
        auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObject>("start_sync_tx").Value();
        auto transactionId = FromProto<TObjectId>(rsp->object_id());
        auto transactionManager = Host->GetMasterClient()->GetTransactionManager();
        Operation->SetSyncSchedulerTransaction(transactionManager->Attach(transactionId));
    }

    LOG_INFO("Scheduler sync transaction started (SyncTransactionId: %v)",
        Operation->GetSyncSchedulerTransaction()->GetId());
}

void TOperationControllerBase::StartInputTransaction(TTransactionId parentTransactionId)
{
    LOG_INFO("Starting input transaction");

    auto channel = AuthenticatedInputMasterClient->GetMasterChannel(EMasterChannelKind::Leader);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TMasterYPathProxy::CreateObject();
        ToProto(req->mutable_transaction_id(), parentTransactionId);
        req->set_type(static_cast<int>(EObjectType::Transaction));

        auto* reqExt = req->mutable_extensions()->MutableExtension(NTransactionClient::NProto::TTransactionCreationExt::transaction_creation_ext);
        reqExt->set_timeout(Config->OperationTransactionTimeout.MilliSeconds());

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Scheduler input for operation %v", OperationId));
        ToProto(req->mutable_object_attributes(), *attributes);

        GenerateMutationId(req);
        batchReq->AddRequest(req, "start_in_tx");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error starting input transaction");
    const auto& batchRsp = batchRspOrError.Value();
    if (Operation->GetState() != EOperationState::Initializing &&
        Operation->GetState() != EOperationState::Reviving)
        throw TFiberCanceledException();

    {
        auto rspOrError = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObject>("start_in_tx");
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error starting input transaction");
        const auto& rsp = rspOrError.Value();
        auto id = FromProto<TTransactionId>(rsp->object_id());
        auto transactionManager = AuthenticatedInputMasterClient->GetTransactionManager();
        Operation->SetInputTransaction(transactionManager->Attach(id));
    }
}

void TOperationControllerBase::StartOutputTransaction(TTransactionId parentTransactionId)
{
    LOG_INFO("Starting output transaction");

    auto channel = AuthenticatedOutputMasterClient->GetMasterChannel(EMasterChannelKind::Leader);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TMasterYPathProxy::CreateObject();
        ToProto(req->mutable_transaction_id(), parentTransactionId);
        req->set_type(static_cast<int>(EObjectType::Transaction));

        auto* reqExt = req->mutable_extensions()->MutableExtension(NTransactionClient::NProto::TTransactionCreationExt::transaction_creation_ext);
        reqExt->set_enable_uncommitted_accounting(false);
        reqExt->set_timeout(Config->OperationTransactionTimeout.MilliSeconds());

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Scheduler output for operation %v", OperationId));
        ToProto(req->mutable_object_attributes(), *attributes);

        GenerateMutationId(req);
        batchReq->AddRequest(req, "start_out_tx");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error starting output transactions");
    if (Operation->GetState() != EOperationState::Initializing &&
        Operation->GetState() != EOperationState::Reviving)
        throw TFiberCanceledException();

    {
        const auto& batchRsp = batchRspOrError.Value();
        auto rspOrError = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObject>("start_out_tx");
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error starting output transaction");
        const auto& rsp = rspOrError.Value();
        auto id = FromProto<TTransactionId>(rsp->object_id());
        auto transactionManager = AuthenticatedOutputMasterClient->GetTransactionManager();
        Operation->SetOutputTransaction(transactionManager->Attach(id));
    }
}

void TOperationControllerBase::PickIntermediateDataCell()
{
    auto connection = AuthenticatedOutputMasterClient->GetConnection();
    const auto& secondaryCellTags = connection->GetSecondaryMasterCellTags();
    IntermediateOutputCellTag = secondaryCellTags.empty()
        ? connection->GetPrimaryMasterCellTag()
        : secondaryCellTags[rand() % secondaryCellTags.size()];
}

void TOperationControllerBase::InitCells()
{
    auto client = Host->GetMasterClient();
    auto connection = client->GetConnection();

    auto createPool = [&] (TCellTag cellTag) -> TCellData* {
        auto it = CellMap.find(cellTag);
        if (it == CellMap.end()) {
            it = CellMap.insert(std::make_pair(cellTag, TCellData())).first;
        }

        auto& data = it->second;
        data.ChunkListPool = New<TChunkListPool>(
            Config,
            client->GetMasterChannel(EMasterChannelKind::Leader, cellTag),
            CancelableControlInvoker,
            Operation->GetId(),
            Operation->GetOutputTransaction()->GetId());

        return &data;
    };

    for (const auto& table : OutputTables) {
        auto* data = createPool(table.CellTag);
        ++data->OutputTableCount;
    }

    // NB: The choice of cell must be deterministic.
    IntermediateOutputChunkListPool = createPool(IntermediateOutputCellTag)->ChunkListPool;
}

const TOperationControllerBase::TCellData& TOperationControllerBase::GetCellData(TCellTag cellTag)
{
    auto it = CellMap.find(cellTag);
    YCHECK(it != CellMap.end());
    return it->second;
}

void TOperationControllerBase::InitInputChunkScratcher()
{
    if (UnavailableInputChunkCount > 0) {
        LOG_INFO("Waiting for %v unavailable input chunks", UnavailableInputChunkCount);
        InputChunkScratcher->Start();
    }
}

void TOperationControllerBase::SuspendUnavailableInputStripes()
{
    YCHECK(UnavailableInputChunkCount == 0);

    for (auto& pair : InputChunkMap) {
        const auto& chunkDescriptor = pair.second;
        if (chunkDescriptor.State == EInputChunkState::Waiting) {
            LOG_TRACE("Input chunk is unavailable (ChunkId: %v)", pair.first);
            for (const auto& inputStripe : chunkDescriptor.InputStripes) {
                if (inputStripe.Stripe->WaitingChunkCount == 0) {
                    inputStripe.Task->GetChunkPoolInput()->Suspend(inputStripe.Cookie);
                }
                ++inputStripe.Stripe->WaitingChunkCount;
            }
            ++UnavailableInputChunkCount;
        }
    }
}

void TOperationControllerBase::ReinstallLivePreview()
{
    auto masterConnector = Host->GetMasterConnector();

    if (IsOutputLivePreviewSupported()) {
        for (const auto& table : OutputTables) {
            std::vector<TChunkTreeId> childrenIds;
            childrenIds.reserve(table.OutputChunkTreeIds.size());
            for (const auto& pair : table.OutputChunkTreeIds) {
                childrenIds.push_back(pair.second);
            }
            masterConnector->AttachToLivePreview(
                Operation,
                table.LivePreviewChunkListId,
                childrenIds);
        }
    }

    if (IsIntermediateLivePreviewSupported()) {
        std::vector<TChunkTreeId> childrenIds;
        childrenIds.reserve(ChunkOriginMap.size());
        for (const auto& pair : ChunkOriginMap) {
            if (!pair.second->IsLost) {
                childrenIds.push_back(pair.first);
            }
        }
        masterConnector->AttachToLivePreview(
            Operation,
            IntermediateTable.LivePreviewChunkListId,
            childrenIds);
    }
}

void TOperationControllerBase::AbortAllJoblets()
{
    for (const auto& pair : JobletMap) {
        auto joblet = pair.second;
        JobCounter.Aborted(1);
        joblet->Task->OnJobAborted(joblet, TAbortedJobSummary(pair.first, EAbortReason::Scheduler));
    }
    JobletMap.clear();
}

void TOperationControllerBase::DoLoadSnapshot()
{
    LOG_INFO("Started loading snapshot");

    auto snapshot = Operation->Snapshot();
    TMemoryInput input(snapshot.Begin(), snapshot.Size());

    TLoadContext context;
    context.SetInput(&input);

    NPhoenix::TSerializer::InplaceLoad(context, this);

    LOG_INFO("Finished loading snapshot");
}

TFuture<void> TOperationControllerBase::Commit()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return BIND(&TThis::DoCommit, MakeStrong(this))
        .AsyncVia(CancelableBackgroundInvoker)
        .Run();
}

void TOperationControllerBase::DoCommit()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    TeleportOutputChunks();
    AttachOutputChunks();
    EndUploadOutputTables();
    CustomCommit();

    LOG_INFO("Results committed");
}

void TOperationControllerBase::TeleportOutputChunks()
{
    auto teleporter = New<TChunkTeleporter>(
        Config,
        AuthenticatedOutputMasterClient,
        CancelableControlInvoker,
        Operation->GetSyncSchedulerTransaction()->GetId(),
        Logger);

    for (const auto& table : OutputTables) {
        for (const auto& pair : table.OutputChunkTreeIds) {
            const auto& id = pair.second;
            auto type = TypeFromId(id);
            if (type == EObjectType::Chunk || type == EObjectType::ErasureChunk) {
                teleporter->RegisterChunk(id, table.CellTag);
            }
        }
    }

    WaitFor(teleporter->Run())
        .ThrowOnError();
}

void TOperationControllerBase::AttachOutputChunks()
{
    for (auto& table : OutputTables) {
        auto objectIdPath = FromObjectId(table.ObjectId);
        const auto& path = table.Path.GetPath();

        LOG_INFO("Attaching output chunks (Path: %v)",
            path);

        auto channel = AuthenticatedOutputMasterClient->GetMasterChannel(EMasterChannelKind::Leader, table.CellTag);
        TObjectServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();

        // Split large outputs into separate requests.
        {
            TChunkListYPathProxy::TReqAttachPtr req;
            int reqSize = 0;
            auto flushReq = [ & ]() {
                if (req) {
                    batchReq->AddRequest(req, "attach");
                    reqSize = 0;
                    req.Reset();
                }
            };

            auto addChunkTree = [ & ](const TChunkTreeId& chunkTreeId) {
                if (!req) {
                    req = TChunkListYPathProxy::Attach(FromObjectId(table.OutputChunkListId));
                    req->set_request_statistics(false);
                    GenerateMutationId(req);
                }
                ToProto(req->add_children_ids(), chunkTreeId);
                ++reqSize;
                if (reqSize >= Config->MaxChildrenPerAttachRequest) {
                    flushReq();
                }
            };

            if (table.KeyColumns && IsSortedOutputSupported()) {
                // Sorted output generated by user operation requires rearranging.
                YCHECK(table.Endpoints.size() % 2 == 0);

                LOG_DEBUG("Sorting %v endpoints", table.Endpoints.size());
                std::sort(
                    table.Endpoints.begin(),
                    table.Endpoints.end(),
                    [ = ](const TEndpoint& lhs, const TEndpoint& rhs) -> bool {
                        // First sort by keys.
                        // Then sort by ChunkTreeKeys.
                        auto keysResult = CompareRows(lhs.Key, rhs.Key);
                        if (keysResult != 0) {
                            return keysResult < 0;
                        }
                        return lhs.ChunkTreeKey < rhs.ChunkTreeKey;
                    });

                int outputCount = table.Endpoints.size() / 2;
                for (int outputIndex = 0; outputIndex < outputCount; ++outputIndex) {
                    auto& leftEndpoint = table.Endpoints[2 * outputIndex];
                    auto& rightEndpoint = table.Endpoints[2 * outputIndex + 1];
                    if (leftEndpoint.ChunkTreeKey != rightEndpoint.ChunkTreeKey) {
                        THROW_ERROR_EXCEPTION("Output table %v is not sorted: job outputs have overlapping key ranges",
                            table.Path.GetPath());
                    }
                    auto pair = table.OutputChunkTreeIds.equal_range(leftEndpoint.ChunkTreeKey);
                    auto it = pair.first;
                    addChunkTree(it->second);
                    // In user operations each ChunkTreeKey corresponds to a single OutputChunkTreeId.
                    // Let's check it.
                    YCHECK(++it == pair.second);
                }
            } else {
                for (const auto& pair : table.OutputChunkTreeIds) {
                    addChunkTree(pair.second);
                }
            }

            flushReq();
        }

        {
            auto req = TChunkListYPathProxy::GetStatistics(FromObjectId(table.OutputChunkListId));
            batchReq->AddRequest(req, "get_statistics");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error attaching chunks to output table %v",
            path);
        const auto& batchRsp = batchRspOrError.Value();

        {
            auto rsp = batchRsp->GetResponse<TChunkListYPathProxy::TRspGetStatistics>("get_statistics").Value();
            table.DataStatistics = rsp->statistics();
        }
    }
}

void TOperationControllerBase::CustomCommit()
{ }

void TOperationControllerBase::EndUploadOutputTables()
{
    auto channel = AuthenticatedOutputMasterClient->GetMasterChannel(EMasterChannelKind::Leader);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    for (const auto& table : OutputTables) {
        auto objectIdPath = FromObjectId(table.ObjectId);
        const auto& path = table.Path.GetPath();

        LOG_INFO("Finishing upload to output to table (Path: %v, KeyColumns: %v)",
            path,
            table.KeyColumns ? MakeNullable("[" + JoinToString(*table.KeyColumns) + "]") : Null);

        {
            auto req = TTableYPathProxy::EndUpload(objectIdPath);
            *req->mutable_statistics() = table.DataStatistics;
            if (table.KeyColumns) {
                ToProto(req->mutable_key_columns(), *table.KeyColumns);
            }
            SetTransactionId(req, table.UploadTransactionId);
            GenerateMutationId(req);
            batchReq->AddRequest(req, "end_upload");
        }
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error finishing upload to output tables");
}

void TOperationControllerBase::OnJobRunning(const TJobId& /* jobId */, const TJobStatus& /* status */)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
}

void TOperationControllerBase::OnJobStarted(const TJobId& jobId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto joblet = GetJoblet(jobId);
    auto address = joblet->Address;
    LogEventFluently(ELogEventType::JobStarted)
        .Item("job_id").Value(joblet->JobId)
        .Item("operation_id").Value(OperationId)
        .Item("resource_limits").Value(joblet->ResourceLimits)
        .Item("node_address").Value(address)
        .Item("job_type").Value(joblet->JobType)
        .Item("total_data_size").Value(joblet->InputStripeList->TotalDataSize)
        .Item("local_data_size").Value(joblet->InputStripeList->LocalDataSize)
        .Item("scheduling_locality").Value(joblet->Task->GetLocality(address));

    JobCounter.Start(1);
}

void TOperationControllerBase::OnJobCompleted(const TCompletedJobSummary& jobSummary)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto& jobId = jobSummary.Id;
    const auto& result = jobSummary.Result;

    JobCounter.Completed(1);

    const auto& schedulerResultEx = result->GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

    // Populate node directory by adding additional nodes returned from the job.
    // NB: Job's output may become some other job's input.
    InputNodeDirectory->MergeFrom(schedulerResultEx.output_node_directory());

    auto joblet = GetJoblet(jobId);
    joblet->Task->OnJobCompleted(joblet, jobSummary);

    RemoveJoblet(jobId);

    UpdateTask(joblet->Task);

    if (IsCompleted()) {
        OnOperationCompleted();
    }
}

void TOperationControllerBase::OnJobFailed(const TFailedJobSummary& jobSummary)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto& jobId = jobSummary.Id;
    const auto& result = jobSummary.Result;

    auto error = FromProto<TError>(result->error());

    JobCounter.Failed(1);

    auto joblet = GetJoblet(jobId);
    joblet->Task->OnJobFailed(joblet, jobSummary);

    RemoveJoblet(jobId);

    if (error.Attributes().Get<bool>("fatal", false)) {
        OnOperationFailed(error);
        return;
    }

    int failedJobCount = JobCounter.GetFailed();
    int maxFailedJobCount = Spec->MaxFailedJobCount.Get(Config->MaxFailedJobCount);
    if (failedJobCount >= maxFailedJobCount) {
        OnOperationFailed(TError("Failed jobs limit exceeded")
            << TErrorAttribute("max_failed_job_count", maxFailedJobCount));
    }
}

void TOperationControllerBase::OnJobAborted(const TAbortedJobSummary& jobSummary)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto& jobId = jobSummary.Id;
    auto abortReason = jobSummary.AbortReason;

    JobCounter.Aborted(1, abortReason);

    auto joblet = GetJoblet(jobId);
    joblet->Task->OnJobAborted(joblet, jobSummary);

    RemoveJoblet(jobId);

    if (abortReason == EAbortReason::FailedChunks) {
        const auto& result = jobSummary.Result;
        const auto& schedulerResultExt = result->GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

        for (const auto& chunkId : schedulerResultExt.failed_chunk_ids()) {
            OnChunkFailed(FromProto<TChunkId>(chunkId));
        }
    }
}

void TOperationControllerBase::OnChunkFailed(const TChunkId& chunkId)
{
    auto it = InputChunkMap.find(chunkId);
    if (it == InputChunkMap.end()) {
        LOG_WARNING("Intermediate chunk %v has failed", chunkId);
        OnIntermediateChunkUnavailable(chunkId);
    } else {
        LOG_WARNING("Input chunk %v has failed", chunkId);
        OnInputChunkUnavailable(chunkId, it->second);
    }
}

void TOperationControllerBase::OnInputChunkAvailable(const TChunkId& chunkId, TInputChunkDescriptor& descriptor, const TChunkReplicaList& replicas)
{
    if (descriptor.State != EInputChunkState::Waiting)
        return;

    LOG_TRACE("Input chunk is available (ChunkId: %v)", chunkId);

    --UnavailableInputChunkCount;
    YCHECK(UnavailableInputChunkCount >= 0);

    // Update replicas in place for all input chunks with current chunkId.
    for (auto& chunkSpec : descriptor.ChunkSpecs) {
        chunkSpec->mutable_replicas()->Clear();
        ToProto(chunkSpec->mutable_replicas(), replicas);
    }

    descriptor.State = EInputChunkState::Active;

    for (const auto& inputStripe : descriptor.InputStripes) {
        --inputStripe.Stripe->WaitingChunkCount;
        if (inputStripe.Stripe->WaitingChunkCount > 0)
            continue;

        auto task = inputStripe.Task;
        task->GetChunkPoolInput()->Resume(inputStripe.Cookie, inputStripe.Stripe);
        if (task->HasInputLocality()) {
            AddTaskLocalityHint(task, inputStripe.Stripe);
        }
        AddTaskPendingHint(task);
    }
}

void TOperationControllerBase::OnInputChunkUnavailable(const TChunkId& chunkId, TInputChunkDescriptor& descriptor)
{
    if (descriptor.State != EInputChunkState::Active)
        return;

    LOG_TRACE("Input chunk is unavailable (ChunkId: %v)", chunkId);

    ++UnavailableInputChunkCount;

    switch (Spec->UnavailableChunkTactics) {
        case EUnavailableChunkAction::Fail:
            OnOperationFailed(TError("Input chunk %v is unavailable",
                chunkId));
            break;

        case EUnavailableChunkAction::Skip: {
            descriptor.State = EInputChunkState::Skipped;
            for (const auto& inputStripe : descriptor.InputStripes) {
                inputStripe.Task->GetChunkPoolInput()->Suspend(inputStripe.Cookie);

                // Remove given chunk from the stripe list.
                SmallVector<TChunkSlicePtr, 1> slices;
                std::swap(inputStripe.Stripe->ChunkSlices, slices);

                std::copy_if(
                    slices.begin(),
                    slices.end(),
                    inputStripe.Stripe->ChunkSlices.begin(),
                    [&] (TChunkSlicePtr slice) {
                        return chunkId != FromProto<TChunkId>(slice->GetChunkSpec()->chunk_id());
                    });

                // Reinstall patched stripe.
                inputStripe.Task->GetChunkPoolInput()->Resume(inputStripe.Cookie, inputStripe.Stripe);
                AddTaskPendingHint(inputStripe.Task);
            }
            InputChunkScratcher->Start();
            break;
        }

        case EUnavailableChunkAction::Wait: {
            descriptor.State = EInputChunkState::Waiting;
            for (const auto& inputStripe : descriptor.InputStripes) {
                if (inputStripe.Stripe->WaitingChunkCount == 0) {
                    inputStripe.Task->GetChunkPoolInput()->Suspend(inputStripe.Cookie);
                }
                ++inputStripe.Stripe->WaitingChunkCount;
            }
            InputChunkScratcher->Start();
            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TOperationControllerBase::OnIntermediateChunkUnavailable(const TChunkId& chunkId)
{
    auto it = ChunkOriginMap.find(chunkId);
    YCHECK(it != ChunkOriginMap.end());
    auto completedJob = it->second;
    if (completedJob->IsLost)
        return;

    LOG_INFO("Job is lost (Address: %v, JobId: %v, SourceTask: %v, OutputCookie: %v, InputCookie: %v)",
        completedJob->Address,
        completedJob->JobId,
        completedJob->SourceTask->GetId(),
        completedJob->OutputCookie,
        completedJob->InputCookie);

    JobCounter.Lost(1);
    completedJob->IsLost = true;
    completedJob->DestinationPool->Suspend(completedJob->InputCookie);
    completedJob->SourceTask->GetChunkPoolOutput()->Lost(completedJob->OutputCookie);
    completedJob->SourceTask->OnJobLost(completedJob);
    AddTaskPendingHint(completedJob->SourceTask);
}

bool TOperationControllerBase::IsOutputLivePreviewSupported() const
{
    return false;
}

bool TOperationControllerBase::IsIntermediateLivePreviewSupported() const
{
    return false;
}

void TOperationControllerBase::Abort()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Aborting operation");

    Running = false;

    CancelableContext->Cancel();

    LOG_INFO("Operation aborted");
}

void TOperationControllerBase::CheckTimeLimit()
{
    auto timeLimit = Config->OperationTimeLimit;
    if (Spec->TimeLimit) {
        timeLimit = Spec->TimeLimit;
    }

    if (timeLimit) {
        if (TInstant::Now() - Operation->GetStartTime() > *timeLimit) {
            OnOperationFailed(TError("Operation is running for too long, aborted")
                << TErrorAttribute("time_limit", *timeLimit));
        }
    }
}

TJobId TOperationControllerBase::ScheduleJob(
    ISchedulingContext* context,
    const TNodeResources& jobLimits)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!Running ||
        Operation->GetState() != EOperationState::Running ||
        Operation->GetSuspended())
    {
        LOG_TRACE("Operation is not running, scheduling request ignored");
        return NullJobId;
    }

    if (GetPendingJobCount() == 0) {
        LOG_TRACE("No pending jobs left, scheduling request ignored");
        return NullJobId;
    }

    auto jobId = DoScheduleJob(context, jobLimits);
    if (!jobId) {
        return NullJobId;
    }

    OnJobStarted(jobId);

    return jobId;
}

void TOperationControllerBase::CustomizeJoblet(TJobletPtr /* joblet */)
{ }

void TOperationControllerBase::CustomizeJobSpec(TJobletPtr /* joblet */, TJobSpec* /* jobSpec */)
{ }

void TOperationControllerBase::RegisterTask(TTaskPtr task)
{
    Tasks.push_back(std::move(task));
}

void TOperationControllerBase::RegisterTaskGroup(TTaskGroupPtr group)
{
    TaskGroups.push_back(std::move(group));
}

void TOperationControllerBase::UpdateTask(TTaskPtr task)
{
    int oldPendingJobCount = CachedPendingJobCount;
    int newPendingJobCount = CachedPendingJobCount + task->GetPendingJobCountDelta();
    CachedPendingJobCount = newPendingJobCount;

    int oldTotalJobCount = JobCounter.GetTotal();
    JobCounter.Increment(task->GetTotalJobCountDelta());
    int newTotalJobCount = JobCounter.GetTotal();

    CachedNeededResources += task->GetTotalNeededResourcesDelta();

    LOG_DEBUG_IF(
        newPendingJobCount != oldPendingJobCount || newTotalJobCount != oldTotalJobCount,
        "Task updated (Task: %v, PendingJobCount: %v -> %v, TotalJobCount: %v -> %v, NeededResources: {%v})",
        task->GetId(),
        oldPendingJobCount,
        newPendingJobCount,
        oldTotalJobCount,
        newTotalJobCount,
        FormatResources(CachedNeededResources));

    task->CheckCompleted();
}

void TOperationControllerBase::UpdateAllTasks()
{
    for (auto& task: Tasks) {
        task->ResetCachedMinNeededResources();
        UpdateTask(task);
    }
}

void TOperationControllerBase::MoveTaskToCandidates(
    TTaskPtr task,
    std::multimap<i64, TTaskPtr>& candidateTasks)
{
    const auto& neededResources = task->GetMinNeededResources();
    task->CheckResourceDemandSanity(neededResources);
    i64 minMemory = neededResources.memory();
    candidateTasks.insert(std::make_pair(minMemory, task));
    LOG_DEBUG("Task moved to candidates (Task: %v, MinMemory: %v)",
        task->GetId(),
        minMemory / (1024 * 1024));

}

void TOperationControllerBase::AddTaskPendingHint(TTaskPtr task)
{
    if (task->GetPendingJobCount() > 0) {
        auto group = task->GetGroup();
        if (group->NonLocalTasks.insert(task).second) {
            LOG_DEBUG("Task pending hint added (Task: %v)", task->GetId());
            MoveTaskToCandidates(task, group->CandidateTasks);
        }
    }
    UpdateTask(task);
}

void TOperationControllerBase::AddAllTaskPendingHints()
{
    for (auto task : Tasks) {
        AddTaskPendingHint(task);
    }
}

void TOperationControllerBase::DoAddTaskLocalityHint(TTaskPtr task, const Stroka& address)
{
    auto group = task->GetGroup();
    if (group->LocalTasks[address].insert(task).second) {
        LOG_TRACE("Task locality hint added (Task: %v, Address: %v)",
            task->GetId(),
            address);
    }
}

void TOperationControllerBase::AddTaskLocalityHint(TTaskPtr task, const Stroka& address)
{
    DoAddTaskLocalityHint(task, address);
    UpdateTask(task);
}

void TOperationControllerBase::AddTaskLocalityHint(TTaskPtr task, TChunkStripePtr stripe)
{
    for (const auto& chunkSlice : stripe->ChunkSlices) {
        for (ui32 protoReplica : chunkSlice->GetChunkSpec()->replicas()) {
            auto replica = FromProto<NChunkClient::TChunkReplica>(protoReplica);

            if (chunkSlice->GetLocality(replica.GetIndex()) > 0) {
                const auto& descriptor = InputNodeDirectory->GetDescriptor(replica);
                DoAddTaskLocalityHint(task, descriptor.GetDefaultAddress());
            }
        }
    }
    UpdateTask(task);
}

void TOperationControllerBase::ResetTaskLocalityDelays()
{
    LOG_DEBUG("Task locality delays are reset");
    for (auto group : TaskGroups) {
        for (const auto& pair : group->DelayedTasks) {
            auto task = pair.second;
            if (task->GetPendingJobCount() > 0) {
                MoveTaskToCandidates(task, group->CandidateTasks);
            }
        }
        group->DelayedTasks.clear();
    }
}

bool TOperationControllerBase::CheckJobLimits(
    TTaskPtr task,
    const TNodeResources& jobLimits,
    const TNodeResources& nodeResourceLimits)
{
    auto neededResources = task->GetMinNeededResources();
    if (Dominates(jobLimits, neededResources)) {
        return true;
    }
    task->CheckResourceDemandSanity(nodeResourceLimits, neededResources);
    return false;
}

TJobId TOperationControllerBase::DoScheduleJob(
    ISchedulingContext* context,
    const TNodeResources& jobLimits)
{
    auto localJobId = DoScheduleLocalJob(context, jobLimits);
    if (localJobId) {
        return localJobId;
    }

    auto nonLocalJobId = DoScheduleNonLocalJob(context, jobLimits);
    if (nonLocalJobId) {
        return nonLocalJobId;
    }

    return NullJobId;
}

TJobId TOperationControllerBase::DoScheduleLocalJob(
    ISchedulingContext* context,
    const TNodeResources& jobLimits)
{
    const auto& nodeResourceLimits = context->ResourceLimits();
    const auto& address = context->GetAddress();

    for (auto group : TaskGroups) {
        if (!Dominates(jobLimits, group->MinNeededResources)) {
            continue;
        }

        auto localTasksIt = group->LocalTasks.find(address);
        if (localTasksIt == group->LocalTasks.end()) {
            continue;
        }

        i64 bestLocality = 0;
        TTaskPtr bestTask = nullptr;

        auto& localTasks = localTasksIt->second;
        auto it = localTasks.begin();
        while (it != localTasks.end()) {
            auto jt = it++;
            auto task = *jt;

            // Make sure that the task have positive locality.
            // Remove pending hint if not.
            i64 locality = task->GetLocality(address);
            if (locality <= 0) {
                localTasks.erase(jt);
                LOG_TRACE("Task locality hint removed (Task: %v, Address: %v)",
                    task->GetId(),
                    address);
                continue;
            }

            if (locality <= bestLocality) {
                continue;
            }

            if (task->GetPendingJobCount() == 0) {
                UpdateTask(task);
                continue;
            }

            if (!CheckJobLimits(task, jobLimits, nodeResourceLimits)) {
                continue;
            }

            bestLocality = locality;
            bestTask = task;
        }

        if (!Running) {
            return NullJobId;
        }

        if (bestTask) {
            LOG_DEBUG(
                "Attempting to schedule a local job (Task: %v, Address: %v, Locality: %v, JobLimits: {%v}, "
                "PendingDataSize: %v, PendingJobCount: %v)",
                bestTask->GetId(),
                address,
                bestLocality,
                FormatResources(jobLimits),
                bestTask->GetPendingDataSize(),
                bestTask->GetPendingJobCount());
            auto jobId = bestTask->ScheduleJob(context, jobLimits);
            if (jobId) {
                UpdateTask(bestTask);
                return jobId;
            }
        }
    }
    return NullJobId;
}

TJobId TOperationControllerBase::DoScheduleNonLocalJob(
    ISchedulingContext* context,
    const TNodeResources& jobLimits)
{
    auto now = context->GetNow();
    const auto& nodeResourceLimits = context->ResourceLimits();
    const auto& address = context->GetAddress();

    for (auto group : TaskGroups) {
        if (!Dominates(jobLimits, group->MinNeededResources)) {
            continue;
        }

        auto& nonLocalTasks = group->NonLocalTasks;
        auto& candidateTasks = group->CandidateTasks;
        auto& delayedTasks = group->DelayedTasks;

        // Move tasks from delayed to candidates.
        while (!delayedTasks.empty()) {
            auto it = delayedTasks.begin();
            auto deadline = it->first;
            if (now < deadline) {
                break;
            }
            auto task = it->second;
            delayedTasks.erase(it);
            if (task->GetPendingJobCount() == 0) {
                LOG_DEBUG("Task pending hint removed (Task: %v)",
                    task->GetId());
                YCHECK(nonLocalTasks.erase(task) == 1);
                UpdateTask(task);
            } else {
                LOG_DEBUG("Task delay deadline reached (Task: %v)", task->GetId());
                MoveTaskToCandidates(task, candidateTasks);
            }
        }

        // Consider candidates in the order of increasing memory demand.
        {
            int processedTaskCount = 0;
            auto it = candidateTasks.begin();
            while (it != candidateTasks.end()) {
                ++processedTaskCount;
                auto task = it->second;

                // Make sure that the task is ready to launch jobs.
                // Remove pending hint if not.
                if (task->GetPendingJobCount() == 0) {
                    LOG_DEBUG("Task pending hint removed (Task: %v)", task->GetId());
                    candidateTasks.erase(it++);
                    YCHECK(nonLocalTasks.erase(task) == 1);
                    UpdateTask(task);
                    continue;
                }

                // Check min memory demand for early exit.
                if (task->GetMinNeededResources().memory() > jobLimits.memory()) {
                    break;
                }

                if (!CheckJobLimits(task, jobLimits, nodeResourceLimits)) {
                    ++it;
                    continue;
                }

                if (!task->GetDelayedTime()) {
                    task->SetDelayedTime(now);
                }

                auto deadline = *task->GetDelayedTime() + task->GetLocalityTimeout();
                if (deadline > now) {
                    LOG_DEBUG("Task delayed (Task: %v, Deadline: %v)",
                        task->GetId(),
                        deadline);
                    delayedTasks.insert(std::make_pair(deadline, task));
                    candidateTasks.erase(it++);
                    continue;
                }

                if (!Running) {
                    return NullJobId;
                }

                LOG_DEBUG(
                    "Attempting to schedule a non-local job (Task: %v, Address: %v, JobLimits: {%v}, "
                    "PendingDataSize: %v, PendingJobCount: %v)",
                    task->GetId(),
                    address,
                    FormatResources(jobLimits),
                    task->GetPendingDataSize(),
                    task->GetPendingJobCount());

                auto jobId = task->ScheduleJob(context, jobLimits);
                if (jobId) {
                    UpdateTask(task);
                    LOG_DEBUG("Processed %v tasks", processedTaskCount);
                    return jobId;
                }

                // If task failed to schedule job, its min resources might have been updated.
                auto minMemory = task->GetMinNeededResources().memory();
                if (it->first == minMemory) {
                    ++it;
                } else {
                    it = candidateTasks.erase(it);
                    candidateTasks.insert(std::make_pair(minMemory, task));
                }
            }

            LOG_DEBUG("Processed %v tasks", processedTaskCount);
        }
    }
    return NullJobId;
}

TCancelableContextPtr TOperationControllerBase::GetCancelableContext() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CancelableContext;
}

IInvokerPtr TOperationControllerBase::GetCancelableControlInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CancelableControlInvoker;
}

IInvokerPtr TOperationControllerBase::GetCancelableBackgroundInvoker() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CancelableBackgroundInvoker;
}

int TOperationControllerBase::GetPendingJobCount() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Avoid accessing the state while not prepared.
    if (!Prepared) {
        return 0;
    }

    // NB: For suspended operations we still report proper pending job count
    // but zero demand.
    if (Operation->GetState() != EOperationState::Running) {
        return 0;
    }

    return CachedPendingJobCount;
}

int TOperationControllerBase::GetTotalJobCount() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Avoid accessing the state while not prepared.
    if (!Prepared) {
        return 0;
    }

    return JobCounter.GetTotal();
}

TNodeResources TOperationControllerBase::GetNeededResources() const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (Operation->GetState() != EOperationState::Running) {
        return ZeroNodeResources();
    }

    return CachedNeededResources;
}

void TOperationControllerBase::OnOperationCompleted()
{
    VERIFY_THREAD_AFFINITY_ANY();

    CancelableControlInvoker->Invoke(BIND(&TThis::DoOperationCompleted, MakeStrong(this)));
}

void TOperationControllerBase::DoOperationCompleted()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Operation completed");

    Running = false;

    Host->OnOperationCompleted(Operation);
}

void TOperationControllerBase::OnOperationFailed(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    CancelableControlInvoker->Invoke(BIND(&TThis::DoOperationFailed, MakeStrong(this), error));
}

void TOperationControllerBase::DoOperationFailed(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Running = false;

    Host->OnOperationFailed(Operation, error);
}

void TOperationControllerBase::CreateLivePreviewTables()
{
    // NB: use root credentials.
    auto channel = Host->GetMasterClient()->GetMasterChannel(EMasterChannelKind::Leader);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    auto addRequest = [&] (
            const Stroka& path,
            int replicationFactor,
            const Stroka& key,
            const TYsonString& acl)
    {
        {
            auto req = TCypressYPathProxy::Create(path);

            req->set_type(static_cast<int>(EObjectType::Table));
            req->set_ignore_existing(true);

            auto attributes = CreateEphemeralAttributes();
            attributes->Set("replication_factor", replicationFactor);

            ToProto(req->mutable_node_attributes(), *attributes);

            batchReq->AddRequest(req, key);
        }

        {
            auto req = TYPathProxy::Set(path + "/@acl");
            req->set_value(acl.Data());

            batchReq->AddRequest(req, key);
        }

        {
            auto req = TYPathProxy::Set(path + "/@inherit_acl");
            req->set_value(ConvertToYsonString(false).Data());

            batchReq->AddRequest(req, key);
        }
    };

    if (IsOutputLivePreviewSupported()) {
        LOG_INFO("Creating output tables for live preview");

        for (int index = 0; index < OutputTables.size(); ++index) {
            const auto& table = OutputTables[index];
            auto path = GetLivePreviewOutputPath(OperationId, index);
            addRequest(path, table.Options->ReplicationFactor, "create_output", OutputTables[index].EffectiveAcl);
        }
    }

    if (IsIntermediateLivePreviewSupported()) {
        LOG_INFO("Creating intermediate table for live preview");

        auto path = GetLivePreviewIntermediatePath(OperationId);
        addRequest(path, 1, "create_intermediate", ConvertToYsonString(Spec->IntermediateDataAcl));
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error creating live preview tables");
    const auto& batchRsp = batchRspOrError.Value();

    auto handleResponse = [&] (TLivePreviewTableBase& table, TCypressYPathProxy::TRspCreatePtr rsp) {
        table.LivePreviewTableId = FromProto<NCypressClient::TNodeId>(rsp->node_id());
    };

    if (IsOutputLivePreviewSupported()) {
        auto rspsOrError = batchRsp->GetResponses<TCypressYPathProxy::TRspCreate>("create_output");
        YCHECK(rspsOrError.size() == 3 * OutputTables.size());
        for (int index = 0; index < OutputTables.size(); ++index) {
            handleResponse(OutputTables[index], rspsOrError[3 * index].Value());
        }

        LOG_INFO("Output live preview tables created");
    }

    if (IsIntermediateLivePreviewSupported()) {
        auto rspsOrError = batchRsp->GetResponses<TCypressYPathProxy::TRspCreate>("create_intermediate");
        handleResponse(IntermediateTable, rspsOrError[0].Value());

        LOG_INFO("Intermediate live preview table created");
    }
}

void TOperationControllerBase::PrepareLivePreviewTablesForUpdate()
{
    // XXX(babenko): fixme
    //// NB: use root credentials.
    //auto channel = Host->GetMasterClient()->GetMasterChannel(EMasterChannelKind::Leader);
    //TObjectServiceProxy proxy(channel);
    //
    //auto batchReq = proxy.ExecuteBatch();
    //
    //// XXX(babenko): multicell
    //auto addRequest = [&] (const TLivePreviewTableBase& table, const Stroka& key) {
    //    auto req = TTableYPathProxy::PrepareForUpdate(FromObjectId(table.LivePreviewTableId));
    //    req->set_update_mode(static_cast<int>(EUpdateMode::Overwrite));
    //    req->set_lock_mode(static_cast<int>(ELockMode::Exclusive));
    //    SetTransactionId(req, Operation->GetAsyncSchedulerTransaction());
    //    batchReq->AddRequest(req, key);
    //};
    //
    //if (IsOutputLivePreviewSupported()) {
    //    LOG_INFO("Preparing live preview output tables for update");
    //
    //    for (const auto& table : OutputTables) {
    //        addRequest(table, "prepare_output");
    //    }
    //}
    //
    //if (IsIntermediateLivePreviewSupported()) {
    //    LOG_INFO("Preparing live preview intermediate table for update");
    //
    //    addRequest(IntermediateTable, "prepare_intermediate");
    //}
    //
    //auto batchRspOrError = WaitFor(batchReq->Invoke());
    //THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error preparing live preview tables for update");
    //const auto& batchRsp = batchRspOrError.Value();
    //
    //auto handleResponse = [&] (TLivePreviewTableBase& table, TTableYPathProxy::TRspPrepareForUpdatePtr rsp) {
    //    table.LivePreviewChunkListId = FromProto<NCypressClient::TNodeId>(rsp->chunk_list_id());
    //};
    //
    //if (IsOutputLivePreviewSupported()) {
    //    auto rspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspPrepareForUpdate>("prepare_output");
    //    YCHECK(rspsOrError.size() == OutputTables.size());
    //    for (int index = 0; index < OutputTables.size(); ++index) {
    //        handleResponse(OutputTables[index], rspsOrError[index].Value());
    //    }
    //
    //    LOG_INFO("Output live preview tables prepared for update");
    //}
    //
    //if (IsIntermediateLivePreviewSupported()) {
    //    auto rspOrError = batchRsp->GetResponse<TTableYPathProxy::TRspPrepareForUpdate>("prepare_intermediate");
    //    handleResponse(IntermediateTable, rspOrError.Value());
    //
    //    LOG_INFO("Intermediate live preview table prepared for update");
    //}
}

void TOperationControllerBase::GetInputTablesBasicAttributes()
{
    LOG_INFO("Getting basic attributes of input tables");

    auto channel = AuthenticatedInputMasterClient->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    for (const auto& table : InputTables) {
        auto req = TTableYPathProxy::GetBasicAttributes(table.Path.GetPath());
        req->set_permissions(static_cast<ui32>(EPermission::Read));
        SetTransactionId(req, Operation->GetInputTransaction());
        batchReq->AddRequest(req, "get_basic_attributes");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error getting basic attributes of input tables");
    const auto& batchRsp = batchRspOrError.Value();

    auto rspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspGetBasicAttributes>("get_basic_attributes");
    for (int index = 0; index < InputTables.size(); ++index) {
        auto& table = InputTables[index];
        auto path = table.Path.GetPath();

        {
            const auto& rspOrError = rspsOrError[index];
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting basic attributes of input table %v",
                path);
            const auto& rsp = rspOrError.Value();

            table.ObjectId = FromProto<TObjectId>(rsp->object_id());
            table.CellTag = rsp->cell_tag();

            auto type = TypeFromId(table.ObjectId);
            if (type != EObjectType::Table) {
                THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv, actual %Qlv",
                    table.Path.GetPath(),
                    EObjectType::Table,
                    type);
            }

            LOG_INFO("Basic attributes of input table received (Path: %v, ObjectId: %v, CellTag: %v)",
                path,
                table.ObjectId,
                table.CellTag);
        }
    }
}

void TOperationControllerBase::GetOutputTablesBasicAttributes()
{
    LOG_INFO("Getting basic attributes of output tables");

    auto channel = AuthenticatedOutputMasterClient->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    for (const auto& table : OutputTables) {
        auto req = TTableYPathProxy::GetBasicAttributes(table.Path.GetPath());
        req->set_permissions(static_cast<ui32>(EPermission::Write));
        SetTransactionId(req, Operation->GetOutputTransaction());
        batchReq->AddRequest(req, "get_basic_attributes");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error getting basic attributes of output tables");
    const auto& batchRsp = batchRspOrError.Value();

    auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_basic_attributes");
    for (int index = 0; index < OutputTables.size(); ++index) {
        auto& table = OutputTables[index];
        const auto& path = table.Path.GetPath();
        {
            const auto& rspOrError = rspsOrError[index];
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting basic attributes of output table %v",
                path);
            const auto& rsp = rspOrError.Value();

            table.ObjectId = FromProto<TObjectId>(rsp->object_id());
            table.CellTag = rsp->cell_tag();

            auto type = TypeFromId(table.ObjectId);
            if (type != EObjectType::Table) {
                THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv, actual %Qlv",
                    table.Path.GetPath(),
                    EObjectType::Table,
                    type);
            }

            LOG_INFO("Basic attributes of output table received (Path: %v, ObjectId: %v, CellTag: %v)",
                path,
                table.ObjectId,
                table.CellTag);
        }
    }
}

void TOperationControllerBase::GetFilesBasicAttributes(std::vector<TUserFile>* files)
{
    LOG_INFO("Getting basic attributes of files");

    auto channel = AuthenticatedOutputMasterClient->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    for (const auto& file : *files) {
        auto req = TObjectYPathProxy::GetBasicAttributes(file.Path.GetPath());
        req->set_permissions(static_cast<ui32>(EPermission::Read));
        SetTransactionId(req, Operation->GetInputTransaction());
        batchReq->AddRequest(req, "get_basic_attributes");
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error getting basic attributes of files");
    const auto& batchRsp = batchRspOrError.Value();

    auto rspsOrError = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_basic_attributes");
    for (int index = 0; index < files->size(); ++index) {
        auto& file = (*files)[index];
        const auto& path = file.Path.GetPath();
        const auto& rspOrError = rspsOrError[index];
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting basic attributes of file %v",
            path);
        const auto& rsp = rspOrError.Value();

        file.ObjectId = FromProto<TObjectId>(rsp->object_id());
        file.CellTag = rsp->cell_tag();

        file.Type = TypeFromId(file.ObjectId);
        if (file.Type != EObjectType::File && file.Type != EObjectType::Table) {
            THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv or %Qlv, actual %Qlv",
                path,
                EObjectType::File,
                EObjectType::Table,
                file.Type);
        }
    }
}

void TOperationControllerBase::FetchInputTables()
{
    for (auto& table : InputTables) {
        auto objectIdPath = FromObjectId(table.ObjectId);
        const auto& path = table.Path.GetPath();
        const auto& ranges = table.Path.GetRanges();
        if (ranges.empty())
            continue;

        LOG_INFO("Fetching input table (Path: %v, RangeCount: %v)",
            path,
            ranges.size());

        auto channel = AuthenticatedInputMasterClient->GetMasterChannel(EMasterChannelKind::LeaderOrFollower, table.CellTag);
        TObjectServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();

        for (const auto& range : ranges) {
            for (i64 index = 0; index * Config->MaxChunkCountPerFetch < table.ChunkCount; ++index) {
                auto adjustedRange = range;
                auto chunkCountLowerLimit = index * Config->MaxChunkCountPerFetch;
                if (adjustedRange.LowerLimit().HasChunkIndex()) {
                    chunkCountLowerLimit = std::max(chunkCountLowerLimit, adjustedRange.LowerLimit().GetChunkIndex());
                }
                adjustedRange.LowerLimit().SetChunkIndex(chunkCountLowerLimit);

                auto chunkCountUpperLimit = (index + 1) * Config->MaxChunkCountPerFetch;
                if (adjustedRange.UpperLimit().HasChunkIndex()) {
                    chunkCountUpperLimit = std::min(chunkCountUpperLimit, adjustedRange.UpperLimit().GetChunkIndex());
                }
                adjustedRange.UpperLimit().SetChunkIndex(chunkCountUpperLimit);

                auto req = TTableYPathProxy::Fetch(FromObjectId(table.ObjectId));
                InitializeFetchRequest(req.Get(), table.Path);
                ToProto(req->mutable_ranges(), std::vector<TReadRange>({adjustedRange}));
                req->set_fetch_all_meta_extensions(true);
                req->set_fetch_parity_replicas(IsParityReplicasFetchEnabled());
                SetTransactionId(req, Operation->GetInputTransaction());
                batchReq->AddRequest(req, "fetch");
            }
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error fetching input table %v",
            path);
        const auto& batchRsp = batchRspOrError.Value();

        auto rspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspFetch>("fetch");
        for (const auto& rspOrError : rspsOrError) {
            const auto& rsp = rspOrError.Value();
            table.Chunks = ProcessFetchResponse(
                AuthenticatedInputMasterClient,
                rsp,
                table.CellTag,
                InputNodeDirectory,
                Config->MaxChunksPerLocateRequest,
                Logger);
        }

        LOG_INFO("Input table fetched (Path: %v, ChunkCount: %v)",
            path,
            table.Chunks.size());
    }
}

void TOperationControllerBase::LockInputTables()
{
    LOG_INFO("Locking input tables");

    auto channel = AuthenticatedInputMasterClient->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    for (const auto& table : InputTables) {
        auto objectIdPath = FromObjectId(table.ObjectId);
        {
            auto req = TTableYPathProxy::Lock(objectIdPath);
            req->set_mode(static_cast<int>(ELockMode::Snapshot));
            SetTransactionId(req, Operation->GetInputTransaction());
            GenerateMutationId(req);
            batchReq->AddRequest(req, "lock");
        }
        {
            auto req = TTableYPathProxy::Get(objectIdPath);
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("sorted");
            attributeFilter.Keys.push_back("sorted_by");
            attributeFilter.Keys.push_back("chunk_count");
            ToProto(req->mutable_attribute_filter(), attributeFilter);
            SetTransactionId(req, Operation->GetInputTransaction());
            batchReq->AddRequest(req, "get_attributes");
        }
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error locking input tables");
    const auto& batchRsp = batchRspOrError.Value();

    {
        auto lockInRspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspLock>("lock");
        auto getInAttributesRspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspGet>("get_attributes");
        for (int index = 0; index < InputTables.size(); ++index) {
            auto& table = InputTables[index];
            auto path = table.Path.GetPath();
            {
                const auto& rspOrError = lockInRspsOrError[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error locking input table %v",
                    path);
            }
            {
                const auto& rspOrError = getInAttributesRspsOrError[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting attributes for input table %v",
                    path);

                const auto& rsp = rspOrError.Value();
                auto node = ConvertToNode(TYsonString(rsp->value()));
                const auto& attributes = node->Attributes();

                if (attributes.Get<bool>("sorted")) {
                    table.SortedBy = attributes.Get<TKeyColumns>("sorted_by");
                }

                table.ChunkCount = attributes.Get<int>("chunk_count");
            }
            LOG_INFO("Input table locked (Path: %v, SortedBy: %v, ChunkCount: %v)",
                path,
                table.SortedBy ? MakeNullable("[" + JoinToString(*table.SortedBy) + "]") : Null,
                table.ChunkCount);
        }
    }
}

void TOperationControllerBase::BeginUploadOutputTables()
{
    LOG_INFO("Locking output tables");

    auto channel = AuthenticatedOutputMasterClient->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();
    for (const auto& table : OutputTables) {
        auto objectIdPath = FromObjectId(table.ObjectId);
        {
            auto req = TTableYPathProxy::Lock(objectIdPath);
            req->set_mode(static_cast<int>(table.LockMode));
            GenerateMutationId(req);
            SetTransactionId(req, Operation->GetOutputTransaction());
            batchReq->AddRequest(req, "lock");
        }
        {
            auto req = TTableYPathProxy::Get(objectIdPath);
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("channels");
            attributeFilter.Keys.push_back("compression_codec");
            attributeFilter.Keys.push_back("erasure_codec");
            attributeFilter.Keys.push_back("row_count");
            attributeFilter.Keys.push_back("replication_factor");
            attributeFilter.Keys.push_back("account");
            attributeFilter.Keys.push_back("vital");
            attributeFilter.Keys.push_back("effective_acl");
            ToProto(req->mutable_attribute_filter(), attributeFilter);
            SetTransactionId(req, Operation->GetOutputTransaction());
            batchReq->AddRequest(req, "get_attributes");
        }
        {
            auto req = TTableYPathProxy::BeginUpload(objectIdPath);
            SetTransactionId(req, Operation->GetOutputTransaction());
            GenerateMutationId(req);
            req->set_update_mode(static_cast<int>(table.UpdateMode));
            req->set_lock_mode(static_cast<int>(table.LockMode));
            batchReq->AddRequest(req, "begin_upload");
        }
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error locking output tables");
    const auto& batchRsp = batchRspOrError.Value();

    auto lockOutRsps = batchRsp->GetResponses<TTableYPathProxy::TRspLock>("lock");
    auto getOutAttributesRspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspGet>("get_attributes");
    auto beginUploadRspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspBeginUpload>("begin_upload");
    for (int index = 0; index < OutputTables.size(); ++index) {
        auto& table = OutputTables[index];
        const auto& path = table.Path.GetPath();
        {
            const auto& rspOrError = lockOutRsps[index];
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error locking output table %v",
                path);
        }
        {
            const auto& rspOrError = getOutAttributesRspsOrError[index];
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting attributes of output table %v",
                path);

            const auto& rsp = rspOrError.Value();
            auto node = ConvertToNode(TYsonString(rsp->value()));
            const auto& attributes = node->Attributes();

            if (attributes.Get<i64>("row_count") > 0 &&
                table.AppendRequested &&
                table.UpdateMode == EUpdateMode::Overwrite) {
                THROW_ERROR_EXCEPTION("Cannot append sorted data to non-empty output table %v",
                    path);
            }

            table.Options->Channels = attributes.Get<NChunkClient::TChannels>("channels", TChannels());
            table.Options->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
            table.Options->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
            table.Options->ReplicationFactor = attributes.Get<int>("replication_factor");
            table.Options->Account = attributes.Get<Stroka>("account");
            table.Options->ChunksVital = attributes.Get<bool>("vital");

            table.EffectiveAcl = attributes.GetYson("effective_acl");
        }
        {
            const auto& rspOrError = beginUploadRspsOrError[index];
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error starting upload to output table %v",
                path);

            const auto& rsp = rspOrError.Value();
            table.UploadTransactionId = FromProto<TTransactionId>(rsp->upload_transaction_id());
        }
        LOG_INFO("Output table locked (Path: %v, Options: %v, UploadTransactionId: %v)",
            path,
            ConvertToYsonString(table.Options, EYsonFormat::Text).Data(),
            table.UploadTransactionId);
    }
}

void TOperationControllerBase::GetOutputTablesUploadParams()
{
    yhash<TCellTag, std::vector<TOutputTable*>> cellTagToTables;
    for (auto& table : OutputTables) {
        cellTagToTables[table.CellTag].push_back(&table);
    }

    for (const auto& pair : cellTagToTables) {
        auto cellTag = pair.first;
        const auto& tables = pair.second;

        LOG_INFO("Getting output tables upload parameters (CellTag: %v)", cellTag);

        auto channel = AuthenticatedOutputMasterClient->GetMasterChannel(EMasterChannelKind::LeaderOrFollower, cellTag);
        TObjectServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();
        for (const auto& table : tables) {
            auto objectIdPath = FromObjectId(table->ObjectId);
            {
                auto req = TTableYPathProxy::GetUploadParams(objectIdPath);
                SetTransactionId(req, table->UploadTransactionId);
                batchReq->AddRequest(req, "get_upload_params");
            }
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error getting upload parameters of output tables");
        const auto& batchRsp = batchRspOrError.Value();

        auto getUploadParamsRspsOrError = batchRsp->GetResponses<TTableYPathProxy::TRspGetUploadParams>("get_upload_params");
        for (int index = 0; index < tables.size(); ++index) {
            auto* table = tables[index];
            const auto& path = table->Path.GetPath();
            {
                const auto& rspOrError = getUploadParamsRspsOrError[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting upload parameters of output table %v",
                    path);

                const auto& rsp = rspOrError.Value();
                table->OutputChunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());

                LOG_INFO("Upload parameters of output table received (Path: %v, ChunkListId: %v)",
                    path,
                    table->OutputChunkListId);
            }
        }
    }
}

void TOperationControllerBase::FetchUserFiles(std::vector<TUserFile>* files)
{
    for (auto& file : *files) {
        auto objectIdPath = FromObjectId(file.ObjectId);
        const auto& path = file.Path.GetPath();

        LOG_INFO("Fetching user file (Path: %v)",
            path);

        auto channel = AuthenticatedInputMasterClient->GetMasterChannel(EMasterChannelKind::LeaderOrFollower, file.CellTag);
        TObjectServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TChunkOwnerYPathProxy::Fetch(objectIdPath);
            ToProto(req->mutable_ranges(), std::vector<TReadRange>({TReadRange()}));
            switch (file.Type) {
                case EObjectType::Table:
                    req->set_fetch_all_meta_extensions(true);
                    InitializeFetchRequest(req.Get(), file.Path);
                    break;

                case EObjectType::File:
                    req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
                    break;

                default:
                    YUNREACHABLE();
            }
            SetTransactionId(req, Operation->GetInputTransaction());
            batchReq->AddRequest(req, "fetch");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error fetching user file %v",
             path);
        const auto& batchRsp = batchRspOrError.Value();

        {
            auto rsp = batchRsp->GetResponse<TChunkOwnerYPathProxy::TRspFetch>("fetch").Value();
            file.ChunkSpecs = ProcessFetchResponse(
                AuthenticatedInputMasterClient,
                rsp,
                file.CellTag,
                AuxNodeDirectory,
                Config->MaxChunksPerLocateRequest,
                Logger);
        }

        LOG_INFO("User file fetched (Path: %v, FileName: %v)",
            path,
            file.FileName);
    }
}

void TOperationControllerBase::LockUserFiles(
    std::vector<TUserFile>* files,
    const std::vector<Stroka>& attributeKeys)
{
    LOG_INFO("Locking user files");

    auto channel = AuthenticatedOutputMasterClient->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
    TObjectServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();

    for (const auto& file : *files) {
        auto objectIdPath = FromObjectId(file.ObjectId);
        const auto& path = file.Path.GetPath();

        {
            auto req = TCypressYPathProxy::Lock(objectIdPath);
            req->set_mode(static_cast<int>(ELockMode::Snapshot));
            GenerateMutationId(req);
            SetTransactionId(req, Operation->GetInputTransaction());
            batchReq->AddRequest(req, "lock");
        }
        {
            auto req = TYPathProxy::Get(path);
            SetTransactionId(req, Operation->GetInputTransaction());
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            if (file.Type == EObjectType::File) {
                attributeFilter.Keys.push_back("executable");
                attributeFilter.Keys.push_back("file_name");
            }
            attributeFilter.Keys.push_back("key");
            attributeFilter.Keys.push_back("chunk_count");
            attributeFilter.Keys.push_back("uncompressed_data_size");
            attributeFilter.Keys.insert(attributeFilter.Keys.end(), attributeKeys.begin(), attributeKeys.end());
            ToProto(req->mutable_attribute_filter(), attributeFilter);
            batchReq->AddRequest(req, "get_attributes");
        }
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error locking user files");
    const auto& batchRsp = batchRspOrError.Value();

    TEnumIndexedVector<yhash_set<Stroka>, EOperationStage> userFileNames;
    auto validateUserFileName = [&] (const TUserFile& file) {
        // TODO(babenko): more sanity checks?
        auto path = file.Path.GetPath();
        const auto& fileName = file.FileName;
        if (fileName.empty()) {
            THROW_ERROR_EXCEPTION("Empty user file name for %v",
                path);
        }
        if (!userFileNames[file.Stage].insert(fileName).second) {
            THROW_ERROR_EXCEPTION("Duplicate user file name %Qv for %v",
                fileName,
                path);
        }
    };

    {
        auto getAttributesRspsOrError = batchRsp->GetResponses<TYPathProxy::TRspGetKey>("get_attributes");
        for (int index = 0; index < files->size(); ++index) {
            auto& file = (*files)[index];
            const auto& path = file.Path.GetPath();

            {
                const auto& rsp = getAttributesRspsOrError[index].Value();

                auto node = ConvertToNode(TYsonString(rsp->value()));
                file.Attributes = node->Attributes().Clone();
                const auto& attributes = *file.Attributes;

                file.FileName = attributes.Get<Stroka>("key");
                file.FileName = attributes.Get<Stroka>("file_name", file.FileName);
                file.FileName = file.Path.GetFileName().Get(file.FileName);

                switch (file.Type) {
                    case EObjectType::File:
                        file.Executable = attributes.Get<bool>("executable", false);
                        break;

                    case EObjectType::Table:
                        file.Format = file.Path.Attributes().GetYson("format");
                        break;

                    default:
                        YUNREACHABLE();
                }

                i64 fileSize = attributes.Get<i64>("uncompressed_data_size");
                if (fileSize > Config->MaxFileSize) {
                    THROW_ERROR_EXCEPTION(
                        "User file %v exceeds size limit: %v > %v",
                        path,
                        fileSize,
                        Config->MaxFileSize);
                }

                i64 chunkCount = attributes.Get<i64>("chunk_count");
                if (chunkCount > Config->MaxChunkCountPerFetch) {
                    THROW_ERROR_EXCEPTION(
                        "User file %v exceeds chunk count limit: %v > %v",
                        path,
                        chunkCount,
                        Config->MaxChunkCountPerFetch);
                }

                LOG_INFO("User file locked (Path: %v, Stage: %v, FileName: %v)",
                    path,
                    file.Stage,
                    file.FileName);
            }

            validateUserFileName(file);
        }
    }
}

void TOperationControllerBase::InitQuerySpec(
    NProto::TSchedulerJobSpecExt* schedulerJobSpecExt,
    const Stroka& queryString,
    const TTableSchema& schema)
{
    auto* querySpec = schedulerJobSpecExt->mutable_input_query_spec();
    auto ast = PrepareJobQueryAst(queryString);
    auto registry = CreateBuiltinFunctionRegistry();
    auto externalFunctions = GetExternalFunctions(ast, registry);

    std::vector<TUserFile> udfFiles;
    std::vector<TUdfDescriptorPtr> udfDescriptors;

    if (!externalFunctions.empty()) {
        if (!Config->UdfRegistryPath) {
            THROW_ERROR_EXCEPTION("External UDF registry is not configured");
        }

        for (const auto& function : externalFunctions) {
            LOG_INFO("Requesting UDF descriptor (Function: %v)", function);
            TUserFile file;
            file.Path = GetUdfDescriptorPath(*Config->UdfRegistryPath, function);
            udfFiles.push_back(file);
        }

        GetFilesBasicAttributes(&udfFiles);

        LockUserFiles(
            &udfFiles,
            {
                FunctionDescriptorAttribute,
                AggregateDescriptorAttribute
            });

        FetchUserFiles(&udfFiles);

        for (const auto& file : udfFiles) {
            if (file.Type != EObjectType::File) {
                THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv, actual %Qlv",
                    file.Path,
                    EObjectType::File,
                    file.Type);
            }
            auto descriptor = New<TUdfDescriptor>();
            descriptor->Name = file.FileName;
            descriptor->FunctionDescriptor = file.Attributes->Find<TCypressFunctionDescriptorPtr>(FunctionDescriptorAttribute);
            descriptor->AggregateDescriptor = file.Attributes->Find<TCypressAggregateDescriptorPtr>(AggregateDescriptorAttribute);
            udfDescriptors.push_back(std::move(descriptor));
        }

        registry = CreateJobFunctionRegistry(udfDescriptors, Null, std::move(registry));
    }

    auto query = PrepareJobQuery(queryString, std::move(ast), schema, registry);
    ToProto(querySpec->mutable_query(), query);

    for (const auto& descriptor : udfDescriptors) {
        auto* protoDescriptor = querySpec->add_udf_descriptors();
        ToProto(protoDescriptor, ConvertToYsonString(descriptor).Data());
    }

    for (const auto& file : udfFiles) {
        auto* protoDescriptor = querySpec->add_udf_files();
        protoDescriptor->set_type(static_cast<int>(file.Type));
        protoDescriptor->set_file_name(file.FileName);
        ToProto(protoDescriptor->mutable_chunks(), file.ChunkSpecs);
    }
}

void TOperationControllerBase::CollectTotals()
{
    for (const auto& table : InputTables) {
        for (const auto& chunk : table.Chunks) {
            i64 chunkDataSize;
            i64 chunkRowCount;
            i64 chunkValueCount;
            i64 chunkCompressedDataSize;
            NChunkClient::GetStatistics(chunk, &chunkDataSize, &chunkRowCount, &chunkValueCount, &chunkCompressedDataSize);

            TotalEstimatedInputDataSize += chunkDataSize;
            TotalEstimatedInputRowCount += chunkRowCount;
            TotalEstimatedInputValueCount += chunkValueCount;
            TotalEstimatedCompressedDataSize += chunkCompressedDataSize;
            ++TotalEstimatedInputChunkCount;
        }
    }

    LOG_INFO("Estimated input totals collected (ChunkCount: %v, DataSize: %v, RowCount: %v, ValueCount: %v, CompressedDataSize: %v)",
        TotalEstimatedInputChunkCount,
        TotalEstimatedInputDataSize,
        TotalEstimatedInputRowCount,
        TotalEstimatedInputValueCount,
        TotalEstimatedCompressedDataSize);
}

void TOperationControllerBase::CustomPrepare()
{ }

// NB: must preserve order of chunks in the input tables, no shuffling.
std::vector<TRefCountedChunkSpecPtr> TOperationControllerBase::CollectInputChunks() const
{
    std::vector<TRefCountedChunkSpecPtr> result;
    for (int tableIndex = 0; tableIndex < InputTables.size(); ++tableIndex) {
        const auto& table = InputTables[tableIndex];
        for (const auto& chunkSpec : table.Chunks) {
            auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
            if (IsUnavailable(chunkSpec, NeedsAllChunkParts())) {
                switch (Spec->UnavailableChunkStrategy) {
                    case EUnavailableChunkAction::Fail:
                        THROW_ERROR_EXCEPTION("Input chunk %v is unavailable",
                            chunkId);

                    case EUnavailableChunkAction::Skip:
                        LOG_TRACE("Skipping unavailable chunk (ChunkId: %v)",
                            chunkId);
                        continue;

                    case EUnavailableChunkAction::Wait:
                        // Do nothing.
                        break;

                    default:
                        YUNREACHABLE();
                }
            }
            auto chunk = New<TRefCountedChunkSpec>(chunkSpec);
            chunk->set_table_index(tableIndex);
            result.push_back(chunk);
        }
    }
    return result;
}

std::vector<TChunkStripePtr> TOperationControllerBase::SliceChunks(
    const std::vector<TRefCountedChunkSpecPtr>& chunkSpecs,
    i64 maxSliceDataSize,
    int* jobCount)
{
    std::vector<TChunkStripePtr> result;
    auto appendStripes = [&] (std::vector<TChunkSlicePtr> slices) {
        for (const auto& slice : slices) {
            result.push_back(New<TChunkStripe>(slice));
        }
    };

    // TODO(ignat): we slice on two parts even if TotalEstimatedInputDataSize very small.
    i64 sliceDataSize = std::min(
        maxSliceDataSize,
        (i64)std::max(Config->SliceDataSizeMultiplier * TotalEstimatedInputDataSize / *jobCount, 1.0));

    for (const auto& chunkSpec : chunkSpecs) {
        int oldSize = result.size();

        bool hasNontrivialLimits = !IsCompleteChunk(*chunkSpec);

        auto codecId = NErasure::ECodec(chunkSpec->erasure_codec());
        if (hasNontrivialLimits || codecId == NErasure::ECodec::None) {
            auto slices = SliceChunkByRowIndexes(chunkSpec, sliceDataSize);
            appendStripes(slices);
        } else {
            for (const auto& slice : CreateErasureChunkSlices(chunkSpec, codecId)) {
                auto slices = slice->SliceEvenly(sliceDataSize);
                appendStripes(slices);
            }
        }

        LOG_TRACE("Slicing chunk (ChunkId: %v, SliceCount: %v)",
            FromProto<TChunkId>(chunkSpec->chunk_id()),
            result.size() - oldSize);
    }

    *jobCount = std::min(*jobCount, static_cast<int>(result.size()));
    if (!result.empty()) {
        *jobCount = std::max(*jobCount, 1 + (static_cast<int>(result.size()) - 1) / Config->MaxChunkStripesPerJob);
    }

    return result;
}

std::vector<TChunkStripePtr> TOperationControllerBase::SliceInputChunks(
    i64 maxSliceDataSize,
    int* jobCount)
{
    return SliceChunks(CollectInputChunks(), maxSliceDataSize, jobCount);
}


TKeyColumns TOperationControllerBase::CheckInputTablesSorted(const TNullable<TKeyColumns>& keyColumns)
{
    YCHECK(!InputTables.empty());

    for (const auto& table : InputTables) {
        if (!table.SortedBy) {
            THROW_ERROR_EXCEPTION("Input table %v is not sorted",
                table.Path.GetPath());
        }
    }

    if (keyColumns) {
        for (const auto& table : InputTables) {
            if (!CheckKeyColumnsCompatible(*table.SortedBy, *keyColumns)) {
                THROW_ERROR_EXCEPTION("Input table %v is sorted by columns [%v] that are not compatible with the requested columns [%v]",
                    table.Path.GetPath(),
                    JoinToString(*table.SortedBy),
                    JoinToString(*keyColumns));
            }
        }
        return *keyColumns;
    } else {
        const auto& referenceTable = InputTables[0];
        for (const auto& table : InputTables) {
            if (table.SortedBy != referenceTable.SortedBy) {
                THROW_ERROR_EXCEPTION("Key columns do not match: input table %v is sorted by [%v] while input table %v is sorted by [%v]",
                    table.Path.GetPath(),
                    JoinToString(*table.SortedBy),
                    referenceTable.Path.GetPath(),
                    JoinToString(*referenceTable.SortedBy));
            }
        }
        return *referenceTable.SortedBy;
    }
}

bool TOperationControllerBase::CheckKeyColumnsCompatible(
    const TKeyColumns& fullColumns,
    const TKeyColumns& prefixColumns)
{
    if (fullColumns.size() < prefixColumns.size()) {
        return false;
    }

    for (int index = 0; index < prefixColumns.size(); ++index) {
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

bool TOperationControllerBase::IsParityReplicasFetchEnabled() const
{
    return false;
}

void TOperationControllerBase::UpdateAllTasksIfNeeded(const TProgressCounter& jobCounter)
{
    if (jobCounter.GetAborted(EAbortReason::ResourceOverdraft) == Config->MaxMemoryReserveAbortJobCount) {
        UpdateAllTasks();
    }
}

bool TOperationControllerBase::IsMemoryReserveEnabled(const TProgressCounter& jobCounter) const
{
    return jobCounter.GetAborted(EAbortReason::ResourceOverdraft) < Config->MaxMemoryReserveAbortJobCount;
}

i64 TOperationControllerBase::GetMemoryReserve(bool memoryReserveEnabled, TUserJobSpecPtr userJobSpec) const
{
    if (memoryReserveEnabled) {
        return static_cast<i64>(userJobSpec->MemoryLimit * userJobSpec->MemoryReserveFactor);
    } else {
        return userJobSpec->MemoryLimit;
    }
}

void TOperationControllerBase::RegisterOutput(
    const TChunkTreeId& chunkTreeId,
    int key,
    int tableIndex,
    TOutputTable& table)
{
    table.OutputChunkTreeIds.insert(std::make_pair(key, chunkTreeId));

    if (IsOutputLivePreviewSupported()) {
        auto masterConnector = Host->GetMasterConnector();
        masterConnector->AttachToLivePreview(
            Operation,
            table.LivePreviewChunkListId,
            chunkTreeId);
    }

    LOG_DEBUG("Output chunk tree registered (Table: %v, ChunkTreeId: %v, Key: %v)",
        tableIndex,
        chunkTreeId,
        key);
}

void TOperationControllerBase::RegisterEndpoints(
    const TBoundaryKeysExt& boundaryKeys,
    int key,
    TOutputTable* outputTable)
{
    {
        TEndpoint endpoint;
        FromProto(&endpoint.Key, boundaryKeys.min());
        endpoint.Left = true;
        endpoint.ChunkTreeKey = key;
        outputTable->Endpoints.push_back(endpoint);
    }
    {
        TEndpoint endpoint;
        FromProto(&endpoint.Key, boundaryKeys.max());
        endpoint.Left = false;
        endpoint.ChunkTreeKey = key;
        outputTable->Endpoints.push_back(endpoint);
    }
}

void TOperationControllerBase::RegisterOutput(
    TRefCountedChunkSpecPtr chunkSpec,
    int key,
    int tableIndex)
{
    auto& table = OutputTables[tableIndex];

    if (table.KeyColumns && IsSortedOutputSupported()) {
        auto boundaryKeys = GetProtoExtension<TBoundaryKeysExt>(chunkSpec->chunk_meta().extensions());
        RegisterEndpoints(boundaryKeys, key, &table);
    }

    RegisterOutput(FromProto<TChunkId>(chunkSpec->chunk_id()), key, tableIndex, table);
}

void TOperationControllerBase::RegisterOutput(
    TJobletPtr joblet,
    int key,
    const TCompletedJobSummary& jobSummary)
{
    const auto* userJobResult = FindUserJobResult(jobSummary.Result);

    for (int tableIndex = 0; tableIndex < OutputTables.size(); ++tableIndex) {
        auto& table = OutputTables[tableIndex];
        RegisterOutput(joblet->ChunkListIds[tableIndex], key, tableIndex, table);

        if (table.KeyColumns && IsSortedOutputSupported()) {
            YCHECK(userJobResult);
            const auto& boundaryKeys = userJobResult->output_boundary_keys(tableIndex);
            RegisterEndpoints(boundaryKeys, key, &table);
        }
    }
}

void TOperationControllerBase::RegisterInputStripe(TChunkStripePtr stripe, TTaskPtr task)
{
    yhash_set<TChunkId> visitedChunks;

    TStripeDescriptor stripeDescriptor;
    stripeDescriptor.Stripe = stripe;
    stripeDescriptor.Task = task;
    stripeDescriptor.Cookie = task->GetChunkPoolInput()->Add(stripe);

    for (const auto& slice : stripe->ChunkSlices) {
        auto chunkSpec = slice->GetChunkSpec();
        auto chunkId = FromProto<TChunkId>(chunkSpec->chunk_id());

        auto pair = InputChunkMap.insert(std::make_pair(chunkId, TInputChunkDescriptor()));
        auto& chunkDescriptor = pair.first->second;

        if (InputChunkSpecs.insert(chunkSpec).second) {
            chunkDescriptor.ChunkSpecs.push_back(chunkSpec);
        }

        if (IsUnavailable(*chunkSpec, NeedsAllChunkParts())) {
            chunkDescriptor.State = EInputChunkState::Waiting;
        }

        if (visitedChunks.insert(chunkId).second) {
            chunkDescriptor.InputStripes.push_back(stripeDescriptor);
        }
    }
}

void TOperationControllerBase::RegisterIntermediate(
    TJobletPtr joblet,
    TCompletedJobPtr completedJob,
    TChunkStripePtr stripe)
{
    for (const auto& chunkSlice : stripe->ChunkSlices) {
        auto chunkId = FromProto<TChunkId>(chunkSlice->GetChunkSpec()->chunk_id());
        YCHECK(ChunkOriginMap.insert(std::make_pair(chunkId, completedJob)).second);

        if (IsIntermediateLivePreviewSupported()) {
            auto masterConnector = Host->GetMasterConnector();
            masterConnector->AttachToLivePreview(
                Operation,
                IntermediateTable.LivePreviewChunkListId,
                chunkId);
        }
    }
}

bool TOperationControllerBase::HasEnoughChunkLists(bool intermediate)
{
    if (intermediate) {
        return IntermediateOutputChunkListPool->HasEnough(1);
    }

    for (const auto& pair : CellMap) {
        const auto& data = pair.second;
        if (!data.ChunkListPool->HasEnough(data.OutputTableCount)) {
            return false;
        }
    }

    return true;
}

TChunkListId TOperationControllerBase::ExtractChunkList(TCellTag cellTag)
{
    const auto& data = GetCellData(cellTag);
    return data.ChunkListPool->Extract();
}

void TOperationControllerBase::ReleaseChunkLists(const std::vector<TChunkListId>& ids)
{
    yhash<TCellTag, std::vector<TChunkListId>> cellTagToIds;
    for (const auto& id : ids) {
        cellTagToIds[CellTagFromId(id)].push_back(id);
    }

    for (const auto& pair : cellTagToIds) {
        const auto& data = GetCellData(pair.first);
        data.ChunkListPool->Release(pair.second);
    }
}

void TOperationControllerBase::RegisterJoblet(TJobletPtr joblet)
{
    YCHECK(JobletMap.insert(std::make_pair(joblet->JobId, joblet)).second);
}

TOperationControllerBase::TJobletPtr TOperationControllerBase::GetJoblet(const TJobId& jobId)
{
    auto it = JobletMap.find(jobId);
    YCHECK(it != JobletMap.end());
    return it->second;
}

void TOperationControllerBase::RemoveJoblet(const TJobId& jobId)
{
    YCHECK(JobletMap.erase(jobId) == 1);
}

void TOperationControllerBase::BuildProgress(IYsonConsumer* consumer) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    BuildYsonMapFluently(consumer)
        .Item("jobs").Value(JobCounter)
        .Item("ready_job_count").Value(GetPendingJobCount())
        .Item("job_statistics").Do(BIND(&TOperation::BuildJobStatistics, Operation))
        .Item("estimated_input_statistics").BeginMap()
            .Item("chunk_count").Value(TotalEstimatedInputChunkCount)
            .Item("uncompressed_data_size").Value(TotalEstimatedInputDataSize)
            .Item("compressed_data_size").Value(TotalEstimatedCompressedDataSize)
            .Item("row_count").Value(TotalEstimatedInputRowCount)
            .Item("unavailable_chunk_count").Value(UnavailableInputChunkCount)
        .EndMap()
        .Item("live_preview").BeginMap()
            .Item("output_supported").Value(IsOutputLivePreviewSupported())
            .Item("intermediate_supported").Value(IsIntermediateLivePreviewSupported())
        .EndMap();
}

void TOperationControllerBase::BuildBriefProgress(IYsonConsumer* consumer) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    BuildYsonMapFluently(consumer)
        .Item("jobs").Value(JobCounter);
}

void TOperationControllerBase::BuildResult(IYsonConsumer* consumer) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto error = FromProto<TError>(Operation->Result().error());
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("error").Value(error)
        .EndMap();
}

void TOperationControllerBase::BuildBriefSpec(IYsonConsumer* consumer) const
{
    BuildYsonMapFluently(consumer)
        .DoIf(Spec->Title.HasValue(), [&] (TFluentMap fluent) {
            fluent
                .Item("title").Value(*Spec->Title);
        })
        .Item("input_table_paths").ListLimited(GetInputTablePaths(), 1)
        .Item("output_table_paths").ListLimited(GetOutputTablePaths(), 1);
}

std::vector<TOperationControllerBase::TPathWithStage> TOperationControllerBase::GetFilePaths() const
{
    return std::vector<TPathWithStage>();
}

bool TOperationControllerBase::NeedsAllChunkParts() const
{
    return false;
}

bool TOperationControllerBase::IsRowCountPreserved() const
{
    return false;
}

int TOperationControllerBase::SuggestJobCount(
    i64 totalDataSize,
    i64 dataSizePerJob,
    TNullable<int> configJobCount,
    int maxJobCount) const
{
    i64 suggestionBySize = (totalDataSize + dataSizePerJob - 1) / dataSizePerJob;
    i64 jobCount = configJobCount.Get(suggestionBySize);
    return static_cast<int>(Clamp(jobCount, 1, maxJobCount));
}

void TOperationControllerBase::InitUserJobSpecTemplate(
    NScheduler::NProto::TUserJobSpec* jobSpec,
    TUserJobSpecPtr config,
    const std::vector<TUserFile>& files)
{
    jobSpec->set_shell_command(config->Command);
    jobSpec->set_memory_limit(config->MemoryLimit);
    jobSpec->set_iops_threshold(config->IopsThreshold);
    jobSpec->set_use_yamr_descriptors(config->UseYamrDescriptors);
    jobSpec->set_check_input_fully_consumed(config->CheckInputFullyConsumed);
    jobSpec->set_max_stderr_size(config->MaxStderrSize);
    jobSpec->set_enable_core_dump(config->EnableCoreDump);
    jobSpec->set_custom_statistics_count_limit(config->CustomStatisticsCountLimit);

    {
        // Set input and output format.
        TFormat inputFormat(EFormatType::Yson);
        TFormat outputFormat(EFormatType::Yson);

        if (config->Format) {
            inputFormat = outputFormat = *config->Format;
        }

        if (config->InputFormat) {
            inputFormat = *config->InputFormat;
        }

        if (config->OutputFormat) {
            outputFormat = *config->OutputFormat;
        }

        jobSpec->set_input_format(ConvertToYsonString(inputFormat).Data());
        jobSpec->set_output_format(ConvertToYsonString(outputFormat).Data());
    }

    auto fillEnvironment = [&] (yhash_map<Stroka, Stroka>& env) {
        for (const auto& pair : env) {
            jobSpec->add_environment(Format("%v=%v", pair.first, pair.second));
        }
    };

    // Global environment.
    fillEnvironment(Config->Environment);

    // Local environment.
    fillEnvironment(config->Environment);

    jobSpec->add_environment(Format("YT_OPERATION_ID=%v", OperationId));

    for (const auto& file : files) {
        auto *descriptor = jobSpec->add_files();
        descriptor->set_type(static_cast<int>(file.Type));
        descriptor->set_file_name(file.FileName);
        ToProto(descriptor->mutable_chunks(), file.ChunkSpecs);
        switch (file.Type) {
            case EObjectType::File:
                descriptor->set_executable(file.Executable);
                break;
            case EObjectType::Table:
                descriptor->set_format(file.Format.Data());
                break;
            default:
                YUNREACHABLE();
        }
    }
}

void TOperationControllerBase::InitUserJobSpec(
    NScheduler::NProto::TUserJobSpec* jobSpec,
    TJobletPtr joblet,
    i64 memoryReserve)
{
    auto asyncSchedulerTransactionId = Operation->GetAsyncSchedulerTransaction()->GetId();
    ToProto(jobSpec->mutable_async_scheduler_transaction_id(), asyncSchedulerTransactionId);

    jobSpec->set_memory_reserve(memoryReserve);

    jobSpec->add_environment(Format("YT_JOB_INDEX=%v", joblet->JobIndex));
    jobSpec->add_environment(Format("YT_JOB_ID=%v", joblet->JobId));
    if (joblet->StartRowIndex >= 0) {
        jobSpec->add_environment(Format("YT_START_ROW_INDEX=%v", joblet->StartRowIndex));
    }
}

i64 TOperationControllerBase::GetFinalOutputIOMemorySize(TJobIOConfigPtr ioConfig) const
{
    i64 result = 0;
    for (const auto& outputTable : OutputTables) {
        if (outputTable.Options->ErasureCodec == NErasure::ECodec::None) {
            i64 maxBufferSize = std::max(
                ioConfig->TableWriter->MaxRowWeight,
                ioConfig->TableWriter->MaxBufferSize);
            result += GetOutputWindowMemorySize(ioConfig) + maxBufferSize;
        } else {
            auto* codec = NErasure::GetCodec(outputTable.Options->ErasureCodec);
            double replicationFactor = (double) codec->GetTotalPartCount() / codec->GetDataPartCount();
            result += static_cast<i64>(ioConfig->TableWriter->DesiredChunkSize * replicationFactor);
        }
    }

    if (!ioConfig->TableWriter->SyncChunkSwitch) {
        // Each writer may have up to 2 active chunks: closing one and current one.
        result *= 2;
    }
    return result;
}

i64 TOperationControllerBase::GetFinalIOMemorySize(
    TJobIOConfigPtr ioConfig,
    const TChunkStripeStatisticsVector& stripeStatistics) const
{
    i64 result = 0;
    for (const auto& stat : stripeStatistics) {
        result += GetInputIOMemorySize(ioConfig, stat);
    }
    result += GetFinalOutputIOMemorySize(ioConfig);
    return result;
}

void TOperationControllerBase::InitIntermediateInputConfig(TJobIOConfigPtr config)
{
    // Disable master requests.
    config->TableReader->AllowFetchingSeedsFromMaster = false;
}

void TOperationControllerBase::InitIntermediateOutputConfig(TJobIOConfigPtr config)
{
    // Don't replicate intermediate output.
    config->TableWriter->UploadReplicationFactor = 1;
    config->TableWriter->MinUploadReplicationFactor = 1;

    // Cache blocks on nodes.
    config->TableWriter->PopulateCache = true;

    // Don't move intermediate chunks.
    config->TableWriter->ChunksMovable = false;

    // Don't sync intermediate chunks.
    config->TableWriter->SyncOnClose = false;
}

void TOperationControllerBase::ValidateKey(const TOwningKey& key)
{
    for (int i = 0; i < key.GetCount(); ++i) {
        ValidateKeyValue(key[i]);
    }
}

void TOperationControllerBase::InitFinalOutputConfig(TJobIOConfigPtr /* config */)
{ }

TFluentLogEvent TOperationControllerBase::LogEventFluently(ELogEventType eventType)
{
    return Host->LogEventFluently(eventType)
        .Item("operation_id").Value(OperationId);
}

IClientPtr TOperationControllerBase::CreateClient()
{
    TClientOptions options;
    options.User = Operation->GetAuthenticatedUser();
    return Host
        ->GetMasterClient()
        ->GetConnection()
        ->CreateClient(options);
}

const NProto::TUserJobResult* TOperationControllerBase::FindUserJobResult(const TRefCountedJobResultPtr& result)
{
    const auto& schedulerJobResultExt = result->GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

    if (schedulerJobResultExt.has_user_job_result()) {
        return &schedulerJobResultExt.user_job_result();
    }
    return nullptr;
}

void TOperationControllerBase::Persist(TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, TotalEstimatedInputChunkCount);
    Persist(context, TotalEstimatedInputDataSize);
    Persist(context, TotalEstimatedInputRowCount);
    Persist(context, TotalEstimatedInputValueCount);
    Persist(context, TotalEstimatedCompressedDataSize);

    Persist(context, UnavailableInputChunkCount);

    Persist(context, JobCounter);

    Persist(context, InputNodeDirectory);
    Persist(context, AuxNodeDirectory);

    Persist(context, InputTables);

    Persist(context, OutputTables);

    Persist(context, IntermediateOutputCellTag);

    Persist(context, IntermediateTable);

    Persist(context, Files);

    Persist(context, Tasks);

    Persist(context, TaskGroups);

    Persist(context, InputChunkMap);

    Persist(context, CachedPendingJobCount);

    Persist(context, CachedNeededResources);

    Persist(context, ChunkOriginMap);

    Persist(context, JobletMap);

    Persist(context, JobIndexGenerator);

    // NB: Scheduler snapshots need not be stable.
    Persist<
        TSetSerializer<
            TDefaultSerializer,
            TUnsortedTag
        >
    >(context, InputChunkSpecs);

    if (context.IsLoad()) {
        for (auto task : Tasks) {
            task->Initialize();
        }
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

