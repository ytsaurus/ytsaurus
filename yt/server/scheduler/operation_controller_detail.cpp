#include "stdafx.h"
#include "operation_controller_detail.h"
#include "private.h"
#include "chunk_list_pool.h"
#include "chunk_pool.h"
#include "helpers.h"
#include "master_connector.h"

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/helpers.h>

#include <ytlib/node_tracker_client/node_directory_builder.h>

#include <ytlib/chunk_client/chunk_list_ypath_proxy.h>
#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_slice.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/private.h>

#include <ytlib/object_client/helpers.h>
#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/object_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/transaction_client/transaction_ypath_proxy.h>
#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/scheduler/config.h>
#include <ytlib/scheduler/helpers.h>

#include <ytlib/cgroup/statistics.h>

#include <ytlib/api/connection.h>

#include <core/concurrency/scheduler.h>

#include <core/rpc/helpers.h>

#include <core/erasure/codec.h>

#include <core/ytree/fluent.h>
#include <core/ytree/convert.h>
#include <core/ytree/attribute_helpers.h>

#include <util/string/cast.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient;
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
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NScheduler::NProto;
using namespace NTableClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NNodeTrackerClient::NProto;
using namespace NConcurrency;
using namespace NApi;
using namespace NRpc;

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TUserTableBase::Persist(TPersistenceContext& context)
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
    TUserTableBase::Persist(context);

    using NYT::Persist;
    Persist(context, ChunkCount);
    Persist(context, Chunks);
    Persist(context, KeyColumns);
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
    TUserTableBase::Persist(context);
    TLivePreviewTableBase::Persist(context);

    using NYT::Persist;
    Persist(context, Clear);
    Persist(context, Overwrite);
    Persist(context, LockMode);
    Persist(context, Options);
    Persist(context, OutputChunkListId);
    // NB: Scheduler snapshots need not be stable.
    CustomPersist<
        TMultiMapSerializer<
            TDefaultSerializer,
            TDefaultSerializer,
            TUnsortedTag
        >
    >(context, OutputChunkTreeIds);
    Persist(context, Endpoints);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TIntermediateTable::Persist(TPersistenceContext& context)
{
    TLivePreviewTableBase::Persist(context);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TUserFileBase::Persist(TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Path);
    Persist(context, Stage);
    Persist(context, FileName);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TRegularUserFile::Persist(TPersistenceContext& context)
{
    TUserFileBase::Persist(context);

    using NYT::Persist;
    Persist(context, FetchResponse);
    Persist(context, Executable);
}

////////////////////////////////////////////////////////////////////

void TOperationControllerBase::TUserTableFile::Persist(TPersistenceContext& context)
{
    TUserFileBase::Persist(context);

    using NYT::Persist;
    Persist(context, FetchResponse);
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
    CustomPersist<
        TSetSerializer<
            TDefaultSerializer,
            TUnsortedTag
        >
    >(context, NonLocalTasks);
    CustomPersist<
        TMultiMapSerializer<
            TDefaultSerializer,
            TDefaultSerializer,
            TUnsortedTag
        >
    >(context, CandidateTasks);
    CustomPersist<
        TMultiMapSerializer<
            TDefaultSerializer,
            TDefaultSerializer,
            TUnsortedTag
        >
    >(context, DelayedTasks);
    CustomPersist<
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
        Controller->GetCancelableControlInvoker(),
        BIND(&TInputChunkScratcher::LocateChunks, MakeWeak(this)),
        Controller->Config->ChunkScratchPeriod))
    , Proxy(masterChannel)
    , Started(false)
    , Logger(Controller->Logger)
{ }

void TOperationControllerBase::TInputChunkScratcher::Start()
{
    if (Started)
        return;

    Started = true;

    LOG_DEBUG("Starting input chunk scratcher");

    NextChunkIterator = Controller->InputChunkMap.begin();
    PeriodicExecutor->Start();
}

void TOperationControllerBase::TInputChunkScratcher::LocateChunks()
{
    VERIFY_THREAD_AFFINITY(Controller->ControlThread);

    auto startIterator = NextChunkIterator;
    auto req = Proxy.LocateChunks();

    for (int chunkCount = 0; chunkCount < Controller->Config->MaxChunksPerScratch; ++chunkCount) {
        ToProto(req->add_chunk_ids(), NextChunkIterator->first);

        ++NextChunkIterator;
        if (NextChunkIterator == Controller->InputChunkMap.end()) {
            NextChunkIterator = Controller->InputChunkMap.begin();
        }

        if (NextChunkIterator == startIterator) {
            // Total number of chunks is less than MaxChunksPerScratch.
            break;
        }
    }

    LOG_DEBUG("Locating input chunks (Count: %v)",
        req->chunk_ids_size());

    req->Invoke().Subscribe(
        BIND(&TInputChunkScratcher::OnLocateChunksResponse, MakeWeak(this))
            .Via(Controller->GetCancelableControlInvoker()));
}

void TOperationControllerBase::TInputChunkScratcher::OnLocateChunksResponse(TChunkServiceProxy::TRspLocateChunksPtr rsp)
{
    VERIFY_THREAD_AFFINITY(Controller->ControlThread);

    if (!rsp->IsOK()) {
        LOG_WARNING(*rsp, "Failed to locate input chunks");
        return;
    }

    Controller->NodeDirectory->MergeFrom(rsp->node_directory());

    int availableCount = 0;
    int unavailableCount = 0;
    for (const auto& chunkInfo : rsp->chunks()) {
        auto chunkId = FromProto<TChunkId>(chunkInfo.chunk_id());
        auto it = Controller->InputChunkMap.find(chunkId);
        YCHECK(it != Controller->InputChunkMap.end());

        auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(chunkInfo.replicas());

        auto& descriptor = it->second;
        YCHECK(!descriptor.ChunkSpecs.empty());
        auto& chunkSpec = descriptor.ChunkSpecs.front();
        auto codecId = NErasure::ECodec(chunkSpec->erasure_codec());

        if (IsUnavailable(replicas, codecId, Controller->NeedsAllChunkParts())) {
            ++unavailableCount;
            Controller->OnInputChunkUnavailable(chunkId, descriptor);
        } else {
            ++availableCount;
            Controller->OnInputChunkAvailable(chunkId, descriptor, replicas);
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

TJobPtr TOperationControllerBase::TTask::ScheduleJob(
    ISchedulingContext* context,
    const TNodeResources& jobLimits)
{
    int chunkListCount = GetChunkListCountPerJob();
    if (!Controller->HasEnoughChunkLists(chunkListCount)) {
        LOG_DEBUG("Job chunk list demand is not met");
        return nullptr;
    }

    int jobIndex = Controller->JobIndexGenerator.Next();
    auto joblet = New<TJoblet>(this, jobIndex);

    auto node = context->GetNode();
    const auto& address = node->GetAddress();
    auto* chunkPoolOutput = GetChunkPoolOutput();
    joblet->OutputCookie = chunkPoolOutput->Extract(address);
    if (joblet->OutputCookie == IChunkPoolOutput::NullCookie) {
        LOG_DEBUG("Job input is empty");
        return nullptr;
    }

    joblet->InputStripeList = chunkPoolOutput->GetStripeList(joblet->OutputCookie);
    joblet->MemoryReserveEnabled = IsMemoryReserveEnabled();

    auto neededResources = GetNeededResources(joblet);

    // Check the usage against the limits. This is the last chance to give up.
    if (!Dominates(jobLimits, neededResources)) {
        LOG_DEBUG("Job actual resource demand is not met (Limits: {%v}, Demand: {%v})",
            FormatResources(jobLimits),
            FormatResources(neededResources));
        CheckResourceDemandSanity(node, neededResources);
        chunkPoolOutput->Aborted(joblet->OutputCookie);
        // Seems like cached min needed resources are too optimistic.
        ResetCachedMinNeededResources();
        return nullptr;
    }

    auto jobType = GetJobType();

    // Async part.
    auto this_ = MakeStrong(this);
    auto controller = MakeStrong(Controller); // hold the controller
    auto operation = MakeStrong(Controller->Operation); // hold the operation
    auto jobSpecBuilder = BIND([this, this_, joblet, controller, operation] (TJobSpec* jobSpec) {
        BuildJobSpec(joblet, jobSpec);
        Controller->CustomizeJobSpec(joblet, jobSpec);

        auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        schedulerJobSpecExt->set_job_proxy_memory_control(Controller->Spec->JobProxyMemoryControl);
        schedulerJobSpecExt->set_enable_sort_verification(Controller->Spec->EnableSortVerification);

        // Adjust sizes if approximation flag is set.
        if (joblet->InputStripeList->IsApproximate) {
            schedulerJobSpecExt->set_input_uncompressed_data_size(static_cast<i64>(
                schedulerJobSpecExt->input_uncompressed_data_size() *
                ApproximateSizesBoostFactor));
            schedulerJobSpecExt->set_input_row_count(static_cast<i64>(
                schedulerJobSpecExt->input_row_count() *
                ApproximateSizesBoostFactor));
        }
    });

    joblet->Job = context->StartJob(
        Controller->Operation,
        jobType,
        neededResources,
        jobSpecBuilder);

    LOG_INFO(
        "Job scheduled (JobId: %v, OperationId: %v, JobType: %v, Address: %v, JobIndex: %v, ChunkCount: %v (%v local), "
        "Approximate: %lv, DataSize: %v (%v local), RowCount: %v, ResourceLimits: {%v})",
        joblet->Job->GetId(),
        Controller->Operation->GetId(),
        jobType,
        context->GetNode()->GetAddress(),
        jobIndex,
        joblet->InputStripeList->TotalChunkCount,
        joblet->InputStripeList->LocalChunkCount,
        joblet->InputStripeList->IsApproximate,
        joblet->InputStripeList->TotalDataSize,
        joblet->InputStripeList->LocalDataSize,
        joblet->InputStripeList->TotalRowCount,
        FormatResources(neededResources));

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
    if (Controller->IsRowCountPreserved()) {
        const auto& statistics = joblet->Job->Result().statistics();
        if (statistics.input().row_count() != statistics.output().row_count()) {
            Controller->OnOperationFailed(TError(
                "Input/output row count mismatch in completed job: %v != %v",
                statistics.input().row_count(),
                statistics.output().row_count())
                << TErrorAttribute("task", GetId()));
        }
    }
    GetChunkPoolOutput()->Completed(joblet->OutputCookie);
}

void TOperationControllerBase::TTask::ReinstallJob(TJobletPtr joblet, EJobReinstallReason reason)
{
    Controller->ChunkListPool->Release(joblet->ChunkListIds);

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

void TOperationControllerBase::TTask::OnJobFailed(TJobletPtr joblet)
{
    ReinstallJob(joblet, EJobReinstallReason::Failed);
}

void TOperationControllerBase::TTask::OnJobAborted(TJobletPtr joblet)
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
    TExecNodePtr node,
    const TNodeResources& neededResources)
{
    // The task is requesting more than some node is willing to provide it.
    // Maybe it's OK and we should wait for some time.
    // Or maybe it's not and the task is requesting something no one is able to provide.

    // First check if this very node has enough resources (including those currently
    // allocated by other jobs).
    if (Dominates(node->ResourceLimits(), neededResources))
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
    TNodeDirectoryBuilder directoryBuilder(Controller->NodeDirectory, schedulerJobSpecExt->mutable_node_directory());
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
    TNodeDirectoryBuilder directoryBuilder(Controller->NodeDirectory, schedulerJobSpecExt->mutable_node_directory());
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
            chunkSpec->set_partition_tag(partitionTag.Get());
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
    for (int index = 0; index < static_cast<int>(Controller->OutputTables.size()); ++index) {
        const auto& table = Controller->OutputTables[index];
        auto* outputSpec = schedulerJobSpecExt->add_output_specs();
        outputSpec->set_table_writer_options(ConvertToYsonString(table.Options).Data());
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
    options->KeyColumns = keyColumns;
    outputSpec->set_table_writer_options(ConvertToYsonString(options).Data());
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

TNodeResources TOperationControllerBase::TTask::GetNeededResources(TJobletPtr joblet) const
{
    UNUSED(joblet);
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
        joblet->Job->GetId(),
        this,
        joblet->OutputCookie,
        destinationPool,
        inputCookie,
        joblet->Job->GetNode()->GetAddress());

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

void TOperationControllerBase::TTask::RegisterOutput(TJobletPtr joblet, int key)
{
    Controller->RegisterOutput(joblet, key);
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
    , AuthenticatedMasterClient(CreateClient())
    , AuthenticatedInputMasterClient(AuthenticatedMasterClient)
    , AuthenticatedOutputMasterClient(AuthenticatedMasterClient)
    , Logger(OperationLogger)
    , CancelableContext(New<TCancelableContext>())
    , CancelableControlInvoker(CancelableContext->CreateInvoker(Host->GetControlInvoker()))
    , CancelableBackgroundInvoker(CancelableContext->CreateInvoker(Host->GetBackgroundInvoker()))
    , Prepared(false)
    , Running(false)
    , TotalInputChunkCount(0)
    , TotalInputDataSize(0)
    , TotalInputRowCount(0)
    , TotalInputValueCount(0)
    , TotalIntermeidateChunkCount(0)
    , TotalIntermediateDataSize(0)
    , TotalIntermediateRowCount(0)
    , TotalOutputChunkCount(0)
    , TotalOutputDataSize(0)
    , TotalOutputRowCount(0)
    , UnavailableInputChunkCount(0)
    , JobCounter(0)
    , CompletedJobStatistics(ZeroJobStatistics())
    , FailedJobStatistics(ZeroJobStatistics())
    , AbortedJobStatistics(ZeroJobStatistics())
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
        Spec->Title ? ~(*Spec->Title) : "<Null>");

    NodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();

    for (const auto& path : GetInputTablePaths()) {
        TInputTable table;
        table.Path = path;
        InputTables.push_back(table);
    }

    for (const auto& path : GetOutputTablePaths()) {
        TOutputTable table;
        table.Path = path;
        if (!path.GetAppend()) {
            table.Clear = true;
            table.Overwrite = true;
            table.LockMode = ELockMode::Exclusive;
        }

        table.Options->KeyColumns = path.Attributes().Find<TKeyColumns>("sorted_by");
        if (table.Options->KeyColumns) {
            if (!IsSortedOutputSupported()) {
                THROW_ERROR_EXCEPTION("Sorted outputs are not supported");
            } else {
                table.Clear = true;
                table.LockMode = ELockMode::Exclusive;
            }
        }

        OutputTables.push_back(table);
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

    InputChunkScratcher = New<TInputChunkScratcher>(this, AuthenticatedInputMasterClient->GetMasterChannel());
}

void TOperationControllerBase::DoInitialize()
{ }

TFuture<TError> TOperationControllerBase::Prepare()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto this_ = MakeStrong(this);
    return
        BIND(&TThis::DoPrepare, this_)
            .AsyncVia(CancelableBackgroundInvoker)
            .Run()
            .Apply(BIND([this, this_] (const TError& error) -> TError {
                if (error.IsOK()) {
                    Prepared = true;
                    Running = true;
                }
                return error;
            }).AsyncVia(CancelableControlInvoker));
}

TError TOperationControllerBase::DoPrepare()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    try {
        InitChunkListPool();

        GetInputObjectIds();
        GetOutputObjectIds();

        ValidateFileTypes();

        RequestInputObjects();
        RequestOutputObjects();
        RequestFileObjects();

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
            return TError();
        }

        SuspendUnavailableInputStripes();

        AddAllTaskPendingHints();

        // Input chunk scratcher initialization should be the last step to avoid races,
        // because input chunk scratcher works in control thread.
        InitInputChunkScratcher();

        return TError();
    } catch (const std::exception& ex) {
        return TError(ex);
    }
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

TFuture<TError> TOperationControllerBase::Revive()
{
    auto this_ = MakeStrong(this);
    return BIND(&TOperationControllerBase::DoRevive, this_)
        .AsyncVia(CancelableBackgroundInvoker)
        .Run()
        .Apply(BIND([this, this_] () -> TError {
            ReinstallLivePreview();
            Prepared = true;
            Running = true;
            return TError();
        }).AsyncVia(CancelableControlInvoker));
}

void TOperationControllerBase::DoRevive()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    InitChunkListPool();

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
    auto operationId = Operation->GetId();

    LOG_INFO("Starting async scheduler transaction (OperationId: %v)",
        operationId);

    TObjectServiceProxy proxy(AuthenticatedMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TMasterYPathProxy::CreateObjects();
        req->set_type(EObjectType::Transaction);

        auto* reqExt = req->MutableExtension(NTransactionClient::NProto::TReqStartTransactionExt::create_transaction_ext);
        reqExt->set_timeout(Config->OperationTransactionTimeout.MilliSeconds());
        reqExt->set_enable_uncommitted_accounting(false);
        reqExt->set_enable_staged_accounting(false);

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Scheduler async for operation %v",
            operationId));
        ToProto(req->mutable_object_attributes(), *attributes);

        GenerateMutationId(req);
        batchReq->AddRequest(req, "start_async_tx");
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError(), "Error starting async scheduler transaction");
    if (Operation->GetState() != EOperationState::Initializing &&
        Operation->GetState() != EOperationState::Reviving)
        throw TFiberCanceledException();


    {
        auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObjects>("start_async_tx");
        auto transactionId = FromProto<TObjectId>(rsp->object_ids(0));
        auto transactionManager = AuthenticatedMasterClient->GetTransactionManager();
        TTransactionAttachOptions options(transactionId);
        options.AutoAbort = false;
        options.Ping = true;
        options.PingAncestors = false;
        Operation->SetAsyncSchedulerTransaction(transactionManager->Attach(options));
    }

    LOG_INFO("Scheduler async transaction started (AsyncTranasctionId: %v, OperationId: %v)",
        Operation->GetAsyncSchedulerTransaction()->GetId(),
        operationId);
}

void TOperationControllerBase::StartSyncSchedulerTransaction()
{
    auto operationId = Operation->GetId();

    LOG_INFO("Starting sync scheduler transaction (OperationId: %v)",
        operationId);

    TObjectServiceProxy proxy(AuthenticatedMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    {
        auto userTransaction = Operation->GetUserTransaction();
        auto req = TMasterYPathProxy::CreateObjects();
        if (userTransaction) {
            ToProto(req->mutable_transaction_id(), userTransaction->GetId());
        }
        req->set_type(EObjectType::Transaction);

        auto* reqExt = req->MutableExtension(NTransactionClient::NProto::TReqStartTransactionExt::create_transaction_ext);
        reqExt->set_timeout(Config->OperationTransactionTimeout.MilliSeconds());

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Scheduler sync for operation %v",
            operationId));
        ToProto(req->mutable_object_attributes(), *attributes);

        GenerateMutationId(req);
        batchReq->AddRequest(req, "start_sync_tx");
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError(), "Error starting sync scheduler transaction");
    if (Operation->GetState() != EOperationState::Initializing &&
        Operation->GetState() != EOperationState::Reviving)
        throw TFiberCanceledException();

    {
        auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObjects>("start_sync_tx");
        auto transactionid = FromProto<TObjectId>(rsp->object_ids(0));
        TTransactionAttachOptions options(transactionid);
        options.AutoAbort = false;
        options.Ping = true;
        options.PingAncestors = false;
        Operation->SetSyncSchedulerTransaction(Host->GetMasterClient()->GetTransactionManager()->Attach(options));
    }

    LOG_INFO("Scheduler sync transaction started (SyncTransactionId: %v, OperationId: %v)",
        Operation->GetSyncSchedulerTransaction()->GetId(),
        operationId);
}

void TOperationControllerBase::StartInputTransaction(TTransactionId parentTransactionId)
{
    auto operationId = Operation->GetId();

    LOG_INFO("Starting input transaction (OperationId: %v)", operationId);

    TObjectServiceProxy proxy(AuthenticatedInputMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TMasterYPathProxy::CreateObjects();
        ToProto(req->mutable_transaction_id(), parentTransactionId);
        req->set_type(EObjectType::Transaction);

        auto* reqExt = req->MutableExtension(NTransactionClient::NProto::TReqStartTransactionExt::create_transaction_ext);
        reqExt->set_timeout(Config->OperationTransactionTimeout.MilliSeconds());

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Scheduler input for operation %v",
            operationId));
        ToProto(req->mutable_object_attributes(), *attributes);

        GenerateMutationId(req);
        batchReq->AddRequest(req, "start_in_tx");
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError(), "Error starting input transaction");
    if (Operation->GetState() != EOperationState::Initializing &&
        Operation->GetState() != EOperationState::Reviving)
        throw TFiberCanceledException();

    {
        auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObjects>("start_in_tx");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error starting input transaction");
        auto id = FromProto<TTransactionId>(rsp->object_ids(0));
        auto transactionManager = AuthenticatedInputMasterClient->GetTransactionManager();
        TTransactionAttachOptions options(id);
        options.AutoAbort = false;
        options.Ping = true;
        Operation->SetInputTransaction(transactionManager->Attach(options));
    }
}

void TOperationControllerBase::StartOutputTransaction(TTransactionId parentTransactionId)
{
    auto operationId = Operation->GetId();

    LOG_INFO("Starting output transaction (OperationId: %v)", operationId);

    TObjectServiceProxy proxy(AuthenticatedOutputMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TMasterYPathProxy::CreateObjects();
        ToProto(req->mutable_transaction_id(), parentTransactionId);
        req->set_type(EObjectType::Transaction);

        auto* reqExt = req->MutableExtension(NTransactionClient::NProto::TReqStartTransactionExt::create_transaction_ext);
        reqExt->set_enable_uncommitted_accounting(false);
        reqExt->set_timeout(Config->OperationTransactionTimeout.MilliSeconds());

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Scheduler output for operation %v",
            operationId));
        ToProto(req->mutable_object_attributes(), *attributes);

        GenerateMutationId(req);
        batchReq->AddRequest(req, "start_out_tx");
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError(), "Error starting output transactions");
    if (Operation->GetState() != EOperationState::Initializing &&
        Operation->GetState() != EOperationState::Reviving)
        throw TFiberCanceledException();

    {
        auto rsp = batchRsp->GetResponse<TMasterYPathProxy::TRspCreateObjects>("start_out_tx");
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error starting output transaction");
        auto id = FromProto<TTransactionId>(rsp->object_ids(0));
        auto transactionManager = AuthenticatedOutputMasterClient->GetTransactionManager();
        TTransactionAttachOptions options(id);
        options.AutoAbort = false;
        options.Ping = true;
        Operation->SetOutputTransaction(transactionManager->Attach(options));
    }
}

void TOperationControllerBase::InitChunkListPool()
{
    ChunkListPool = New<TChunkListPool>(
        Config,
        Host->GetMasterClient()->GetMasterChannel(),
        CancelableControlInvoker,
        Operation->GetId(),
        Operation->GetOutputTransaction()->GetId());
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
        joblet->Task->OnJobAborted(joblet);
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

TFuture<TError> TOperationControllerBase::Commit()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return
        BIND(&TThis::DoCommit, MakeStrong(this))
            .AsyncVia(CancelableBackgroundInvoker)
            .Run();
}

TError TOperationControllerBase::DoCommit()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    try {
        CommitResults();
        return TError();
    } catch (const std::exception& ex) {
        return TError(ex);
    }
}

void TOperationControllerBase::CommitResults()
{
    LOG_INFO("Committing results");

    TObjectServiceProxy proxy(AuthenticatedOutputMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    for (auto& table : OutputTables) {
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

            auto addChunkTree = [&] (const TChunkTreeId chunkTreeId) {
                if (!req) {
                    req = TChunkListYPathProxy::Attach(FromObjectId(table.OutputChunkListId));
                    GenerateMutationId(req);
                }
                ToProto(req->add_children_ids(), chunkTreeId);
                ++reqSize;
                if (reqSize >= Config->MaxChildrenPerAttachRequest) {
                    flushReq();
                }
            };

            if (table.Options->KeyColumns && IsSortedOutputSupported()) {
                // Sorted output generated by user operation requires rearranging.
                YCHECK(table.Endpoints.size() % 2 == 0);

                LOG_DEBUG("Sorting %v endpoints", static_cast<int>(table.Endpoints.size()));
                std::sort(
                    table.Endpoints.begin(),
                    table.Endpoints.end(),
                    [=] (const TEndpoint& lhs, const TEndpoint& rhs) -> bool {
                        // First sort by keys.
                        // Then sort by ChunkTreeKeys.
                        auto keysResult = CompareRows(lhs.Key, rhs.Key);
                        if (keysResult != 0) {
                            return keysResult < 0;
                        }
                        return lhs.ChunkTreeKey < rhs.ChunkTreeKey;
                });

                auto outputCount = static_cast<int>(table.Endpoints.size()) / 2;
                for (int outputIndex = 0; outputIndex < outputCount; ++outputIndex) {
                    auto& leftEndpoint = table.Endpoints[2 * outputIndex];
                    auto& rightEndpoint = table.Endpoints[2 * outputIndex + 1];
                    if (leftEndpoint.ChunkTreeKey != rightEndpoint.ChunkTreeKey) {
                        auto error = TError("Output table %v is not sorted: job outputs have overlapping key ranges",
                            table.Path.GetPath());

                        LOG_DEBUG(error);
                        THROW_ERROR error;
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

        if (table.Options->KeyColumns) {
            LOG_INFO("Table %v will be marked as sorted by %v",
                table.Path.GetPath(),
                ConvertToYsonString(table.Options->KeyColumns.Get(), EYsonFormat::Text).Data());
            auto req = TTableYPathProxy::SetSorted(path);
            ToProto(req->mutable_key_columns(), table.Options->KeyColumns.Get());
            SetTransactionId(req, Operation->GetOutputTransaction());
            GenerateMutationId(req);
            batchReq->AddRequest(req, "set_out_sorted");
        }
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError(), "Error committing results");

    LOG_INFO("Results committed");
}

void TOperationControllerBase::OnJobRunning(TJobPtr job, const TJobStatus& status)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    UNUSED(job);
    UNUSED(status);
}

void TOperationControllerBase::OnJobStarted(TJobPtr job)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LogEventFluently(ELogEventType::JobStarted)
        .Item("job_id").Value(job->GetId())
        .Item("operation_id").Value(job->GetOperation()->GetId())
        .Item("resource_limits").Value(job->ResourceLimits())
        .Item("node_address").Value(job->GetNode()->GetAddress());

    JobCounter.Start(1);
}

void TOperationControllerBase::OnJobCompleted(TJobPtr job)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto& result = job->Result();

    LogFinishedJobFluently(ELogEventType::JobCompleted, job);

    const auto& jobStatistics = result.statistics();
    JobCounter.Completed(1);
    CompletedJobStatistics += jobStatistics;

    if (jobStatistics.has_statistics()) {
        auto statistics = ConvertTo<NJobProxy::TStatistics>(TYsonString(jobStatistics.statistics()));
        Statistics.Merge(statistics);
    }

    const auto& schedulerResultEx = result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

    // Populate node directory by adding additional nodes returned from the job.
    NodeDirectory->MergeFrom(schedulerResultEx.node_directory());

    auto joblet = GetJoblet(job);
    joblet->Task->OnJobCompleted(joblet);

    RemoveJoblet(job);

    UpdateTask(joblet->Task);

    if (IsCompleted()) {
        OnOperationCompleted();
    }
}

void TOperationControllerBase::OnJobFailed(TJobPtr job)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto& result = job->Result();

    auto error = FromProto<TError>(result.error());

    LogFinishedJobFluently(ELogEventType::JobFailed, job)
        .Item("error").Value(error);

    JobCounter.Failed(1);
    FailedJobStatistics += result.statistics();

    auto joblet = GetJoblet(job);
    joblet->Task->OnJobFailed(joblet);

    RemoveJoblet(job);

    if (error.Attributes().Get<bool>("fatal", false)) {
        OnOperationFailed(error);
        return;
    }

    int failedJobCount = JobCounter.GetFailed();
    int maxFailedJobCount = Spec->MaxFailedJobCount.Get(Config->MaxFailedJobCount);
    if (failedJobCount >= maxFailedJobCount) {
        OnOperationFailed(TError("Failed jobs limit %v has been reached",
            maxFailedJobCount));
        return;
    }
}

void TOperationControllerBase::OnJobAborted(TJobPtr job)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto abortReason = GetAbortReason(job);
    const auto& result = job->Result();

    LogFinishedJobFluently(ELogEventType::JobAborted, job)
        .Item("reason").Value(abortReason);

    JobCounter.Aborted(1, abortReason);
    AbortedJobStatistics += result.statistics();

    auto joblet = GetJoblet(job);
    joblet->Task->OnJobAborted(joblet);

    RemoveJoblet(job);

    if (abortReason == EAbortReason::FailedChunks) {
        const auto& result = job->Result();
        const auto& schedulerResultExt = result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

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

TJobPtr TOperationControllerBase::ScheduleJob(
    ISchedulingContext* context,
    const TNodeResources& jobLimits)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!Running ||
        Operation->GetState() != EOperationState::Running ||
        Operation->GetSuspended())
    {
        LOG_TRACE("Operation is not running, scheduling request ignored");
        return nullptr;
    }

    if (GetPendingJobCount() == 0) {
        LOG_TRACE("No pending jobs left, scheduling request ignored");
        return nullptr;
    }

    auto job = DoScheduleJob(context, jobLimits);
    if (!job) {
        return nullptr;
    }

    OnJobStarted(job);

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
                const auto& descriptor = NodeDirectory->GetDescriptor(replica);
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

bool TOperationControllerBase::CheckJobLimits(TExecNodePtr node, TTaskPtr task, const TNodeResources& jobLimits)
{
    auto neededResources = task->GetMinNeededResources();
    if (Dominates(jobLimits, neededResources)) {
        return true;
    }
    task->CheckResourceDemandSanity(node, neededResources);
    return false;
}

TJobPtr TOperationControllerBase::DoScheduleJob(
    ISchedulingContext* context,
    const TNodeResources& jobLimits)
{
    auto localJob = DoScheduleLocalJob(context, jobLimits);
    if (localJob) {
        return localJob;
    }

    auto nonLocalJob = DoScheduleNonLocalJob(context, jobLimits);
    if (nonLocalJob) {
        return nonLocalJob;
    }

    return nullptr;
}

TJobPtr TOperationControllerBase::DoScheduleLocalJob(
    ISchedulingContext* context,
    const TNodeResources& jobLimits)
{
    auto node = context->GetNode();
    const auto& address = node->GetAddress();

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

            if (!CheckJobLimits(node, task, jobLimits)) {
                continue;
            }

            bestLocality = locality;
            bestTask = task;
        }

        if (!Running) {
            return nullptr;
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
            auto job = bestTask->ScheduleJob(context, jobLimits);
            if (job) {
                bestTask->SetDelayedTime(Null);
                UpdateTask(bestTask);
                return job;
            }
        }
    }
    return nullptr;
}

TJobPtr TOperationControllerBase::DoScheduleNonLocalJob(
    ISchedulingContext* context,
    const TNodeResources& jobLimits)
{
    auto now = TInstant::Now();
    const auto& node = context->GetNode();
    const auto& address = node->GetAddress();

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
            auto it = candidateTasks.begin();
            while (it != candidateTasks.end()) {
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

                if (!CheckJobLimits(node, task, jobLimits)) {
                    ++it;
                    continue;
                }

                if (!task->GetDelayedTime()) {
                    task->SetDelayedTime(now);
                }

                auto deadline = task->GetDelayedTime().Get() + task->GetLocalityTimeout();
                if (deadline > now) {
                    LOG_DEBUG("Task delayed (Task: %v, Deadline: %v)",
                        task->GetId(),
                        deadline);
                    delayedTasks.insert(std::make_pair(deadline, task));
                    candidateTasks.erase(it++);
                    continue;
                }

                if (!Running) {
                    return nullptr;
                }

                LOG_DEBUG(
                    "Attempting to schedule a non-local job (Task: %v, Address: %v, JobLimits: {%v}, "
                    "PendingDataSize: %v, PendingJobCount: %v)",
                    task->GetId(),
                    address,
                    FormatResources(jobLimits),
                    task->GetPendingDataSize(),
                    task->GetPendingJobCount());

                auto job = task->ScheduleJob(context, jobLimits);
                if (job) {
                    UpdateTask(task);
                    return job;
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
        }
    }
    return nullptr;
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
    TObjectServiceProxy proxy(Host->GetMasterClient()->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    auto addRequest = [&] (
            const Stroka& path,
            int replicationFactor,
            const Stroka& key) {
        auto req = TCypressYPathProxy::Create(path);

        req->set_type(EObjectType::Table);
        req->set_ignore_existing(true);

        auto attributes = CreateEphemeralAttributes();
        attributes->Set("replication_factor", replicationFactor);

        ToProto(req->mutable_node_attributes(), *attributes);

        batchReq->AddRequest(req, key);
    };

    if (IsOutputLivePreviewSupported()) {
        LOG_INFO("Creating output tables for live preview");

        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            const auto& table = OutputTables[index];
            auto path = GetLivePreviewOutputPath(Operation->GetId(), index);
            addRequest(path, table.Options->ReplicationFactor, "create_output");
        }
    }

    if (IsIntermediateLivePreviewSupported()) {
        LOG_INFO("Creating intermediate table for live preview");

        auto path = GetLivePreviewIntermediatePath(Operation->GetId());
        addRequest(path, 1, "create_intermediate");
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError(), "Error creating live preview tables");

    auto handleResponse = [&] (TLivePreviewTableBase& table, TCypressYPathProxy::TRspCreatePtr rsp) {
        table.LivePreviewTableId = FromProto<NCypressClient::TNodeId>(rsp->node_id());
    };

    if (IsOutputLivePreviewSupported()) {
        auto rsps = batchRsp->GetResponses<TCypressYPathProxy::TRspCreate>("create_output");
        YCHECK(rsps.size() == OutputTables.size());
        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            handleResponse(OutputTables[index], rsps[index]);
        }

        LOG_INFO("Output live preview tables created");
    }

    if (IsIntermediateLivePreviewSupported()) {
        auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspCreate>("create_intermediate");
        handleResponse(IntermediateTable, rsp);

        LOG_INFO("Intermediate live preview table created");
    }
}

void TOperationControllerBase::PrepareLivePreviewTablesForUpdate()
{
    // NB: use root credentials.
    TObjectServiceProxy proxy(Host->GetMasterClient()->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    auto addRequest = [&] (const TLivePreviewTableBase& table, const Stroka& key) {
        auto req = TTableYPathProxy::PrepareForUpdate(FromObjectId(table.LivePreviewTableId));
        req->set_mode(EUpdateMode::Overwrite);
        SetTransactionId(req, Operation->GetAsyncSchedulerTransaction());
        batchReq->AddRequest(req, key);
    };

    if (IsOutputLivePreviewSupported()) {
        LOG_INFO("Preparing live preview output tables for update");

        for (const auto& table : OutputTables) {
            addRequest(table, "prepare_output");
        }
    }

    if (IsIntermediateLivePreviewSupported()) {
        LOG_INFO("Preparing live preview intermediate table for update");

        addRequest(IntermediateTable, "prepare_intermediate");
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(batchRsp->GetCumulativeError(), "Error preparing live preview tables for update");

    auto handleResponse = [&] (TLivePreviewTableBase& table, TTableYPathProxy::TRspPrepareForUpdatePtr rsp) {
        table.LivePreviewChunkListId = FromProto<NCypressClient::TNodeId>(rsp->chunk_list_id());
    };

    if (IsOutputLivePreviewSupported()) {
        auto rsps = batchRsp->GetResponses<TTableYPathProxy::TRspPrepareForUpdate>("prepare_output");
        YCHECK(rsps.size() == OutputTables.size());
        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            handleResponse(OutputTables[index], rsps[index]);
        }

        LOG_INFO("Output live preview tables prepared for update");
    }

    if (IsIntermediateLivePreviewSupported()) {
        auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspPrepareForUpdate>("prepare_intermediate");
        handleResponse(IntermediateTable, rsp);

        LOG_INFO("Intermediate live preview table prepared for update");
    }
}

void TOperationControllerBase::GetInputObjectIds()
{
    LOG_INFO("Getting input object ids");

    TObjectServiceProxy proxy(AuthenticatedInputMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& table : InputTables) {
        auto req = TObjectYPathProxy::GetBasicAttributes(table.Path.GetPath());
        SetTransactionId(req, Operation->GetInputTransaction());
        batchReq->AddRequest(req, "get_in_id");
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error getting ids for input objects");

    {
        auto getInIdRsps = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_in_id");
        for (int index = 0; index < static_cast<int>(InputTables.size()); ++index) {
            auto& table = InputTables[index];
            {
                auto rsp = getInIdRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting id for input table %v",
                    table.Path.GetPath());
                table.ObjectId = FromProto<TObjectId>(rsp->id());
                auto type = EObjectType(rsp->type());

                if (type != EObjectType::Table) {
                    THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv, actual %Qlv",
                        table.Path.GetPath(),
                        EObjectType(EObjectType::Table),
                        type);
                }
            }
        }
    }

    LOG_INFO("Input object ids received");
}

void TOperationControllerBase::GetOutputObjectIds()
{
    LOG_INFO("Getting output object ids");

    TObjectServiceProxy proxy(AuthenticatedOutputMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& table : OutputTables) {
        auto req = TObjectYPathProxy::GetBasicAttributes(table.Path.GetPath());
        SetTransactionId(req, Operation->GetOutputTransaction());
        batchReq->AddRequest(req, "get_out_id");
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error getting ids for output objects");

    {
        auto getOutIdRsps = batchRsp->GetResponses<TObjectYPathProxy::TRspGetBasicAttributes>("get_out_id");
        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            auto& table = OutputTables[index];
            {
                auto rsp = getOutIdRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting id for output table %v",
                    table.Path.GetPath());
                table.ObjectId = FromProto<TObjectId>(rsp->id());
                auto type = EObjectType(rsp->type());

                if (type != EObjectType::Table) {
                    THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv, actual %Qlv",
                        table.Path.GetPath(),
                        EObjectType(EObjectType::Table),
                        type);
                }
            }
        }
    }

    LOG_INFO("Output object ids received");
}

void TOperationControllerBase::ValidateFileTypes()
{
    LOG_INFO("Getting file object types");

    TObjectServiceProxy proxy(AuthenticatedMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& pair : GetFilePaths()) {
        const auto& path = pair.first;
        auto req = TObjectYPathProxy::Get(path.GetPath() + "/@type");
        SetTransactionId(req, Operation->GetInputTransaction());
        batchReq->AddRequest(req, "get_file_types");
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error getting file object types");

    auto paths = GetFilePaths();
    auto getFileTypes = batchRsp->GetResponses<TObjectYPathProxy::TRspGet>("get_file_types");
    for (int index = 0; index < static_cast<int>(paths.size()); ++index) {
        auto richPath = paths[index].first;
        auto path = richPath.GetPath();
        auto stage = paths[index].second;
        auto rsp = getFileTypes[index];
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting type for file %v",
            path);

        auto type = ConvertTo<EObjectType>(TYsonString(rsp->value()));
        TUserFileBase* file;
        switch (type) {
            case EObjectType::File:
                RegularFiles.push_back(TRegularUserFile());
                file = &RegularFiles.back();
                break;
            case EObjectType::Table:
                TableFiles.push_back(TUserTableFile());
                file = &TableFiles.back();
                break;
            default:
                THROW_ERROR_EXCEPTION("Object %v has invalid type: expected %Qlv or %Qlv, actual %Qlv",
                    path,
                    EObjectType(EObjectType::File),
                    EObjectType(EObjectType::Table),
                    type);
        }
        file->Stage = stage;
        file->Path = richPath;
    }

    LOG_INFO("File types received");
}

void TOperationControllerBase::FetchInputTables()
{
    TObjectServiceProxy proxy(AuthenticatedInputMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    for (int tableIndex = 0; tableIndex < InputTables.size(); ++tableIndex) {
        const auto& table = InputTables[tableIndex];

        for (auto range : table.Path.GetRanges()) {
            for (i64 index = 0; index * Config->MaxChunkCountPerFetch < table.ChunkCount; ++index) {
                auto chunkCountLowerLimit = index * Config->MaxChunkCountPerFetch;
                if (range.LowerLimit().HasChunkIndex()) {
                    chunkCountLowerLimit = std::max(chunkCountLowerLimit, range.LowerLimit().GetChunkIndex());
                }
                range.LowerLimit().SetChunkIndex(chunkCountLowerLimit);

                auto chunkCountUpperLimit = (index + 1) * Config->MaxChunkCountPerFetch;
                if (range.UpperLimit().HasChunkIndex()) {
                    chunkCountUpperLimit = std::min(chunkCountUpperLimit, range.UpperLimit().GetChunkIndex());
                }
                range.UpperLimit().SetChunkIndex(chunkCountUpperLimit);

                auto req = TTableYPathProxy::Fetch(FromObjectId(table.ObjectId));
                InitializeFetchRequest(req.Get(), table.Path);
                ToProto(req->mutable_ranges(), std::vector<TReadRange>({range}));
                req->set_fetch_all_meta_extensions(true);
                req->set_fetch_parity_replicas(IsParityReplicasFetchEnabled());
                SetTransactionId(req, Operation->GetInputTransaction());
                batchReq->AddRequest(req, "fetch_input_table_" + ::ToString(tableIndex));
            }
        }
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error fetching input tables");

    for (int tableIndex = 0; tableIndex < InputTables.size(); ++tableIndex) {
        auto& table = InputTables[tableIndex];

        if (table.Path.GetRanges().empty()) {
            continue;
        }

        auto fetchRsps = batchRsp->GetResponses<TTableYPathProxy::TRspFetch>("fetch_input_table_" + ::ToString(tableIndex));
        for (const auto& rsp : fetchRsps) {
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error fetching input table %v", table.Path.GetPath());

            NodeDirectory->MergeFrom(rsp->node_directory());

            for (const auto& chunk : rsp->chunks()) {
                table.Chunks.push_back(chunk);
            }
        }
        LOG_INFO("Input table fetched (Path: %v, ChunkCount: %v)",
            table.Path.GetPath(),
            table.Chunks.size());
    }
}

void TOperationControllerBase::RequestInputObjects()
{
    LOG_INFO("Requesting input objects");

    TObjectServiceProxy proxy(AuthenticatedInputMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& table : InputTables) {
        auto path = FromObjectId(table.ObjectId);
        {
            auto req = TCypressYPathProxy::Lock(path);
            req->set_mode(ELockMode::Snapshot);
            SetTransactionId(req, Operation->GetInputTransaction());
            GenerateMutationId(req);
            batchReq->AddRequest(req, "lock_in");
        }
        {
            auto req = TYPathProxy::Get(path);
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("sorted");
            attributeFilter.Keys.push_back("sorted_by");
            attributeFilter.Keys.push_back("chunk_count");
            ToProto(req->mutable_attribute_filter(), attributeFilter);
            SetTransactionId(req, Operation->GetInputTransaction());
            batchReq->AddRequest(req, "get_in_attributes");
        }
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error requesting input objects");

    {
        auto lockInRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_in");
        auto getInAttributesRsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_in_attributes");
        for (int index = 0; index < static_cast<int>(InputTables.size()); ++index) {
            auto& table = InputTables[index];
            auto path = table.Path.GetPath();
            {
                auto rsp = lockInRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error locking input table %v", path);

                LOG_INFO("Input table locked (Path: %v)", path);
            }
            {
                auto rsp = getInAttributesRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting attributes for input table %v",
                    path);

                auto node = ConvertToNode(TYsonString(rsp->value()));
                const auto& attributes = node->Attributes();

                if (attributes.Get<bool>("sorted")) {
                    table.KeyColumns = attributes.Get<TKeyColumns>("sorted_by");
                    LOG_INFO("Input table is sorted (Path: %v, KeyColumns: %v)",
                        path,
                        ConvertToYsonString(table.KeyColumns.Get(), EYsonFormat::Text).Data());
                } else {
                    LOG_INFO("Input table is not sorted (Path: %v)",
                        path);
                }

                table.ChunkCount = attributes.Get<int>("chunk_count");
            }
        }
    }

    FetchInputTables();

    LOG_INFO("Input object recieved");
}


void TOperationControllerBase::RequestOutputObjects()
{
    LOG_INFO("Requesting output objects");

    TObjectServiceProxy proxy(AuthenticatedOutputMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& table : OutputTables) {
        auto path = FromObjectId(table.ObjectId);
        {
            auto req = TCypressYPathProxy::Lock(path);
            req->set_mode(table.LockMode);
            GenerateMutationId(req);
            SetTransactionId(req, Operation->GetOutputTransaction());
            batchReq->AddRequest(req, "lock_out");
        }
        {
            auto req = TYPathProxy::Get(path);
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("channels");
            attributeFilter.Keys.push_back("compression_codec");
            attributeFilter.Keys.push_back("erasure_codec");
            attributeFilter.Keys.push_back("row_count");
            attributeFilter.Keys.push_back("replication_factor");
            attributeFilter.Keys.push_back("account");
            attributeFilter.Keys.push_back("vital");
            ToProto(req->mutable_attribute_filter(), attributeFilter);
            SetTransactionId(req, Operation->GetOutputTransaction());
            batchReq->AddRequest(req, "get_out_attributes");
        }
        {
            auto req = TTableYPathProxy::PrepareForUpdate(path);
            SetTransactionId(req, Operation->GetOutputTransaction());
            GenerateMutationId(req);
            req->set_mode(table.Clear ? NChunkClient::EUpdateMode::Overwrite : NChunkClient::EUpdateMode::Append);
            batchReq->AddRequest(req, "prepare_for_update");
        }
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error requesting output objects");

    {
        auto lockOutRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_out");
        auto getOutAttributesRsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_out_attributes");
        auto prepareForUpdateRsps = batchRsp->GetResponses<TTableYPathProxy::TRspPrepareForUpdate>("prepare_for_update");
        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            auto& table = OutputTables[index];
            auto path = table.Path.GetPath();
            {
                auto rsp = lockOutRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error locking output table %v",
                    path);

                LOG_INFO("Output table %v locked",
                    path);
            }
            {
                auto rsp = getOutAttributesRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting attributes for output table %v",
                    path);

                auto node = ConvertToNode(TYsonString(rsp->value()));
                const auto& attributes = node->Attributes();

                Deserialize(
                    table.Options->Channels,
                    ConvertToNode(attributes.GetYson("channels")));

                i64 initialRowCount = attributes.Get<i64>("row_count");
                if (initialRowCount > 0 && table.Clear && !table.Overwrite) {
                    THROW_ERROR_EXCEPTION("Can't append sorted data to non-empty output table %v",
                        table.Path.GetPath());
                }
                table.Options->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
                table.Options->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
                table.Options->ReplicationFactor = attributes.Get<int>("replication_factor");
                table.Options->Account = attributes.Get<Stroka>("account");
                table.Options->ChunksVital = attributes.Get<bool>("vital");

                LOG_INFO("Output table attributes received (Path: %v, Options: %v)",
                    path,
                    ConvertToYsonString(table.Options, EYsonFormat::Text).Data());
            }
            {
                auto rsp = prepareForUpdateRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error preparing output table %v for update",
                    path);

                table.OutputChunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
                LOG_INFO("Output table prepared for update (Path: %v, ChunkListId: %v)",
                    path,
                    table.OutputChunkListId);
            }
        }
    }

    LOG_INFO("Output objects recieved");
}

void TOperationControllerBase::FetchFileObjects()
{
    TObjectServiceProxy proxy(AuthenticatedMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& file : RegularFiles) {
        auto path = file.Path.GetPath();
        auto req = TFileYPathProxy::Fetch(path);
        ToProto(req->mutable_ranges(), std::vector<TReadRange>({TReadRange()}));
        SetTransactionId(req, Operation->GetInputTransaction());
        req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
        batchReq->AddRequest(req, "fetch_regular_files");
    }

    for (const auto& file : TableFiles) {
        auto path = file.Path.GetPath();
        auto req = TTableYPathProxy::Fetch(path);
        ToProto(req->mutable_ranges(), std::vector<TReadRange>({TReadRange()}));
        req->set_fetch_all_meta_extensions(true);
        InitializeFetchRequest(req.Get(), file.Path);
        SetTransactionId(req, Operation->GetInputTransaction());
        batchReq->AddRequest(req, "fetch_table_files");
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error requesting file objects");

    auto fetchRegularFileRsps = batchRsp->GetResponses<TFileYPathProxy::TRspFetch>("fetch_regular_files");
    for (int index = 0; index < static_cast<int>(RegularFiles.size()); ++index) {
        auto& file = RegularFiles[index];
        auto rsp = fetchRegularFileRsps[index];
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error fetching regular file %v", file.Path.GetPath());

        file.FetchResponse.Swap(rsp.Get());

        LOG_INFO("Regular file fetched (Path: %v)", file.Path.GetPath());
    }

    auto fetchTableFileRsps = batchRsp->GetResponses<TTableYPathProxy::TRspFetch>("fetch_table_files");
    for (int index = 0; index < static_cast<int>(TableFiles.size()); ++index) {
        auto& file = TableFiles[index];
        auto rsp = fetchTableFileRsps[index];
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error fetching table file chunks");

        NodeDirectory->MergeFrom(rsp->node_directory());

        file.FetchResponse.Swap(rsp.Get());
        LOG_INFO("Table file fetched (Path: %v)", file.Path.GetPath());
    }
}

void TOperationControllerBase::RequestFileObjects()
{
    LOG_INFO("Requesting file objects");

    TObjectServiceProxy proxy(AuthenticatedMasterClient->GetMasterChannel());
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& file : RegularFiles) {
        auto path = file.Path.GetPath();
        {
            auto req = TCypressYPathProxy::Lock(path);
            req->set_mode(ELockMode::Snapshot);
            GenerateMutationId(req);
            SetTransactionId(req, Operation->GetInputTransaction());
            batchReq->AddRequest(req, "lock_regular_file");
        }
        {
            auto req = TYPathProxy::GetKey(path);
            SetTransactionId(req, Operation->GetInputTransaction());
            batchReq->AddRequest(req, "get_regular_file_name");
        }
        {
            auto req = TYPathProxy::Get(path);
            SetTransactionId(req, Operation->GetOutputTransaction());
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("executable");
            attributeFilter.Keys.push_back("file_name");
            attributeFilter.Keys.push_back("chunk_count");
            attributeFilter.Keys.push_back("uncompressed_data_size");
            ToProto(req->mutable_attribute_filter(), attributeFilter);
            batchReq->AddRequest(req, "get_regular_file_attributes");
        }
    }

    for (const auto& file : TableFiles) {
        auto path = file.Path.GetPath();
        {
            auto req = TCypressYPathProxy::Lock(path);
            req->set_mode(ELockMode::Snapshot);
            GenerateMutationId(req);
            SetTransactionId(req, Operation->GetInputTransaction());
            batchReq->AddRequest(req, "lock_table_file");
        }
        {
            auto req = TYPathProxy::GetKey(path);
            SetTransactionId(req, Operation->GetInputTransaction());
            batchReq->AddRequest(req, "get_table_file_name");
        }
        {
            auto req = TYPathProxy::Get(path);
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("chunk_count");
            attributeFilter.Keys.push_back("uncompressed_data_size");
            ToProto(req->mutable_attribute_filter(), attributeFilter);
            SetTransactionId(req, Operation->GetInputTransaction());
            batchReq->AddRequest(req, "get_table_file_attributes");
        }
    }

    auto batchRsp = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error requesting file objects");

    std::vector<yhash_set<Stroka>> userFileNames;
    userFileNames.resize(EOperationStage::GetDomainSize());

    auto validateUserFileName = [&] (const TUserFileBase& userFile) {
        // TODO(babenko): more sanity checks?
        auto path = userFile.Path.GetPath();
        const auto& fileName = userFile.FileName;
        if (fileName.empty()) {
            THROW_ERROR_EXCEPTION("Empty user file name for %v",
                path);
        }
        if (!userFileNames[static_cast<int>(userFile.Stage)].insert(fileName).second) {
            THROW_ERROR_EXCEPTION("Duplicate user file name %Qv for %v",
                fileName,
                path);
        }
    };

    {
        auto lockRegularFileRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_regular_file");
        auto getRegularFileNameRsps = batchRsp->GetResponses<TYPathProxy::TRspGetKey>("get_regular_file_name");
        auto getRegularFileAttributesRsps = batchRsp->GetResponses<TYPathProxy::TRspGetKey>("get_regular_file_attributes");
        for (int index = 0; index < static_cast<int>(RegularFiles.size()); ++index) {
            auto& file = RegularFiles[index];
            auto path = file.Path.GetPath();
            {
                auto rsp = lockRegularFileRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error locking regular file %v",
                    path);

                LOG_INFO("Regular file locked (Path: %v)",
                    path);
            }
            {
                auto rsp = getRegularFileNameRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    *rsp,
                    "Error getting file name for regular file %v",
                    path);

                file.FileName = rsp->value();
            }
            {
                auto rsp = getRegularFileAttributesRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting attributes for regular file %v",
                    path);

                auto node = ConvertToNode(TYsonString(rsp->value()));
                const auto& attributes = node->Attributes();

                file.FileName = attributes.Get<Stroka>("file_name", file.FileName);
                file.Executable = attributes.Get<bool>("executable", false);

                i64 fileSize = attributes.Get<i64>("uncompressed_data_size");
                if (fileSize > Config->MaxFileSize) {
                    THROW_ERROR_EXCEPTION(
                        "Regular file %v exceeds size limit: %v > %v",
                        path,
                        fileSize,
                        Config->MaxFileSize);
                }

                i64 chunkCount = attributes.Get<i64>("chunk_count");
                if (chunkCount > Config->MaxChunkCountPerFetch) {
                    THROW_ERROR_EXCEPTION(
                        "Regular file %v exceeds chunk count limit: %v > %v",
                        path,
                        chunkCount,
                        Config->MaxChunkCountPerFetch);
                }

                LOG_INFO("Regular file attributes received (Path: %v)", path);
            }

            file.FileName = file.Path.Attributes().Get<Stroka>("file_name", file.FileName);
            file.Executable = file.Path.Attributes().Get<bool>("executable", file.Executable);

            validateUserFileName(file);
        }
    }

    {
        auto lockTableFileRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_table_file");
        auto getTableFileNameRsps = batchRsp->GetResponses<TYPathProxy::TRspGetKey>("get_table_file_name");
        auto getTableFileAttributesRsps = batchRsp->GetResponses<TTableYPathProxy::TRspGet>("get_table_file_attributes");
        for (int index = 0; index < static_cast<int>(TableFiles.size()); ++index) {
            auto& file = TableFiles[index];
            auto path = file.Path.GetPath();
            {
                auto rsp = lockTableFileRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error locking table file %v",
                    path);

                LOG_INFO("Table file locked (Path: %v)",
                    path);
            }
            {
                auto rsp = getTableFileNameRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting table file name");

                auto key = ConvertTo<Stroka>(TYsonString(rsp->value()));
                file.FileName = file.Path.Attributes().Get<Stroka>("file_name", key);
                file.Format = file.Path.Attributes().GetYson("format");
            }
            {
                auto rsp = getTableFileAttributesRsps[index];
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting table file attributes");

                auto node = ConvertToNode(TYsonString(rsp->value()));
                const auto& attributes = node->Attributes();

                i64 tableFileSize = attributes.Get<i64>("uncompressed_data_size");
                if (tableFileSize > Config->MaxFileSize) {
                    THROW_ERROR_EXCEPTION(
                        "Table file %v exceeds size limit: %v > %v",
                        path,
                        tableFileSize,
                        Config->MaxFileSize);
                }

                i64 chunkCount = attributes.Get<i64>("chunk_count");
                if (chunkCount > Config->MaxChunkCountPerFetch) {
                    THROW_ERROR_EXCEPTION(
                        "Table file %v exceeds chunk count limit: %v > %v",
                        path,
                        chunkCount,
                        Config->MaxChunkCountPerFetch);
                }

                LOG_INFO("Table file attributes received (Path: %v, FileName: %v, Format: %v, Size: %v)",
                    path,
                    file.FileName,
                    file.Format.Data(),
                    tableFileSize);
            }

            validateUserFileName(file);
        }
    }

    FetchFileObjects();

    LOG_INFO("File objects received");
}

void TOperationControllerBase::CollectTotals()
{
    for (const auto& table : InputTables) {
        for (const auto& chunk : table.Chunks) {
            i64 chunkDataSize;
            i64 chunkRowCount;
            i64 chunkValueCount;
            NChunkClient::GetStatistics(chunk, &chunkDataSize, &chunkRowCount, &chunkValueCount);

            TotalInputDataSize += chunkDataSize;
            TotalInputRowCount += chunkRowCount;
            TotalInputValueCount += chunkValueCount;
            ++TotalInputChunkCount;
        }
    }

    LOG_INFO("Input totals collected (ChunkCount: %v, DataSize: %v, RowCount: %v, ValueCount: %v)",
        TotalInputChunkCount,
        TotalInputDataSize,
        TotalInputRowCount,
        TotalInputValueCount);
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

std::vector<TChunkStripePtr> TOperationControllerBase::SliceInputChunks(i64 maxSliceDataSize, int jobCount)
{
    std::vector<TChunkStripePtr> result;
    auto appendStripes = [&] (std::vector<TChunkSlicePtr> slices) {
        for (const auto& slice : slices) {
            result.push_back(New<TChunkStripe>(slice));
        }
    };

    // TODO(ignat): we slice on two parts even id TotalInputDataSize very small.
    i64 sliceDataSize = std::min(maxSliceDataSize, (i64)std::max(Config->SliceDataSizeMultiplier * TotalInputDataSize / jobCount, 1.0));

    for (const auto& chunkSpec : CollectInputChunks()) {
        int oldSize = result.size();

        bool hasNontrivialLimits =
            (chunkSpec->has_upper_limit() && !IsTrivial(chunkSpec->upper_limit())) ||
            (chunkSpec->has_lower_limit() && !IsTrivial(chunkSpec->lower_limit()));

        auto codecId = NErasure::ECodec(chunkSpec->erasure_codec());
        if (hasNontrivialLimits || codecId == NErasure::ECodec::None) {
            auto slices = CreateChunkSlice(chunkSpec)->SliceEvenly(sliceDataSize);
            appendStripes(slices);
        } else {
            for (const auto& slice : CreateErasureChunkSlices(chunkSpec, codecId)) {
                auto slices = slice->SliceEvenly(sliceDataSize);
                appendStripes(slices);
            }
        }

        LOG_TRACE("Slicing chunk (ChunkId: %v, SliceCount: %v)",
            FromProto<TChunkId>(chunkSpec->chunk_id()),
            static_cast<int>(result.size() - oldSize));
    }
    return result;
}

std::vector<Stroka> TOperationControllerBase::CheckInputTablesSorted(const TNullable< std::vector<Stroka> >& keyColumns)
{
    YCHECK(!InputTables.empty());

    for (const auto& table : InputTables) {
        if (!table.KeyColumns) {
            THROW_ERROR_EXCEPTION("Input table %v is not sorted",
                table.Path.GetPath());
        }
    }

    if (keyColumns) {
        for (const auto& table : InputTables) {
            if (!CheckKeyColumnsCompatible(table.KeyColumns.Get(), keyColumns.Get())) {
                THROW_ERROR_EXCEPTION("Input table %v is sorted by columns %v that are not compatible with the requested columns %v",
                    table.Path.GetPath(),
                    ConvertToYsonString(table.KeyColumns.Get(), EYsonFormat::Text).Data(),
                    ConvertToYsonString(keyColumns.Get(), EYsonFormat::Text).Data());
            }
        }
        return keyColumns.Get();
    } else {
        const auto& referenceTable = InputTables[0];
        for (const auto& table : InputTables) {
            if (table.KeyColumns != referenceTable.KeyColumns) {
                THROW_ERROR_EXCEPTION("Key columns do not match: input table %v is sorted by columns %v while input table %v is sorted by columns %v",
                    table.Path.GetPath(),
                    ConvertToYsonString(table.KeyColumns.Get(), EYsonFormat::Text).Data(),
                    referenceTable.Path.GetPath(),
                    ConvertToYsonString(referenceTable.KeyColumns.Get(), EYsonFormat::Text).Data());
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

EAbortReason TOperationControllerBase::GetAbortReason(TJobPtr job)
{
    auto error = FromProto<TError>(job->Result().error());
    return error.Attributes().Get<EAbortReason>("abort_reason", EAbortReason::Scheduler);
}

EAbortReason TOperationControllerBase::GetAbortReason(TJobletPtr joblet)
{
    return joblet->Job ? GetAbortReason(joblet->Job) : EAbortReason(EAbortReason::Other);
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
    const TOldBoundaryKeysExt& boundaryKeys,
    int key,
    TOutputTable* outputTable)
{
    YCHECK(CompareKeys(boundaryKeys.start(), boundaryKeys.end()) <= 0);
    {
        TEndpoint endpoint;
        FromProto(&endpoint.Key, boundaryKeys.start());
        endpoint.Left = true;
        endpoint.ChunkTreeKey = key;
        outputTable->Endpoints.push_back(endpoint);
    }
    {
        TEndpoint endpoint;
        FromProto(&endpoint.Key, boundaryKeys.end());
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

    if (table.Options->KeyColumns && IsSortedOutputSupported()) {
        auto boundaryKeys = GetProtoExtension<TOldBoundaryKeysExt>(chunkSpec->chunk_meta().extensions());
        RegisterEndpoints(boundaryKeys, key, &table);
    }

    RegisterOutput(FromProto<TChunkId>(chunkSpec->chunk_id()), key, tableIndex, table);
}

void TOperationControllerBase::RegisterOutput(
    TJobletPtr joblet,
    int key)
{
    // Update output statistics.
    const auto& jobResult = joblet->Job->Result();
    const auto& outputStatistics = jobResult.statistics().output();
    TotalOutputChunkCount += outputStatistics.chunk_count();
    TotalOutputRowCount += outputStatistics.row_count();
    TotalOutputDataSize += outputStatistics.uncompressed_data_size();

    const auto* userJobResult = FindUserJobResult(joblet);

    for (int tableIndex = 0; tableIndex < static_cast<int>(OutputTables.size()); ++tableIndex) {
        auto& table = OutputTables[tableIndex];
        RegisterOutput(joblet->ChunkListIds[tableIndex], key, tableIndex, table);

        if (table.Options->KeyColumns && IsSortedOutputSupported()) {
            YCHECK(userJobResult);
            auto& boundaryKeys = userJobResult->output_boundary_keys(tableIndex);
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
    // Update output statistics.
    const auto& jobResult = joblet->Job->Result();
    const auto& outputStatistics = jobResult.statistics().output();
    TotalIntermeidateChunkCount += outputStatistics.chunk_count();
    TotalIntermediateRowCount += outputStatistics.row_count();
    TotalIntermediateDataSize += outputStatistics.uncompressed_data_size();

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
    YCHECK(JobletMap.insert(std::make_pair(joblet->Job->GetId(), joblet)).second);
}

TOperationControllerBase::TJobletPtr TOperationControllerBase::GetJoblet(TJobPtr job)
{
    auto it = JobletMap.find(job->GetId());
    YCHECK(it != JobletMap.end());
    return it->second;
}

void TOperationControllerBase::RemoveJoblet(TJobPtr job)
{
    YCHECK(JobletMap.erase(job->GetId()) == 1);
}

void TOperationControllerBase::BuildProgress(IYsonConsumer* consumer) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    BuildYsonMapFluently(consumer)
        .Item("jobs").Value(JobCounter)
        .Item("ready_job_count").Value(GetPendingJobCount())
        .Item("statistics").Value(Statistics)
        .Item("job_statistics").BeginMap()
            .Item("completed").Value(CompletedJobStatistics)
            .Item("failed").Value(FailedJobStatistics)
            .Item("aborted").Value(AbortedJobStatistics)
        .EndMap()
        .Item("input_statistics").BeginMap()
            .Item("chunk_count").Value(TotalInputChunkCount)
            .Item("uncompressed_data_size").Value(TotalInputDataSize)
            .Item("row_count").Value(TotalInputRowCount)
            .Item("unavailable_chunk_count").Value(UnavailableInputChunkCount)
        .EndMap()
        .Item("intermediate_statistics").BeginMap()
            .Item("chunk_count").Value(TotalIntermeidateChunkCount)
            .Item("uncompressed_data_size").Value(TotalIntermediateDataSize)
            .Item("row_count").Value(TotalIntermediateRowCount)
        .EndMap()
        .Item("output_statistics").BeginMap()
            .Item("chunk_count").Value(TotalOutputChunkCount)
            .Item("uncompressed_data_size").Value(TotalOutputDataSize)
            .Item("row_count").Value(TotalOutputRowCount)
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
    TNullable<int> configJobCount) const
{
    i64 suggestionBySize = 1 + totalDataSize / dataSizePerJob;
    i64 jobCount = configJobCount.Get(suggestionBySize);
    return static_cast<int>(Clamp(jobCount, 1, Config->MaxJobCount));
}

void TOperationControllerBase::InitUserJobSpecTemplate(
    NScheduler::NProto::TUserJobSpec* jobSpec,
    TUserJobSpecPtr config,
    const std::vector<TRegularUserFile>& regularFiles,
    const std::vector<TUserTableFile>& tableFiles)
{
    jobSpec->set_shell_command(config->Command);
    jobSpec->set_memory_limit(config->MemoryLimit);
    jobSpec->set_use_yamr_descriptors(config->UseYamrDescriptors);
    jobSpec->set_check_input_fully_consumed(config->CheckInputStreamFullyConsumed);
    jobSpec->set_max_stderr_size(config->MaxStderrSize);
    jobSpec->set_enable_core_dump(config->EnableCoreDump);
    jobSpec->set_enable_vm_limit(Config->EnableVMLimit);
    jobSpec->set_enable_io_prio(config->EnableIOPrio);
    jobSpec->set_enable_accounting(Config->EnableAccounting);

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

    jobSpec->add_environment(Format("YT_OPERATION_ID=%v", Operation->GetId()));

    for (const auto& file : regularFiles) {
        auto *descriptor = jobSpec->add_regular_files();
        *descriptor->mutable_file() = file.FetchResponse;
        descriptor->set_executable(file.Executable);
        descriptor->set_file_name(file.FileName);
    }

    for (const auto& file : tableFiles) {
        auto* descriptor = jobSpec->add_table_files();
        *descriptor->mutable_table() = file.FetchResponse;
        descriptor->set_file_name(file.FileName);
        descriptor->set_format(file.Format.Data());
    }
}

void TOperationControllerBase::InitUserJobSpec(
    NScheduler::NProto::TUserJobSpec* jobSpec,
    TJobletPtr joblet,
    i64 memoryReserve)
{
    auto stderrTransactionId = Operation->GetAsyncSchedulerTransaction()->GetId();
    ToProto(jobSpec->mutable_stderr_transaction_id(), stderrTransactionId);

    jobSpec->set_memory_reserve(memoryReserve);

    jobSpec->add_environment(Format("YT_JOB_INDEX=%v", joblet->JobIndex));
    jobSpec->add_environment(Format("YT_JOB_ID=%v", joblet->Job->GetId()));
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
    config->TableWriter->EnableCaching = true;

    // Don't move intermediate chunks.
    config->TableWriter->ChunksMovable = false;

    // Don't sync intermediate chunks.
    config->TableWriter->SyncOnClose = false;
}

void TOperationControllerBase::InitFinalOutputConfig(TJobIOConfigPtr config)
{
    UNUSED(config);
}

TFluentLogEvent TOperationControllerBase::LogEventFluently(ELogEventType eventType)
{
    return EventLogger.LogEventFluently(Host->GetEventLogConsumer())
        .Item("timestamp").Value(Now())
        .Item("event_type").Value(eventType)
        .Item("operation_id").Value(Operation->GetId());
}

TFluentLogEvent TOperationControllerBase::LogFinishedJobFluently(ELogEventType eventType, TJobPtr job)
{
    const auto& result = job->Result();
    const auto& statistics = result.statistics();

    return LogEventFluently(eventType)
        .Item("job_id").Value(job->GetId())
        .Item("operation_id").Value(job->GetOperation()->GetId())
        .Item("start_time").Value(job->GetStartTime())
        .Item("finish_time").Value(job->GetFinishTime())
        .Item("resource_limits").Value(job->ResourceLimits())
        .Item("statistics").Value(statistics)
        .Item("node_address").Value(job->GetNode()->GetAddress());
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

const NProto::TUserJobResult* TOperationControllerBase::FindUserJobResult(TJobletPtr joblet)
{
    const auto& result = joblet->Job->Result();

    if (result.HasExtension(TReduceJobResultExt::reduce_job_result_ext)) {
        return &result
               .GetExtension(TReduceJobResultExt::reduce_job_result_ext)
               .reducer_result();
    }

    if (result.HasExtension(TMapJobResultExt::map_job_result_ext)) {
        return &result
               .GetExtension(TMapJobResultExt::map_job_result_ext)
               .mapper_result();
    }

    return nullptr;
}

void TOperationControllerBase::Persist(TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, TotalInputChunkCount);
    Persist(context, TotalInputDataSize);
    Persist(context, TotalInputRowCount);
    Persist(context, TotalInputValueCount);

    Persist(context, TotalIntermeidateChunkCount);
    Persist(context, TotalIntermediateDataSize);
    Persist(context, TotalIntermediateRowCount);

    Persist(context, TotalOutputChunkCount);
    Persist(context, TotalOutputDataSize);
    Persist(context, TotalOutputRowCount);

    Persist(context, UnavailableInputChunkCount);

    Persist(context, JobCounter);

    Persist(context, CompletedJobStatistics);
    Persist(context, FailedJobStatistics);
    Persist(context, AbortedJobStatistics);

    Persist(context, NodeDirectory);

    Persist(context, InputTables);

    Persist(context, OutputTables);

    Persist(context, IntermediateTable);

    Persist(context, RegularFiles);

    Persist(context, TableFiles);

    Persist(context, Tasks);

    Persist(context, TaskGroups);

    Persist(context, InputChunkMap);

    Persist(context, CachedPendingJobCount);

    Persist(context, CachedNeededResources);

    Persist(context, ChunkOriginMap);

    Persist(context, JobletMap);

    Persist(context, JobIndexGenerator);

    // NB: Scheduler snapshots need not be stable.
    CustomPersist<
        TSetSerializer<
            TDefaultSerializer,
            TUnsortedTag
        >
    >(context, InputChunkSpecs);

    if (context.GetDirection() == EPersistenceDirection::Load) {
        for (auto task : Tasks) {
            task->Initialize();
        }
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

