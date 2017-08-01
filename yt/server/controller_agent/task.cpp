#include "task.h"

#include "chunk_list_pool.h"
#include "job_info.h"
#include "job_splitter.h"
#include "task_host.h"
#include "job_memory.h"
#include "job_metrics_updater.h"

#include <yt/server/scheduler/config.h>
#include <yt/server/scheduler/scheduling_context.h>

#include <yt/ytlib/chunk_client/chunk_slice.h>

#include <yt/ytlib/scheduler/job.pb.h>

#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/job_tracker_client/job.pb.h>

#include <yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/core/misc/digest.h>

#include <yt/core/ytree/convert.h>


namespace NYT {
namespace NControllerAgent {

using namespace NChunkClient;
using namespace NChunkPools;
using namespace NJobTrackerClient::NProto;
using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TTask::TTask()
    : Logger(OperationLogger)
    , CachedPendingJobCount_(-1)
    , CachedTotalJobCount_(-1)
    , DemandSanityCheckDeadline_(0)
    , CompletedFired_(false)
{ }

TTask::TTask(ITaskHostPtr taskHost)
    : Logger(OperationLogger)
    , TaskHost_(taskHost.Get())
    , CachedPendingJobCount_(0)
    , CachedTotalJobCount_(0)
    , DemandSanityCheckDeadline_(0)
    , CompletedFired_(false)
{ }

void TTask::Initialize()
{
    Logger.AddTag("OperationId: %v", TaskHost_->GetOperationId());
    Logger.AddTag("Task: %v", GetId());
}

int TTask::GetPendingJobCount() const
{
    return GetChunkPoolOutput()->GetPendingJobCount();
}

int TTask::GetPendingJobCountDelta()
{
    int oldValue = CachedPendingJobCount_;
    int newValue = GetPendingJobCount();
    CachedPendingJobCount_ = newValue;
    return newValue - oldValue;
}

int TTask::GetTotalJobCount() const
{
    return GetChunkPoolOutput()->GetTotalJobCount();
}

int TTask::GetTotalJobCountDelta()
{
    int oldValue = CachedTotalJobCount_;
    int newValue = GetTotalJobCount();
    CachedTotalJobCount_ = newValue;
    return newValue - oldValue;
}

TNullable<i64> TTask::GetMaximumUsedTmpfsSize() const
{
    return MaximumUsedTmfpsSize_;
}

const TProgressCounter& TTask::GetJobCounter() const
{
    return GetChunkPoolOutput()->GetJobCounter();
}

TJobResources TTask::GetTotalNeededResourcesDelta()
{
    auto oldValue = CachedTotalNeededResources_;
    auto newValue = GetTotalNeededResources();
    CachedTotalNeededResources_ = newValue;
    newValue -= oldValue;
    return newValue;
}

TJobResources TTask::GetTotalNeededResources() const
{
    i64 count = GetPendingJobCount();
    // NB: Don't call GetMinNeededResources if there are no pending jobs.
    return count == 0 ? ZeroJobResources() : GetMinNeededResources() * count;
}

bool TTask::IsIntermediateOutput() const
{
    return false;
}

bool TTask::IsStderrTableEnabled() const
{
    // We write stderr if corresponding options were specified and only for user-type jobs.
    // For example we don't write stderr for sort stage in mapreduce operation
    // even if stderr table were specified.
    return TaskHost_->GetStderrTablePath() && GetUserJobSpec();
}

bool TTask::IsCoreTableEnabled() const
{
    // Same as above.
    return TaskHost_->GetCoreTablePath() && GetUserJobSpec();
}

i64 TTask::GetLocality(TNodeId nodeId) const
{
    return HasInputLocality()
           ? GetChunkPoolOutput()->GetLocality(nodeId)
           : 0;
}

bool TTask::HasInputLocality() const
{
    return true;
}

void TTask::AddInput(TChunkStripePtr stripe)
{
    TaskHost_->RegisterInputStripe(stripe, this);
    if (HasInputLocality()) {
        TaskHost_->AddTaskLocalityHint(stripe, this);
    }
    AddPendingHint();
}

void TTask::AddInput(const std::vector<TChunkStripePtr>& stripes)
{
    for (auto stripe : stripes) {
        if (stripe) {
            AddInput(stripe);
        }
    }
}

void TTask::FinishInput()
{
    LOG_DEBUG("Task input finished");

    GetChunkPoolInput()->Finish();
    AddPendingHint();
    CheckCompleted();
}

void TTask::CheckCompleted()
{
    if (!CompletedFired_ && IsCompleted()) {
        CompletedFired_ = true;
        OnTaskCompleted();
    }
}

TUserJobSpecPtr TTask::GetUserJobSpec() const
{
    return nullptr;
}

void TTask::ScheduleJob(
    ISchedulingContext* context,
    const TJobResources& jobLimits,
    TScheduleJobResult* scheduleJobResult)
{
    if (!CanScheduleJob(context, jobLimits)) {
        scheduleJobResult->RecordFail(EScheduleJobFailReason::TaskRefusal);
        return;
    }

    bool intermediateOutput = IsIntermediateOutput();
    int jobIndex = TaskHost_->NextJobIndex();
    auto joblet = New<TJoblet>(TaskHost_->CreateJobMetricsUpdater(), this, jobIndex);

    const auto& nodeResourceLimits = context->ResourceLimits();
    auto nodeId = context->GetNodeDescriptor().Id;
    const auto& address = context->GetNodeDescriptor().Address;

    auto* chunkPoolOutput = GetChunkPoolOutput();
    auto localityNodeId = HasInputLocality() ? nodeId : InvalidNodeId;
    joblet->OutputCookie = chunkPoolOutput->Extract(localityNodeId);
    if (joblet->OutputCookie == IChunkPoolOutput::NullCookie) {
        LOG_DEBUG("Job input is empty");
        scheduleJobResult->RecordFail(EScheduleJobFailReason::EmptyInput);
        return;
    }

    int sliceCount = chunkPoolOutput->GetStripeListSliceCount(joblet->OutputCookie);
    const auto& jobSpecSliceThrottler = context->GetJobSpecSliceThrottler();
    if (sliceCount > TaskHost_->SchedulerConfig()->HeavyJobSpecSliceCountThreshold) {
        if (!jobSpecSliceThrottler->TryAcquire(sliceCount)) {
            LOG_DEBUG("Job spec throttling is active (SliceCount: %v)",
                      sliceCount);
            chunkPoolOutput->Aborted(joblet->OutputCookie, EAbortReason::SchedulingJobSpecThrottling);
            scheduleJobResult->RecordFail(EScheduleJobFailReason::JobSpecThrottling);
            return;
        }
    } else {
        jobSpecSliceThrottler->Acquire(sliceCount);
    }

    joblet->InputStripeList = chunkPoolOutput->GetStripeList(joblet->OutputCookie);
    auto estimatedResourceUsage = GetNeededResources(joblet);
    auto neededResources = ApplyMemoryReserve(estimatedResourceUsage);

    joblet->EstimatedResourceUsage = estimatedResourceUsage;
    joblet->ResourceLimits = neededResources;

    // Check the usage against the limits. This is the last chance to give up.
    if (!Dominates(jobLimits, neededResources)) {
        LOG_DEBUG("Job actual resource demand is not met (Limits: %v, Demand: %v)",
                  FormatResources(jobLimits),
                  FormatResources(neededResources));
        CheckResourceDemandSanity(nodeResourceLimits, neededResources);
        chunkPoolOutput->Aborted(joblet->OutputCookie, EAbortReason::SchedulingOther);
        // Seems like cached min needed resources are too optimistic.
        ResetCachedMinNeededResources();
        scheduleJobResult->RecordFail(EScheduleJobFailReason::NotEnoughResources);
        return;
    }

    // Async part.
    auto jobSpecBuilder = BIND([=, this_ = MakeStrong(this)] (TJobSpec* jobSpec) {
        BuildJobSpec(joblet, jobSpec);
        jobSpec->set_version(GetJobSpecVersion());
        TaskHost_->CustomizeJobSpec(joblet, jobSpec);

        auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        if (TaskHost_->Spec()->JobProxyMemoryOvercommitLimit) {
            schedulerJobSpecExt->set_job_proxy_memory_overcommit_limit(*TaskHost_->Spec()->JobProxyMemoryOvercommitLimit);
        }
        schedulerJobSpecExt->set_job_proxy_ref_counted_tracker_log_period(ToProto<i64>(TaskHost_->Spec()->JobProxyRefCountedTrackerLogPeriod));
        schedulerJobSpecExt->set_abort_job_if_account_limit_exceeded(TaskHost_->Spec()->SuspendOperationIfAccountLimitExceeded);

        // Adjust sizes if approximation flag is set.
        if (joblet->InputStripeList->IsApproximate) {
            schedulerJobSpecExt->set_input_data_weight(static_cast<i64>(
                schedulerJobSpecExt->input_data_weight() *
                ApproximateSizesBoostFactor));
            schedulerJobSpecExt->set_input_row_count(static_cast<i64>(
                schedulerJobSpecExt->input_row_count() *
                ApproximateSizesBoostFactor));
        }

        if (schedulerJobSpecExt->input_data_weight() > TaskHost_->Spec()->MaxDataWeightPerJob) {
            TaskHost_->OnOperationFailed(TError(
                "Maximum allowed data weight per job violated: %v > %v",
                schedulerJobSpecExt->input_data_weight(),
                TaskHost_->Spec()->MaxDataWeightPerJob));
        }
    });

    auto jobType = GetJobType();
    joblet->JobId = context->GenerateJobId();
    auto restarted = LostJobCookieMap.find(joblet->OutputCookie) != LostJobCookieMap.end();
    joblet->Account = TaskHost_->Spec()->JobNodeAccount;
    scheduleJobResult->JobStartRequest.Emplace(
        joblet->JobId,
        jobType,
        neededResources,
        TaskHost_->IsJobInterruptible(),
        jobSpecBuilder);

    joblet->Restarted = restarted;
    joblet->JobType = jobType;
    joblet->NodeDescriptor = context->GetNodeDescriptor();
    joblet->JobProxyMemoryReserveFactor = TaskHost_->GetJobProxyMemoryDigest(jobType)->GetQuantile(
        TaskHost_->SchedulerConfig()->JobProxyMemoryReserveQuantile);
    auto userJobSpec = GetUserJobSpec();
    if (userJobSpec) {
        joblet->UserJobMemoryReserveFactor = TaskHost_->GetUserJobMemoryDigest(GetJobType())->GetQuantile(
            TaskHost_->SchedulerConfig()->UserJobMemoryReserveQuantile);
    }

    LOG_DEBUG(
        "Job scheduled (JobId: %v, OperationId: %v, JobType: %v, Address: %v, JobIndex: %v, OutputCookie: %v, SliceCount: %v (%v local), "
        "Approximate: %v, DataWeight: %v (%v local), RowCount: %v, Restarted: %v, EstimatedResourceUsage: %v, JobProxyMemoryReserveFactor: %v, "
        "UserJobMemoryReserveFactor: %v, ResourceLimits: %v)",
        joblet->JobId,
        TaskHost_->GetOperationId(),
        jobType,
        address,
        jobIndex,
        joblet->OutputCookie,
        joblet->InputStripeList->TotalChunkCount,
        joblet->InputStripeList->LocalChunkCount,
        joblet->InputStripeList->IsApproximate,
        joblet->InputStripeList->TotalDataWeight,
        joblet->InputStripeList->LocalDataWeight,
        joblet->InputStripeList->TotalRowCount,
        restarted,
        FormatResources(estimatedResourceUsage),
        joblet->JobProxyMemoryReserveFactor,
        joblet->UserJobMemoryReserveFactor,
        FormatResources(neededResources));

    // Prepare chunk lists.
    if (intermediateOutput) {
        joblet->ChunkListIds.push_back(TaskHost_->ExtractChunkList(TaskHost_->GetIntermediateOutputCellTag()));
    } else {
        for (const auto& table : TaskHost_->OutputTables()) {
            joblet->ChunkListIds.push_back(TaskHost_->ExtractChunkList(table.CellTag));
        }
    }

    if (TaskHost_->StderrTable() && IsStderrTableEnabled()) {
        joblet->StderrTableChunkListId = TaskHost_->ExtractChunkList(TaskHost_->StderrTable()->CellTag);
    }

    if (TaskHost_->CoreTable() && IsCoreTableEnabled()) {
        joblet->CoreTableChunkListId = TaskHost_->ExtractChunkList(TaskHost_->CoreTable()->CellTag);
    }

    // Sync part.
    PrepareJoblet(joblet);
    TaskHost_->CustomizeJoblet(joblet);

    TaskHost_->RegisterJoblet(joblet);
    TaskHost_->AddValueToEstimatedHistogram(joblet);

    OnJobStarted(joblet);

    if (TaskHost_->JobSplitter()) {
        TaskHost_->JobSplitter()->OnJobStarted(joblet->JobId, joblet->InputStripeList);
    }
}

bool TTask::IsPending() const
{
    return GetChunkPoolOutput()->GetPendingJobCount() > 0;
}

bool TTask::IsCompleted() const
{
    return IsActive() && GetChunkPoolOutput()->IsCompleted();
}

bool TTask::IsActive() const
{
    return true;
}

i64 TTask::GetTotalDataWeight() const
{
    return GetChunkPoolOutput()->GetTotalDataWeight();
}

i64 TTask::GetCompletedDataWeight() const
{
    return GetChunkPoolOutput()->GetCompletedDataWeight();
}

i64 TTask::GetPendingDataWeight() const
{
    return GetChunkPoolOutput()->GetPendingDataWeight();
}

void TTask::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    // COMPAT(babenko)
    if (context.IsLoad() && context.GetVersion() < 200009) {
        Load<TNullable<TInstant>>(context.LoadContext());
    }

    Persist(context, TaskHost_);

    Persist(context, CachedPendingJobCount_);
    Persist(context, CachedTotalJobCount_);

    Persist(context, CachedTotalNeededResources_);
    Persist(context, CachedMinNeededResources_);

    Persist(context, CompletedFired_);

    Persist(context, LostJobCookieMap);
}

void TTask::PrepareJoblet(TJobletPtr /* joblet */)
{ }

void TTask::OnJobStarted(TJobletPtr joblet)
{ }

void TTask::OnJobCompleted(TJobletPtr joblet, const TCompletedJobSummary& jobSummary)
{
    YCHECK(jobSummary.Statistics);
    const auto& statistics = *jobSummary.Statistics;

    if (!jobSummary.Abandoned) {
        auto outputStatisticsMap = GetOutputDataStatistics(statistics);
        for (int index = 0; index < static_cast<int>(joblet->ChunkListIds.size()); ++index) {
            YCHECK(outputStatisticsMap.find(index) != outputStatisticsMap.end());
            auto outputStatistics = outputStatisticsMap[index];
            if (outputStatistics.chunk_count() == 0) {
                TaskHost_->ChunkListPool()->Reinstall(joblet->ChunkListIds[index]);
                joblet->ChunkListIds[index] = NullChunkListId;
            }
        }

        auto inputStatistics = GetTotalInputDataStatistics(statistics);
        auto outputStatistics = GetTotalOutputDataStatistics(statistics);
        // It's impossible to check row count preservation on interrupted job.
        if (TaskHost_->IsRowCountPreserved() && jobSummary.InterruptReason == EInterruptReason::None) {
            LOG_ERROR_IF(inputStatistics.row_count() != outputStatistics.row_count(),
                "Input/output row count mismatch in completed job (Input: %v, Output: %v, Task: %v)",
                inputStatistics.row_count(),
                outputStatistics.row_count(),
                GetId());
            YCHECK(inputStatistics.row_count() == outputStatistics.row_count());
        }
    } else {
        auto& chunkListIds = joblet->ChunkListIds;
        TaskHost_->ChunkListPool()->Release(chunkListIds);
        std::fill(chunkListIds.begin(), chunkListIds.end(), NullChunkListId);
    }
    GetChunkPoolOutput()->Completed(joblet->OutputCookie, jobSummary);

    TaskHost_->RegisterStderr(joblet, jobSummary);
    TaskHost_->RegisterCores(joblet, jobSummary);

    UpdateMaximumUsedTmpfsSize(statistics);
}

void TTask::ReinstallJob(TJobletPtr joblet, std::function<void()> releaseOutputCookie)
{
    TaskHost_->RemoveValueFromEstimatedHistogram(joblet);
    TaskHost_->ReleaseChunkLists(joblet->ChunkListIds);

    auto list = HasInputLocality()
        ? GetChunkPoolOutput()->GetStripeList(joblet->OutputCookie)
        : nullptr;

    releaseOutputCookie();

    if (HasInputLocality()) {
        for (const auto& stripe : list->Stripes) {
            TaskHost_->AddTaskLocalityHint(stripe, this);
        }
    }
    AddPendingHint();
}

void TTask::OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary)
{
    TaskHost_->RegisterStderr(joblet, jobSummary);
    TaskHost_->RegisterCores(joblet, jobSummary);

    YCHECK(jobSummary.Statistics);
    UpdateMaximumUsedTmpfsSize(*jobSummary.Statistics);

    ReinstallJob(joblet, BIND([=] {GetChunkPoolOutput()->Failed(joblet->OutputCookie);}));
}

void TTask::OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary)
{
    if (joblet->StderrTableChunkListId) {
        TaskHost_->ReleaseChunkLists({joblet->StderrTableChunkListId});
    }
    if (joblet->CoreTableChunkListId) {
        TaskHost_->ReleaseChunkLists({joblet->CoreTableChunkListId});
    }

    ReinstallJob(joblet, BIND([=] {GetChunkPoolOutput()->Aborted(joblet->OutputCookie, jobSummary.AbortReason);}));
}

void TTask::OnJobLost(TCompletedJobPtr completedJob)
{
    YCHECK(LostJobCookieMap.insert(std::make_pair(
        completedJob->OutputCookie,
        completedJob->InputCookie)).second);
}

void TTask::OnTaskCompleted()
{
    LOG_DEBUG("Task completed");
}

bool TTask::CanScheduleJob(
    ISchedulingContext* /*context*/,
    const TJobResources& /*jobLimits*/)
{
    return true;
}

void TTask::DoCheckResourceDemandSanity(
    const TJobResources& neededResources)
{
    if (TaskHost_->ShouldSkipSanityCheck()) {
        return;
    }

    if (!Dominates(*TaskHost_->CachedMaxAvailableExecNodeResources(), neededResources)) {
        // It seems nobody can satisfy the demand.
        TaskHost_->OnOperationFailed(
            TError("No online node can satisfy the resource demand")
                << TErrorAttribute("task", GetId())
                << TErrorAttribute("needed_resources", neededResources));
    }
}

void TTask::CheckResourceDemandSanity(
    const TJobResources& nodeResourceLimits,
    const TJobResources& neededResources)
{
    // The task is requesting more than some node is willing to provide it.
    // Maybe it's OK and we should wait for some time.
    // Or maybe it's not and the task is requesting something no one is able to provide.

    // First check if this very node has enough resources (including those currently
    // allocated by other jobs).
    if (Dominates(nodeResourceLimits, neededResources)) {
        return;
    }

    // Schedule check in controller thread.
    TaskHost_->GetCancelableInvoker()->Invoke(BIND(
        &TTask::DoCheckResourceDemandSanity,
        MakeWeak(this),
        neededResources));
}

void TTask::AddPendingHint()
{
    TaskHost_->AddTaskPendingHint(this);
}

void TTask::AddLocalityHint(TNodeId nodeId)
{
    TaskHost_->AddTaskLocalityHint(nodeId, this);
}

std::unique_ptr<TNodeDirectoryBuilder> TTask::MakeNodeDirectoryBuilder(
    TSchedulerJobSpecExt* schedulerJobSpec)
{
    return TaskHost_->GetOperationType() == EOperationType::RemoteCopy
        ? std::make_unique<TNodeDirectoryBuilder>(
            TaskHost_->InputNodeDirectory(),
            schedulerJobSpec->mutable_input_node_directory())
        : nullptr;
}

void TTask::AddSequentialInputSpec(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    auto directoryBuilder = MakeNodeDirectoryBuilder(schedulerJobSpecExt);
    auto* inputSpec = schedulerJobSpecExt->add_input_table_specs();
    const auto& list = joblet->InputStripeList;
    for (const auto& stripe : list->Stripes) {
        AddChunksToInputSpec(directoryBuilder.get(), inputSpec, stripe);
    }
    UpdateInputSpecTotals(jobSpec, joblet);
}

void TTask::AddParallelInputSpec(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    auto directoryBuilder = MakeNodeDirectoryBuilder(schedulerJobSpecExt);
    const auto& list = joblet->InputStripeList;
    for (const auto& stripe : list->Stripes) {
        auto* inputSpec = stripe->Foreign
                          ? schedulerJobSpecExt->add_foreign_input_table_specs()
                          : schedulerJobSpecExt->add_input_table_specs();
        AddChunksToInputSpec(directoryBuilder.get(), inputSpec, stripe);
    }
    UpdateInputSpecTotals(jobSpec, joblet);
}

void TTask::AddChunksToInputSpec(
    TNodeDirectoryBuilder* directoryBuilder,
    TTableInputSpec* inputSpec,
    TChunkStripePtr stripe)
{
    for (const auto& dataSlice : stripe->DataSlices) {
        inputSpec->add_chunk_spec_count_per_data_slice(dataSlice->ChunkSlices.size());
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            auto newChunkSpec = inputSpec->add_chunk_specs();
            ToProto(newChunkSpec, chunkSlice, dataSlice->Type);
            if (dataSlice->Tag) {
                newChunkSpec->set_data_slice_tag(*dataSlice->Tag);
            }

            if (directoryBuilder) {
                auto replicas = chunkSlice->GetInputChunk()->GetReplicaList();
                directoryBuilder->Add(replicas);
            }
        }
    }

    if (inputSpec->chunk_specs_size() > 0) {
        // Make spec incompatible with older nodes.
        ToProto(inputSpec->add_data_slice_descriptors(), GetIncompatibleDataSliceDescriptor());
    }
}

void TTask::UpdateInputSpecTotals(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    const auto& list = joblet->InputStripeList;
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    schedulerJobSpecExt->set_input_data_weight(
        schedulerJobSpecExt->input_data_weight() +
        list->TotalDataWeight);
    schedulerJobSpecExt->set_input_row_count(
        schedulerJobSpecExt->input_row_count() +
        list->TotalRowCount);
}

void TTask::AddFinalOutputSpecs(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    YCHECK(joblet->ChunkListIds.size() == TaskHost_->OutputTables().size());
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    for (int index = 0; index < TaskHost_->OutputTables().size(); ++index) {
        const auto& table = TaskHost_->OutputTables()[index];
        auto* outputSpec = schedulerJobSpecExt->add_output_table_specs();
        outputSpec->set_table_writer_options(ConvertToYsonString(table.Options).GetData());
        if (table.WriterConfig) {
            outputSpec->set_table_writer_config(table.WriterConfig.GetData());
        }
        outputSpec->set_timestamp(table.Timestamp);
        ToProto(outputSpec->mutable_table_schema(), table.TableUploadOptions.TableSchema);
        ToProto(outputSpec->mutable_chunk_list_id(), joblet->ChunkListIds[index]);
    }
}

void TTask::AddIntermediateOutputSpec(
    TJobSpec* jobSpec,
    TJobletPtr joblet,
    const TKeyColumns& keyColumns)
{
    YCHECK(joblet->ChunkListIds.size() == 1);
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    auto* outputSpec = schedulerJobSpecExt->add_output_table_specs();

    auto options = New<NTableClient::TTableWriterOptions>();
    options->Account = TaskHost_->Spec()->IntermediateDataAccount;
    options->ChunksVital = false;
    options->ChunksMovable = false;
    options->ReplicationFactor = TaskHost_->Spec()->IntermediateDataReplicationFactor;
    options->MediumName = TaskHost_->Spec()->IntermediateDataMediumName;
    options->CompressionCodec = TaskHost_->Spec()->IntermediateCompressionCodec;
    // Distribute intermediate chunks uniformly across storage locations.
    options->PlacementId = TaskHost_->GetOperationId();

    outputSpec->set_table_writer_options(ConvertToYsonString(options).GetData());

    ToProto(outputSpec->mutable_table_schema(), TTableSchema::FromKeyColumns(keyColumns));
    ToProto(outputSpec->mutable_chunk_list_id(), joblet->ChunkListIds[0]);
}

void TTask::ResetCachedMinNeededResources()
{
    CachedMinNeededResources_.Reset();
}

TJobResources TTask::ApplyMemoryReserve(const TExtendedJobResources& jobResources) const
{
    TJobResources result;
    result.SetCpu(jobResources.GetCpu());
    result.SetUserSlots(jobResources.GetUserSlots());
    i64 memory = jobResources.GetFootprintMemory();
    memory += jobResources.GetJobProxyMemory() * TaskHost_->GetJobProxyMemoryDigest(
        GetJobType())->GetQuantile(TaskHost_->SchedulerConfig()->JobProxyMemoryReserveQuantile);
    if (GetUserJobSpec()) {
        memory += jobResources.GetUserJobMemory() * TaskHost_->GetUserJobMemoryDigest(
            GetJobType())->GetQuantile(TaskHost_->SchedulerConfig()->UserJobMemoryReserveQuantile);
    } else {
        YCHECK(jobResources.GetUserJobMemory() == 0);
    }
    result.SetMemory(memory);
    result.SetNetwork(jobResources.GetNetwork());
    return result;
}

void TTask::UpdateMaximumUsedTmpfsSize(const NJobTrackerClient::TStatistics& statistics) {
    auto maxUsedTmpfsSize = FindNumericValue(
        statistics,
        "/user_job/max_tmpfs_size");

    if (!maxUsedTmpfsSize) {
        return;
    }

    if (!MaximumUsedTmfpsSize_ || *MaximumUsedTmfpsSize_ < *maxUsedTmpfsSize) {
        MaximumUsedTmfpsSize_ = *maxUsedTmpfsSize;
    }
}

void TTask::AddFootprintAndUserJobResources(TExtendedJobResources& jobResources) const
{
    jobResources.SetFootprintMemory(GetFootprintMemorySize());
    auto userJobSpec = GetUserJobSpec();
    if (userJobSpec) {
        jobResources.SetUserJobMemory(userJobSpec->MemoryLimit);
    }
}

TJobResources TTask::GetMinNeededResources() const
{
    if (!CachedMinNeededResources_) {
        YCHECK(GetPendingJobCount() > 0);
        CachedMinNeededResources_ = GetMinNeededResourcesHeavy();
    }
    auto result = ApplyMemoryReserve(*CachedMinNeededResources_);
    if (result.GetUserSlots() > 0 && result.GetMemory() == 0) {
        LOG_WARNING("Found min needed resources of task with non-zero user slots and zero memory");
    }
    return result;
}

void TTask::RegisterIntermediate(
    TJobletPtr joblet,
    TChunkStripePtr stripe,
    TTaskPtr destinationTask,
    bool attachToLivePreview)
{
    RegisterIntermediate(
        joblet,
        stripe,
        destinationTask->GetChunkPoolInput(),
        attachToLivePreview);

    if (destinationTask->HasInputLocality()) {
        TaskHost_->AddTaskLocalityHint(stripe, destinationTask);
    }
    destinationTask->AddPendingHint();
}

void TTask::RegisterIntermediate(
    TJobletPtr joblet,
    TChunkStripePtr stripe,
    IChunkPoolInput* destinationPool,
    bool attachToLivePreview)
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
        joblet->InputStripeList->TotalDataWeight,
        destinationPool,
        inputCookie,
        joblet->NodeDescriptor);

    TaskHost_->RegisterIntermediate(
        joblet,
        completedJob,
        stripe,
        attachToLivePreview);
}

TChunkStripePtr TTask::BuildIntermediateChunkStripe(
    google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs)
{
    auto stripe = New<TChunkStripe>();

    i64 currentTableRowIndex = 0;
    for (int index = 0; index < chunkSpecs->size(); ++index) {
        auto inputChunk = New<TInputChunk>(std::move(*chunkSpecs->Mutable(index)));
        // NB(max42): Having correct table row indices on intermediate data is important for
        // some chunk pools. This affects the correctness of sort operation with sorted
        // merge phase over several intermediate chunks.
        inputChunk->SetTableRowIndex(currentTableRowIndex);
        currentTableRowIndex += inputChunk->GetRowCount();
        auto chunkSlice = CreateInputChunkSlice(std::move(inputChunk));
        auto dataSlice = CreateUnversionedInputDataSlice(std::move(chunkSlice));
        // NB(max42): This heavily relies on the property of intermediate data being deterministic
        // (i.e. it may be reproduced with exactly the same content divided into chunks with exactly
        // the same boundary keys when the job output is lost).
        dataSlice->Tag = index;
        stripe->DataSlices.emplace_back(std::move(dataSlice));
    }
    return stripe;
}

void TTask::RegisterOutput(
    TJobletPtr joblet,
    TOutputChunkTreeKey key,
    const TCompletedJobSummary& jobSummary)
{
    TaskHost_->RegisterOutput(joblet, key, jobSummary);
}

////////////////////////////////////////////////////////////////////////////////

TTaskGroup::TTaskGroup()
{
    MinNeededResources.SetUserSlots(1);
}

void TTaskGroup::Persist(const TPersistenceContext& context)
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
    >(context, NodeIdToTasks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
