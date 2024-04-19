#include "auto_merge_task.h"

#include "auto_merge_director.h"
#include "data_flow_graph.h"
#include "job_info.h"
#include "task_host.h"

#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/job_size_constraints.h>

#include <yt/yt/server/lib/chunk_pools/multi_chunk_pool.h>

#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkClient;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkPools;
using namespace NControllerAgent::NProto;
using namespace NNodeTrackerClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TAutoMergeChunkPoolAdapter::TAutoMergeChunkPoolAdapter(
    IPersistentChunkPoolPtr underlyingPool,
    int poolIndex,
    TAutoMergeTask* task)
    : TChunkPoolAdapterBase(underlyingPool)
    , Task_(task)
    , PoolIndex_(poolIndex)
{
    UnderlyingOutput_->GetJobCounter()->AddParent(JobCounter_);

    SetupCallbacks();
    UpdatePendingJobCount();
}

IChunkPoolInput::TCookie TAutoMergeChunkPoolAdapter::AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key)
{
    Task_->GetTaskHost()->GetAutoMergeDirector()->AccountMergeInputChunks(/*intermediateChunkCount*/ stripe->GetChunkCount());

    Task_->CurrentChunkCounts_[PoolIndex_] += stripe->GetChunkCount();

    auto cookie = TChunkPoolInputAdapterBase::AddWithKey(stripe, key);
    if (std::ssize(CookieChunkCount_) <= cookie) {
        CookieChunkCount_.resize(cookie + 1);
    }
    CookieChunkCount_[cookie] = stripe->GetChunkCount();

    return cookie;
}

IChunkPoolInput::TCookie TAutoMergeChunkPoolAdapter::Add(TChunkStripePtr stripe)
{
    return AddWithKey(stripe, TChunkStripeKey());
}

void TAutoMergeChunkPoolAdapter::Suspend(IChunkPoolInput::TCookie cookie)
{
    Task_->GetTaskHost()->GetAutoMergeDirector()->AccountMergeInputChunks(-CookieChunkCount_[cookie]);

    Task_->CurrentChunkCounts_[PoolIndex_] -= CookieChunkCount_[cookie];

    TChunkPoolAdapterBase::Suspend(cookie);
}

const TProgressCounterPtr& TAutoMergeChunkPoolAdapter::GetJobCounter() const
{
    return JobCounter_;
}

IChunkPoolOutput::TCookie TAutoMergeChunkPoolAdapter::Extract(TNodeId nodeId)
{
    if (GetJobCounter()->GetPending() > 0) {
        return UnderlyingOutput_->Extract(nodeId);
    } else {
        return IChunkPoolOutput::NullCookie;
    }
}

void TAutoMergeChunkPoolAdapter::SetShouldScheduleJob(bool shouldScheduleJob)
{
    ShouldScheduleJob_ = shouldScheduleJob;

    UpdatePendingJobCount();
}

void TAutoMergeChunkPoolAdapter::Persist(const TPersistenceContext& context)
{
    TChunkPoolAdapterBase::Persist(context);

    using NYT::Persist;

    Persist(context, Task_);
    Persist(context, CookieChunkCount_);
    Persist(context, PoolIndex_);
    Persist(context, ShouldScheduleJob_);
    Persist(context, JobCounter_);

    if (context.IsLoad()) {
        SetupCallbacks();
    }
}

void TAutoMergeChunkPoolAdapter::SetupCallbacks()
{
    UnderlyingOutput_->GetJobCounter()->SubscribePendingUpdated(
        BIND(&TAutoMergeChunkPoolAdapter::UpdatePendingJobCount, MakeWeak(this)));
    UnderlyingOutput_->GetJobCounter()->SubscribeBlockedUpdated(
        BIND(&TAutoMergeChunkPoolAdapter::UpdatePendingJobCount, MakeWeak(this)));
}

void TAutoMergeChunkPoolAdapter::UpdatePendingJobCount()
{
    const auto& underlyingJobCounter = UnderlyingOutput_->GetJobCounter();
    int pendingJobCount = underlyingJobCounter->GetPending();
    int blockedJobCount = underlyingJobCounter->GetBlocked();
    if (ShouldScheduleJob_) {
        pendingJobCount += blockedJobCount;
        blockedJobCount = 0;
    }

    JobCounter_->SetPending(pendingJobCount);
    JobCounter_->SetBlocked(blockedJobCount);
}

DEFINE_DYNAMIC_PHOENIX_TYPE(TAutoMergeChunkPoolAdapter);

////////////////////////////////////////////////////////////////////////////////

TAutoMergeTask::TAutoMergeTask(
    ITaskHostPtr taskHost,
    int maxChunksPerJob,
    i64 chunkSizeThreshold,
    i64 dataWeightPerJob,
    i64 maxDataWeightPerJob,
    std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
    std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors)
    : TTask(taskHost, std::move(outputStreamDescriptors), std::move(inputStreamDescriptors))
    , FakeProgressCounters_{
        {EMergeJobType::Deep, New<TProgressCounter>()},
        {EMergeJobType::Shallow, New<TProgressCounter>()},
    }
    , EnableShallowMerge_(taskHost->GetSpec()->AutoMerge->EnableShallowMerge)
{
    ChunkPools_.reserve(OutputStreamDescriptors_.size());
    CurrentChunkCounts_.resize(OutputStreamDescriptors_.size(), 0);
    for (int poolIndex = 0; poolIndex < std::ssize(OutputStreamDescriptors_); ++poolIndex) {
        auto autoMergeJobSizeConstraints = CreateExplicitJobSizeConstraints(
            /*canAdjustDataSizePerJob*/ false,
            /*isExplicitJobCount*/ false,
            /*jobCount*/ 1,
            /*dataWeightPerJob*/ dataWeightPerJob,
            /*primaryDataWeightPerJob*/ std::numeric_limits<i64>::max() / 4,
            /*maxDataSlicesPerJob*/ maxChunksPerJob,
            /*maxDataWeightPerJob*/ std::numeric_limits<i64>::max() / 4,
            /*primaryMaxDataWeightPerJob*/ std::numeric_limits<i64>::max() / 4,
            /*inputSliceDataSize*/ std::numeric_limits<i64>::max() / 4,
            /*inputSliceRowCount*/ std::numeric_limits<i64>::max() / 4,
            /*batchRowCount*/ {},
            /*foreignSliceDataWeight*/ 0,
            /*samplingRate*/ std::nullopt);

        TUnorderedChunkPoolOptions options;
        options.RowBuffer = TaskHost_->GetRowBuffer();
        options.Mode = EUnorderedChunkPoolMode::AutoMerge;
        options.JobSizeConstraints = std::move(autoMergeJobSizeConstraints);
        options.MinTeleportChunkDataWeight = 0.5 * maxDataWeightPerJob;
        options.MinTeleportChunkSize = chunkSizeThreshold;
        options.Logger = Logger.WithTag("Name: %v(%v)", GetTitle(), poolIndex);

        auto unorderedPool = CreateUnorderedChunkPool(
            std::move(options),
            TeleportableIntermediateInputStreamDirectory);
        ChunkPools_.push_back(New<TAutoMergeChunkPoolAdapter>(unorderedPool, poolIndex, this));
    }

    std::vector<IPersistentChunkPoolPtr> underlyingPools;
    for (const auto& chunkPool : ChunkPools_) {
        underlyingPools.push_back(chunkPool);
    }
    auto chunkPool = CreateMultiChunkPool(underlyingPools);
    chunkPool->Finalize();
    ChunkPool_ = std::move(chunkPool);

    ChunkPool_->SubscribeChunkTeleported(BIND(&TAutoMergeTask::OnChunkTeleported, MakeWeak(this)));

    // Tentative trees are not allowed for auto-merge jobs since they are genuinely IO-bound.
    TentativeTreeEligibility_.Disable();

    InitAutoMergeJobSpecTemplates();
}

void TAutoMergeTask::DoRegisterInGraph()
{
    for (auto jobType : TEnumTraits<EMergeJobType>::GetDomainValues()) {
        // NB. The registered counters are never updated, as the data from these counters is not used anywhere.
        TaskHost_->GetDataFlowGraph()
            ->RegisterCounter(GetVertexDescriptorForMergeType(jobType), FakeProgressCounters_[jobType],
                              jobType == EMergeJobType::Deep ? EJobType::UnorderedMerge : EJobType::ShallowMerge);
    }
}

EMergeJobType TAutoMergeTask::GetMergeTypeFromJobType(EJobType jobType) const
{
    YT_VERIFY(jobType == EJobType::ShallowMerge || jobType == EJobType::UnorderedMerge);
    return jobType == EJobType::ShallowMerge ? EMergeJobType::Shallow : EMergeJobType::Deep;
}

EMergeJobType TAutoMergeTask::GetCurrentMergeType() const
{
    return EnableShallowMerge_.load() ? EMergeJobType::Shallow : EMergeJobType::Deep;
}

void TAutoMergeTask::UpdateInputEdges(
    const NChunkClient::NProto::TDataStatistics& dataStatistics,
    const TJobletPtr& joblet)
{
    TaskHost_->GetDataFlowGraph()->UpdateEdgeJobDataStatistics(
        InputVertex_,
        GetVertexDescriptorForJoblet(joblet),
        dataStatistics);
}

void TAutoMergeTask::UpdateOutputEdgesForTeleport(const NChunkClient::NProto::TDataStatistics& dataStatistics)
{
    TaskHost_->GetDataFlowGraph()->UpdateEdgeTeleportDataStatistics(
        GetVertexDescriptorForJoblet(nullptr),
        TDataFlowGraph::SinkDescriptor,
        dataStatistics);
}

TString TAutoMergeTask::GetTitle() const
{
    return Format("AutoMerge");
}

TDataFlowGraph::TVertexDescriptor TAutoMergeTask::GetVertexDescriptor() const
{
    return "auto_merge";
}

TDataFlowGraph::TVertexDescriptor TAutoMergeTask::GetVertexDescriptorForMergeType(EMergeJobType type) const
{
    switch (type) {
        case EMergeJobType::Shallow:
            return "shallow_auto_merge";
        case EMergeJobType::Deep:
            return "auto_merge";
        default:
            YT_ABORT();
    }
}

TDataFlowGraph::TVertexDescriptor TAutoMergeTask::GetVertexDescriptorForJoblet(const TJobletPtr& joblet) const
{
    auto mergeType = joblet
        ? GetMergeTypeFromJobType(joblet->JobType)
        : GetCurrentMergeType();
    return GetVertexDescriptorForMergeType(mergeType);
}

TVertexDescriptorList TAutoMergeTask::GetAllVertexDescriptors() const
{
    return {"auto_merge", "shallow_auto_merge"};
}

TExtendedJobResources TAutoMergeTask::GetNeededResources(const TJobletPtr& joblet) const
{
    auto result = TaskHost_->GetAutoMergeResources(joblet->InputStripeList->GetStatistics());
    AddFootprintAndUserJobResources(result);
    return result;
}

NChunkPools::IPersistentChunkPoolInputPtr TAutoMergeTask::GetChunkPoolInput() const
{
    return ChunkPool_;
}

NChunkPools::IPersistentChunkPoolOutputPtr TAutoMergeTask::GetChunkPoolOutput() const
{
    return ChunkPool_;
}

EJobType TAutoMergeTask::GetJobType() const
{
    return EJobType::UnorderedMerge;
}

void TAutoMergeTask::AddJobTypeToJoblet(const TJobletPtr& joblet) const
{
    bool enableShallowMerge = EnableShallowMerge_.load();

    if (enableShallowMerge) {
        YT_VERIFY(joblet->InputStripeList);
        i64 dataWeight = joblet->InputStripeList->TotalDataWeight;
        i64 chunkCount = joblet->InputStripeList->TotalChunkCount;
        i64 dataWeightPerChunk = dataWeight / std::max<i64>(chunkCount, 1);
        i64 minDataWeightPerChunk = TaskHost_->GetSpec()->AutoMerge->ShallowMergeMinDataWeightPerChunk;
        if (dataWeightPerChunk <= minDataWeightPerChunk) {
            YT_LOG_DEBUG(
                "Falling back to deep merge due to low data weight per chunk "
                "(JobId: %v, DataWeight: %v, ChunkCount: %v, DataWeightPerChunk: %v, ShallowMergeMinDataWeightPerChunk: %v)",
                joblet->JobId,
                dataWeight,
                chunkCount,
                dataWeightPerChunk,
                minDataWeightPerChunk);
            enableShallowMerge = false;
        }
    }

    joblet->JobType = enableShallowMerge ? EJobType::ShallowMerge : EJobType::UnorderedMerge;
}

TExtendedJobResources TAutoMergeTask::GetMinNeededResourcesHeavy() const
{
    auto result = TaskHost_->GetAutoMergeResources(
        ChunkPool_->GetApproximateStripeStatistics());
    AddFootprintAndUserJobResources(result);
    return result;
}

void TAutoMergeTask::BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec)
{
    VERIFY_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

    auto poolIndex = *joblet->InputStripeList->PartitionTag;
    jobSpec->CopyFrom(GetJobSpecTemplate(GetTableIndex(poolIndex), GetMergeTypeFromJobType(joblet->JobType)));
    AddSequentialInputSpec(jobSpec, joblet);
    AddOutputTableSpecs(jobSpec, joblet);
}

bool TAutoMergeTask::IsJobInterruptible() const
{
    return false;
}

void TAutoMergeTask::UpdateSelf()
{
    std::vector<bool> shouldScheduleJobs;
    shouldScheduleJobs.reserve(ChunkPools_.size());
    for (int poolIndex = 0; poolIndex < std::ssize(ChunkPools_); ++poolIndex) {
        const auto& chunkPool = ChunkPools_[poolIndex];
        int currentChunkCount = CurrentChunkCounts_[poolIndex];

        bool shouldScheduleJob = TaskHost_->GetAutoMergeDirector()->ShouldScheduleMergeJob(currentChunkCount);
        chunkPool->SetShouldScheduleJob(shouldScheduleJob);
        shouldScheduleJobs.push_back(shouldScheduleJob);
    }

    TaskHost_->UpdateTask(this);

    YT_LOG_DEBUG("Task updated (ShouldScheduleJobs: %v)", shouldScheduleJobs);
}

void TAutoMergeTask::OnJobStarted(TJobletPtr joblet)
{
    TTask::OnJobStarted(joblet);

    int poolIndex = *joblet->InputStripeList->PartitionTag;
    CurrentChunkCounts_[poolIndex] -= joblet->InputStripeList->TotalChunkCount;

    TaskHost_->GetAutoMergeDirector()->OnMergeJobStarted();
}

TJobFinishedResult TAutoMergeTask::OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary)
{
    auto result = TTask::OnJobAborted(joblet, jobSummary);

    int poolIndex = *joblet->InputStripeList->PartitionTag;
    CurrentChunkCounts_[poolIndex] += joblet->InputStripeList->TotalChunkCount;

    if (jobSummary.AbortReason == EAbortReason::ShallowMergeFailed) {
        if (EnableShallowMerge_.exchange(false)) {
            YT_LOG_DEBUG("Shallow merge failed; switching to deep merge (JobId: %v)", jobSummary.Id);
        }
    }

    TaskHost_->GetAutoMergeDirector()->OnMergeJobFinished(/*unregisteredIntermediateChunkCount*/ 0);

    return result;
}

TJobFinishedResult TAutoMergeTask::OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary)
{
    auto result = TTask::OnJobCompleted(joblet, jobSummary);

    // Deciding what to do with these chunks is up to controller.
    // It may do nothing with these chunks, release them immediately
    // or release after next snapshot built but it should eventually
    // discount them in auto merge director.
    TaskHost_->ReleaseIntermediateStripeList(joblet->InputStripeList);

    RegisterOutput(jobSummary, joblet->ChunkListIds, joblet);

    return result;
}

TJobFinishedResult TAutoMergeTask::OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary)
{
    auto result = TTask::OnJobFailed(joblet, jobSummary);

    int poolIndex = *joblet->InputStripeList->PartitionTag;
    CurrentChunkCounts_[poolIndex] += joblet->InputStripeList->TotalChunkCount;

    TaskHost_->GetAutoMergeDirector()->OnMergeJobFinished(/*unregisteredIntermediateChunkCount*/ 0);

    return result;
}

void TAutoMergeTask::SetupCallbacks()
{
    TTask::SetupCallbacks();

    TaskHost_->GetAutoMergeDirector()->SubscribeStateChanged(BIND(&TAutoMergeTask::UpdateSelf, MakeWeak(this)));
}

bool TAutoMergeTask::IsCompleted() const
{
    return TaskHost_->GetAutoMergeDirector()->IsTaskCompleted() && TTask::IsCompleted();
}

void TAutoMergeTask::Persist(const TPersistenceContext& context)
{
    TTask::Persist(context);

    using NYT::Persist;

    Persist(context, ChunkPools_);
    Persist(context, ChunkPool_);
    Persist(context, CurrentChunkCounts_);
    Persist(context, JobSpecTemplates_);
    Persist(context, EnableShallowMerge_);

    ChunkPool_->SubscribeChunkTeleported(BIND(&TAutoMergeTask::OnChunkTeleported, MakeWeak(this)));
}

void TAutoMergeTask::OnChunkTeleported(TInputChunkPtr teleportChunk, std::any tag)
{
    TTask::OnChunkTeleported(teleportChunk, tag);

    auto poolIndex = std::any_cast<int>(tag);
    TaskHost_->RegisterTeleportChunk(std::move(teleportChunk), /*key*/ 0, GetTableIndex(poolIndex));

    --CurrentChunkCounts_[poolIndex];
    TaskHost_->GetAutoMergeDirector()->AccountMergeInputChunks(-1);
}

void TAutoMergeTask::SetStreamDescriptors(TJobletPtr joblet) const
{
    auto poolIndex = *joblet->InputStripeList->PartitionTag;
    joblet->OutputStreamDescriptors = {OutputStreamDescriptors_[poolIndex]};
    joblet->InputStreamDescriptors = InputStreamDescriptors_;
}

int TAutoMergeTask::GetTableIndex(int poolIndex) const
{
    return *OutputStreamDescriptors_[poolIndex]->PartitionTag;
}

TJobSplitterConfigPtr TAutoMergeTask::GetJobSplitterConfig() const
{
    auto config = TaskHost_->GetJobSplitterConfigTemplate();

    // TODO(gritukan): YT-13646.
    config->EnableJobSplitting = false;

    return config;
}

const TJobSpec& TAutoMergeTask::GetJobSpecTemplate(
    int tableIndex,
    EMergeJobType type) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return JobSpecTemplates_[tableIndex][type];
}

void TAutoMergeTask::InitAutoMergeJobSpecTemplates()
{
    const auto& autoMergeSpec = TaskHost_->GetSpec()->AutoMerge;

    const auto tableCount = TaskHost_->GetOutputTableCount();
    JobSpecTemplates_.resize(tableCount);
    for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
        TJobSpec jobSpecTemplate;
        jobSpecTemplate.set_type(static_cast<int>(EJobType::UnorderedMerge));
        auto* jobSpecExt = jobSpecTemplate
            .MutableExtension(TJobSpecExt::job_spec_ext);
        jobSpecExt->set_table_reader_options(
            ConvertToYsonString(
                CreateTableReaderOptions(autoMergeSpec->JobIO)).ToString());

        const auto& outputTable = TaskHost_->GetOutputTable(tableIndex);

        auto dataSourceDirectory = New<TDataSourceDirectory>();
        // NB: chunks read by auto-merge jobs have table index set to output table index,
        // so we need to specify several unused data sources before actual one.
        dataSourceDirectory->DataSources().resize(tableIndex);
        auto& dataSource = dataSourceDirectory->DataSources().emplace_back(MakeUnversionedDataSource(
            GetIntermediatePath(tableIndex),
            outputTable->TableUploadOptions.TableSchema.Get(),
            /*columns*/ std::nullopt,
            /*omittedInaccessibleColumns*/ {},
            /*columnRenameDescriptors*/ {}));
        dataSource.SetAccount(
            TaskHost_->GetSpec()->AutoMerge->UseIntermediateDataAccount
                ? std::make_optional(TaskHost_->GetSpec()->IntermediateDataAccount)
                : outputTable->Account);

        auto dataSinkDirectory = New<TDataSinkDirectory>();
        dataSinkDirectory->DataSinks().push_back(BuildDataSinkFromOutputTable(outputTable));

        SetProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            dataSourceDirectory);
        SetProtoExtension<NChunkClient::NProto::TDataSinkDirectoryExt>(
            jobSpecExt->mutable_extensions(),
            dataSinkDirectory);
        jobSpecExt->set_io_config(
            ConvertToYsonString(autoMergeSpec->JobIO).ToString());

        auto& shallowTemplate = JobSpecTemplates_[tableIndex][EMergeJobType::Shallow];
        shallowTemplate = jobSpecTemplate;
        shallowTemplate.set_type(static_cast<int>(EJobType::ShallowMerge));
        auto* shallowMergeJobSpecExt = shallowTemplate
            .MutableExtension(TShallowMergeJobSpecExt::shallow_merge_job_spec_ext);
        shallowMergeJobSpecExt->set_allow_unknown_extensions(autoMergeSpec->AllowUnknownExtensions);
        if (autoMergeSpec->MaxBlockCount) {
            shallowMergeJobSpecExt->set_max_block_count(*autoMergeSpec->MaxBlockCount);
        }

        auto& deepTemplate = JobSpecTemplates_[tableIndex][EMergeJobType::Deep];
        deepTemplate = std::move(jobSpecTemplate);
    }
}

DEFINE_DYNAMIC_PHOENIX_TYPE(TAutoMergeTask);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
