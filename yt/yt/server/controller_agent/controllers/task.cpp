#include "task.h"

#include "input_manager.h"
#include "job_info.h"
#include "job_memory.h"
#include "job_splitter.h"
#include "task_host.h"
#include "helpers.h"
#include "data_flow_graph.h"

#include <yt/yt/server/controller_agent/chunk_list_pool.h>
#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/scheduling_context.h>

#include <yt/yt/server/lib/chunk_pools/helpers.h>

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>
#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_builder.h>
#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/ytlib/table_client/chunk_slice.h>

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/config.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkClient;
using namespace NChunkPools;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NStatisticPath;

using NYT::FromProto;
using NYT::ToProto;
using NCrypto::TMD5Hash;
using NProfiling::TWallTimer;
using NControllerAgent::NProto::TJobSpec;
using NControllerAgent::NProto::TJobSpecExt;
using NControllerAgent::NProto::TJobResultExt;
using NControllerAgent::NProto::TTableInputSpec;

////////////////////////////////////////////////////////////////////////////////

TTask::TTask()
    : Logger(ControllerLogger())
    , CachedPendingJobCount_{.DefaultCount = -1}
    , CachedTotalJobCount_(-1)
{ }

TTask::TTask(
    ITaskHostPtr taskHost,
    std::vector<TOutputStreamDescriptorPtr> outputStreamDescriptors,
    std::vector<TInputStreamDescriptorPtr> inputStreamDescriptors)
    : Logger(taskHost->GetLogger())
    , TaskHost_(taskHost.Get())
    , OutputStreamDescriptors_(std::move(outputStreamDescriptors))
    , InputStreamDescriptors_(std::move(inputStreamDescriptors))
    , InputChunkMapping_(New<TInputChunkMapping>(EChunkMappingMode::Sorted, Logger))
    , CachedPendingJobCount_{}
    , CachedTotalJobCount_(0)
    , SpeculativeJobManager_(
        this,
        Logger,
        taskHost->GetSpec()->TryAvoidDuplicatingJobs ? 0 : taskHost->GetSpec()->MaxSpeculativeJobCountPerTask)
    , ProbingJobManager_(
        this,
        Logger,
        taskHost->GetSpec()->TryAvoidDuplicatingJobs ? 0 : taskHost->GetSpec()->MaxProbingJobCountPerTask,
        taskHost->GetSpec()->ProbingRatio,
        taskHost->GetSpec()->ProbingPoolTree)
    , ExperimentJobManager_(
        this,
        taskHost->GetSpec(),
        Logger)
{
    if (TaskHost_->GetSpec()->UseClusterThrottlers) {
        ClusterToNetworkBandwidthAvailabilityUpdatedCallback_ = BIND_NO_PROPAGATE(
            &TTask::UpdateNetworkAndTask,
            MakeWeak(this))
        .Via(TaskHost_->GetCancelableInvoker());
    }
}

const std::vector<TOutputStreamDescriptorPtr>& TTask::GetOutputStreamDescriptors() const
{
    return OutputStreamDescriptors_;
}

const std::vector<TInputStreamDescriptorPtr>& TTask::GetInputStreamDescriptors() const
{
    return InputStreamDescriptors_;
}

void TTask::SetInputStreamDescriptors(std::vector<TInputStreamDescriptorPtr> streamDescriptors)
{
    InputStreamDescriptors_ = std::move(streamDescriptors);
}

void TTask::Initialize()
{
    Logger.AddTag("Task: %v", GetTitle());

    SetupCallbacks();

    if (IsSimpleTask()) {
        if (auto userJobSpec = GetUserJobSpec()) {
            MaximumUsedTmpfsSizes_.resize(userJobSpec->TmpfsVolumes.size());
        }
    }

    ExperimentJobManager_.SetJobExperiment(TaskHost_->GetJobExperiment());
}

void TTask::Prepare()
{
    const auto& spec = TaskHost_->GetSpec();
    const auto& userJobSpec = GetUserJobSpec();

    TentativeTreeEligibility_ = TTentativeTreeEligibility(spec->TentativePoolTrees, spec->TentativeTreeEligibility, Logger);

    if (IsInputDataWeightHistogramSupported()) {
        EstimatedInputDataWeightHistogram_ = CreateHistogram();
        InputDataWeightHistogram_ = CreateHistogram();
    }

    JobSplitter_ = CreateJobSplitter(
        GetJobSplitterConfig(),
        GetChunkPoolOutput().Get(),
        Logger);

    if (TaskHost_->GetConfig()->UseResourceOverdraftMemoryMultiplierFromSpec) {
        if (userJobSpec) {
            UserJobMemoryMultiplier_ = userJobSpec->UserJobResourceOverdraftMemoryMultiplier;
            JobProxyMemoryMultiplier_ = userJobSpec->JobProxyResourceOverdraftMemoryMultiplier;
        }
        if (!JobProxyMemoryMultiplier_) {
            JobProxyMemoryMultiplier_ = spec->JobProxyResourceOverdraftMemoryMultiplier;
        }
    } else {
        UserJobMemoryMultiplier_ = TaskHost_->GetConfig()->UserJobResourceOverdraftMemoryMultiplier;
        JobProxyMemoryMultiplier_ = TaskHost_->GetConfig()->JobProxyResourceOverdraftMemoryMultiplier;
    }
}

TString TTask::GetTitle() const
{
    return ToString(GetJobType());
}

void TTask::AddJobTypeToJoblet(const TJobletPtr& joblet) const
{
    joblet->JobType = GetJobType();
}

TDataFlowGraph::TVertexDescriptor TTask::GetVertexDescriptor() const
{
    return FormatEnum(GetJobType());
}

TDataFlowGraph::TVertexDescriptor TTask::GetVertexDescriptorForJoblet(const TJobletPtr& /*joblet*/) const
{
    return GetVertexDescriptor();
}

TVertexDescriptorList TTask::GetAllVertexDescriptors() const
{
    return {GetVertexDescriptor()};
}

TCompositePendingJobCount TTask::GetPendingJobCount() const
{
    if (!IsActive()) {
        return TCompositePendingJobCount{};
    }

    TCompositePendingJobCount result;

    result.DefaultCount = GetChunkPoolOutput()->GetJobCounter()->GetPending() +
        SpeculativeJobManager_.GetPendingJobCount() +
        ExperimentJobManager_.GetPendingJobCount();

    ProbingJobManager_.UpdatePendingJobCount(&result);

    if (auto userJobSpec = GetUserJobSpec()) {
        if (userJobSpec->NetworkProject) {
            const auto& allowedNetworkProjects = TaskHost_->GetConfig()->NetworkProjectsAllowedForOffloading;
            if (!allowedNetworkProjects.contains(*userJobSpec->NetworkProject)) {
                for (const auto& tree : TaskHost_->GetOffloadingPoolTrees()) {
                    result.CountByPoolTree[tree] = 0;
                }
            }
        }
    }

    return result;
}

TCompositePendingJobCount TTask::GetPendingJobCountDelta()
{
    auto oldValue = CachedPendingJobCount_;
    auto newValue = GetPendingJobCount();
    CachedPendingJobCount_ = newValue;
    return newValue - oldValue;
}

bool TTask::HasNoPendingJobs() const
{
    return GetPendingJobCount().IsZero();
}

bool TTask::HasNoPendingJobs(const TString& poolTree) const
{
    return GetPendingJobCount().GetJobCountFor(poolTree) == 0;
}

int TTask::GetTotalJobCount() const
{
    if (!IsActive()) {
        return 0;
    }

    int totalJobCount = GetChunkPoolOutput()->GetJobCounter()->GetTotal();
    for (const auto* jobManager : JobManagers_) {
        totalJobCount += jobManager->GetTotalJobCount();
    }
    return totalJobCount;
}

int TTask::GetTotalJobCountDelta()
{
    int oldValue = CachedTotalJobCount_;
    int newValue = GetTotalJobCount();
    CachedTotalJobCount_ = newValue;
    return newValue - oldValue;
}

std::vector<std::optional<i64>> TTask::GetMaximumUsedTmpfsSizes() const
{
    return MaximumUsedTmpfsSizes_;
}

const TProgressCounterPtr& TTask::GetJobCounter() const
{
    return GetChunkPoolOutput()->GetJobCounter();
}

TCompositeNeededResources TTask::GetTotalNeededResourcesDelta()
{
    if (!TaskHost_->GetSpec()->UseClusterThrottlers) {
        return GetTotalNeededResourcesDefaultDelta();
    }

    if (UnavailableNetworkBandwidthToClustersStartTime_) {
        UnavailableNetworkBandwidthToClustersDuration_ += TInstant::Now() - UnavailableNetworkBandwidthToClustersStartTime_.value();
    }

    auto isNetworkBandwidthAvailable = true;
    for (const auto& [clusterName, isAvailable] : GetClusterToNetworkBandwidthAvailability()) {
        if (!isAvailable) {
            isNetworkBandwidthAvailable = false;

            YT_LOG_DEBUG("Network bandwidth to remote cluster is not available now so zero out needed resources "
                "(Cluster: %v, OldMaxRunnableJobCount: %v)",
                clusterName,
                CurrentMaxRunnableJobCount_);
            break;
        }
    }

    if (!isNetworkBandwidthAvailable) {
        UnavailableNetworkBandwidthToClustersStartTime_ = TInstant::Now();
        // Zero out maximum runnable jobs. It will be coming back once bandwidth becomes available.
        auto result = -CachedTotalNeededResources_;
        CachedTotalNeededResources_ = {};
        CurrentMaxRunnableJobCount_ = 0;
        return result;
    }

    UnavailableNetworkBandwidthToClustersStartTime_.reset();

    auto oldCurrentMaxRunnableJobCount = CurrentMaxRunnableJobCount_;
    // Increase maximum runnable jobs exponentially up to MaxRunnableJobCount.
    CurrentMaxRunnableJobCount_ = std::clamp(2 * CurrentMaxRunnableJobCount_, static_cast<i64>(1), MaxRunnableJobCount);

    YT_LOG_DEBUG("Network bandwidth to all remote clusters is available, increase needed resources if necessary "
        "(OldMaxRunnableJobCount: %v, NewMaxRunnableJobCount: %v)",
        oldCurrentMaxRunnableJobCount,
        CurrentMaxRunnableJobCount_);

    return TTask::GetTotalNeededResourcesDefaultDelta();
}

TCompositeNeededResources TTask::GetTotalNeededResourcesDefaultDelta()
{
    auto oldValue = CachedTotalNeededResources_;
    auto newValue = GetTotalNeededResources();
    CachedTotalNeededResources_ = newValue;
    return newValue - oldValue;
}

TCompositeNeededResources TTask::GetTotalNeededResources(i64 maxRunnableJobCount) const
{
    maxRunnableJobCount = std::min(CurrentMaxRunnableJobCount_, maxRunnableJobCount);

    auto jobCount = GetPendingJobCount();
    // NB: Don't call GetMinNeededResources if there are no pending jobs.
    TCompositeNeededResources result;

    if (jobCount.DefaultCount != 0) {
        result.DefaultResources = GetMinNeededResources().ToJobResources() * std::min(static_cast<i64>(jobCount.DefaultCount), maxRunnableJobCount);
    }
    for (const auto& [tree, count] : jobCount.CountByPoolTree) {
        result.ResourcesByPoolTree[tree] = count == 0
            ? TJobResources{}
            : GetMinNeededResources().ToJobResources() * std::min(static_cast<i64>(count), maxRunnableJobCount);
    }
    return result;
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

TDuration TTask::GetLocalityTimeout() const
{
    return TDuration::Zero();
}

bool TTask::HasInputLocality() const
{
    return true;
}

void TTask::AddInput(TChunkStripePtr stripe)
{
    for (const auto& dataSlice : stripe->DataSlices) {
        // For all pools except sorted pool this simply drops key bounds (keeping them
        // in InputChunkToReadBounds_) as pools have no use for them.
        // For sorted chunk pool behavior is trickier as task adjusts the input read limits
        // to match comparator of sorted chunk pool.
        YT_VERIFY(!dataSlice->IsLegacy);
        AdjustInputKeyBounds(dataSlice);
        // Data slice may be either legacy or not depending on whether task uses
        // legacy sorted chunk pool or not.
    }

    if (TaskHost_->GetSpec()->UseClusterThrottlers) {
        // Add remote input clusters and subscribe to network availability updates.
        auto clusterName = LocalClusterName;
        const auto& inputTables = TaskHost_->GetInputManager()->GetInputTables();
        if (stripe->GetTableIndex() >= 0 && stripe->GetTableIndex() < std::ssize(inputTables)) {
            clusterName = inputTables[stripe->GetTableIndex()]->ClusterName;
        }

        if (!IsLocal(clusterName)) {
            auto guard = WriterGuard(ClusterToNetworkBandwidthAvailabilityLock_);
            if (!ClusterToNetworkBandwidthAvailability_.contains(clusterName)) {
                UpdateClusterToNetworkBandwidthAvailabilityLocked(
                    clusterName,
                    TaskHost_->IsNetworkBandwidthAvailable(clusterName));

                SubscribeToClusterNetworkBandwidthAvailabilityUpdated(clusterName);
            }
        }
    }

    TaskHost_->GetInputManager()->RegisterInputStripe(stripe, this);

    UpdateTask();
}

void TTask::AdjustInputKeyBounds(const TLegacyDataSlicePtr& dataSlice)
{
    YT_VERIFY(!dataSlice->IsLegacy);

    if ((dataSlice->LowerLimit().KeyBound && !dataSlice->LowerLimit().KeyBound.IsUniversal()) ||
        (dataSlice->UpperLimit().KeyBound && !dataSlice->UpperLimit().KeyBound.IsUniversal()))
    {
        YT_VERIFY(IsInput_);

        // Store original read range into read range registry.
        InputReadRangeRegistry_.RegisterDataSlice(dataSlice);
    } else {
        dataSlice->ReadRangeIndex = std::nullopt;
    }

    AdjustDataSliceForPool(dataSlice);
}

void TTask::AdjustDataSliceForPool(const TLegacyDataSlicePtr& dataSlice) const
{
    YT_VERIFY(!dataSlice->IsLegacy);

    dataSlice->LowerLimit().KeyBound = TKeyBound();
    dataSlice->UpperLimit().KeyBound = TKeyBound();

    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
        chunkSlice->LowerLimit().KeyBound = TKeyBound();
        chunkSlice->UpperLimit().KeyBound = TKeyBound();
    }
}

void TTask::AdjustOutputKeyBounds(const TLegacyDataSlicePtr& dataSlice) const
{
    YT_VERIFY(!dataSlice->IsLegacy);
    if (dataSlice->ReadRangeIndex) {
        YT_VERIFY(IsInput_);
        const auto& inputTable = TaskHost_->GetInputTable(dataSlice->GetTableIndex());
        const auto& comparator = inputTable->Comparator;
        YT_VERIFY(comparator);

        // Intersect new read range with the original data slice read range.
        InputReadRangeRegistry_.ApplyReadRange(dataSlice, comparator);
    }
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
    YT_LOG_DEBUG("Task input finished");

    // GetChunkPoolInput() may return nullptr on tasks that do not require input, such as for vanilla operation.
    if (const auto& chunkPoolInput = GetChunkPoolInput()) {
        chunkPoolInput->Finish();
    }
}

void TTask::UpdateTask()
{
    TaskHost_->UpdateTask(this);
}

void TTask::RegisterInGraph()
{
    DoRegisterInGraph();
    UpdateTask();
    CheckCompleted();
}

void TTask::DoRegisterInGraph()
{
    auto progressCounter = GetChunkPoolOutput()->GetJobCounter();
    TaskHost_->GetDataFlowGraph()
        ->RegisterCounter(GetVertexDescriptor(), progressCounter, GetJobType());

    for (const auto* jobManager : JobManagers_) {
        TaskHost_->GetDataFlowGraph()->RegisterCounter(
            GetVertexDescriptor(),
            jobManager->GetProgressCounter(),
            GetJobType());
    }
}

void TTask::RegisterInGraph(TDataFlowGraph::TVertexDescriptor inputVertex)
{
    SetInputVertex(inputVertex);

    RegisterInGraph();
}

void TTask::RegisterCounters(const TProgressCounterPtr& parent)
{
    GetChunkPoolOutput()->GetJobCounter()->AddParent(parent);
    for (const auto* jobManager : JobManagers_) {
        jobManager->GetProgressCounter()->AddParent(parent);
    }
}

void TTask::SwitchIntermediateMedium()
{
    for (const auto& streamDescriptor : OutputStreamDescriptors_) {
        if (!streamDescriptor->SlowMedium.empty()) {
            streamDescriptor->TableWriterOptions->MediumName = streamDescriptor->SlowMedium;
            streamDescriptor->TableWriterOptions->ReplicationFactor = TaskHost_->GetSpec()->IntermediateDataReplicationFactor;
            streamDescriptor->TableWriterConfig = MakeIntermediateTableWriterConfig(
                TaskHost_->GetSpec(),
                /*fastIntermediateMediumEnabled*/ false);
        }
    }
}

void TTask::PatchUserJobSpec(NControllerAgent::NProto::TUserJobSpec* jobSpec, TJobletPtr joblet) const
{
    ExperimentJobManager_.PatchUserJobSpec(jobSpec, joblet);
}

void TTask::CheckCompleted()
{
    if (!CompletedFired_ && IsCompleted()) {
        CompletedFired_ = true;
        OnTaskCompleted();
    }
}

void TTask::ForceComplete()
{
    if (!CompletedFired_) {
        YT_LOG_DEBUG("Task is forcefully completed");
        CompletedFired_ = true;
        OnTaskCompleted();
    }
}

TUserJobSpecPtr TTask::GetUserJobSpec() const
{
    return nullptr;
}

bool TTask::HasUserJob() const
{
    return static_cast<bool>(GetUserJobSpec());
}

ITaskHost* TTask::GetTaskHost()
{
    return TaskHost_;
}

bool TTask::ValidateChunkCount(int /*chunkCount*/)
{
    return true;
}

NScheduler::TAllocationStartDescriptor TTask::CreateAllocationStartDescriptor(
    const TAllocation& allocation,
    bool allowIdleCpuPolicy,
    // COMPAT(pogorelov)
    const NScheduler::NProto::TScheduleAllocationSpec& allocationSpec) const
{
    TAllocationStartDescriptor startDescriptor{
        .Id = allocation.Id,
        .ResourceLimits = allocation.Resources,
        .AllocationAttributes = TAllocationAttributes{
            .WaitingForResourcesOnNodeTimeout = InferWaitingForResourcesTimeout(
                allocationSpec),
            .AllowIdleCpuPolicy = allowIdleCpuPolicy,
        },
    };

    if (auto userJobSpec = GetUserJobSpec()) {
        auto& attributes = startDescriptor.AllocationAttributes;

        attributes.CudaToolkitVersion = userJobSpec->CudaToolkitVersion;
        if (auto& diskRequest = userJobSpec->DiskRequest) {
            attributes.DiskRequest.MediumIndex = diskRequest->MediumIndex;
            attributes.DiskRequest.DiskSpace = diskRequest->DiskSpace;
            attributes.DiskRequest.InodeCount = diskRequest->InodeCount;
        }
        attributes.PortCount = userJobSpec->PortCount;
    }

    return startDescriptor;
}

void TTask::CheckAndProcessOperationCompletedInScheduleJob()
{
    if (TaskHost_->IsCompleted()) {
        TaskHost_->OnOperationCompleted(/*interrupted*/ false);
        YT_LOG_DEBUG("Completed operation while trying to schedule a job");
    }
}

std::expected<TTask::TOutputCookieInfo, EScheduleFailReason>
TTask::GetOutputCookieInfoForFirstJob(const TAllocation& allocation)
{
    auto chunkPoolOutput = GetChunkPoolOutput();
    bool speculative = chunkPoolOutput->GetJobCounter()->GetPending() == 0;

    TOutputCookieInfo result;

    if (TaskHost_->IsTreeProbing(allocation.TreeId)) {
        result.CompetitionType = EJobCompetitionType::Probing;
        result.OutputCookie = ProbingJobManager_.PeekJobCandidate();
    } else if (ExperimentJobManager_.IsTreatmentReady()) {
        result.CompetitionType = EJobCompetitionType::Experiment;
        result.OutputCookie = ExperimentJobManager_.PeekJobCandidate();
    } else if (speculative) {
        result.CompetitionType = EJobCompetitionType::Speculative;
        result.OutputCookie = SpeculativeJobManager_.PeekJobCandidate();
    } else {
        result.CompetitionType = std::nullopt;
        result.OutputCookie = ExtractCookieForAllocation(allocation);
        if (result.OutputCookie == IChunkPoolOutput::NullCookie) {
            YT_LOG_DEBUG("Job input is empty");

            CheckAndProcessOperationCompletedInScheduleJob();

            return std::unexpected(EScheduleFailReason::EmptyInput);
        }
    }

    return result;
}

std::expected<TTask::TOutputCookieInfo, EScheduleFailReason>
TTask::GetOutputCookieInfoForNextJob(const TAllocation& allocation)
{
    const auto& chunkPoolOutput = GetChunkPoolOutput();
    bool speculative = chunkPoolOutput->GetJobCounter()->GetPending() == 0;

    TOutputCookieInfo result;

    if (auto competitionType = allocation.LastJobInfo.CompetitionType.value_or(EJobCompetitionType::Speculative);
        competitionType == EJobCompetitionType::Probing)
    {
        result.CompetitionType = EJobCompetitionType::Probing;
        result.OutputCookie = ProbingJobManager_.PeekJobCandidate();
    } else if (competitionType == EJobCompetitionType::Experiment) {
        if (!ExperimentJobManager_.IsTreatmentReady()) {
            return std::unexpected(EScheduleFailReason::NoPendingJobs);
        }

        result.CompetitionType = EJobCompetitionType::Experiment;
        result.OutputCookie = ExperimentJobManager_.PeekJobCandidate();
    } else {
        YT_VERIFY(competitionType == EJobCompetitionType::Speculative);

        if (speculative) {
            result.CompetitionType = EJobCompetitionType::Speculative;
            result.OutputCookie = SpeculativeJobManager_.PeekJobCandidate();
        } else {
            result.CompetitionType = std::nullopt;
            result.OutputCookie = ExtractCookieForAllocation(allocation);
            if (result.OutputCookie == IChunkPoolOutput::NullCookie) {
                YT_LOG_DEBUG("Job input is empty");

                if (!allocation.LastJobInfo.CompetitionType) {
                    CheckAndProcessOperationCompletedInScheduleJob();
                }

                return std::unexpected(EScheduleFailReason::EmptyInput);
            }
        }
    }

    return result;
}

std::optional<EScheduleFailReason> TTask::TryScheduleJob(
    TAllocation& allocation,
    const TSchedulingContext& context,
    std::optional<TJobId> previousJobId,
    bool treeIsTentative)
{
    auto Logger = this->Logger.WithTag("AllocationId: %v", context.GetAllocationId());
    if (auto failReason = GetScheduleFailReason(context)) {
        return *failReason;
    }

    auto jobIdOrError = TaskHost_->GenerateJobId(allocation.Id, previousJobId.value_or(TJobId()));
    if (!jobIdOrError) {
        return jobIdOrError.error();
    }

    auto cookieInfoOrError = previousJobId
        ? GetOutputCookieInfoForNextJob(allocation)
        : GetOutputCookieInfoForFirstJob(allocation);
    if (!cookieInfoOrError) {
        return cookieInfoOrError.error();
    }

    auto cookieInfo = std::move(cookieInfoOrError.value());
    auto jobId = jobIdOrError.value();

    auto result = TryScheduleJob(
        allocation,
        context,
        jobId,
        treeIsTentative,
        cookieInfo.OutputCookie,
        cookieInfo.CompetitionType);

    if (result) {
        if (!previousJobId) {
            const auto& joblet = allocation.Joblet;

            allocation.LastJobInfo.CompetitionType = joblet->CompetitionType;
            allocation.PoolPath = joblet->PoolPath;
            allocation.Task = this;
            allocation.NodeDescriptor = joblet->NodeDescriptor;
            allocation.Resources = result.value();
        }

        allocation.LastJobInfo.OutputCookie = cookieInfo.OutputCookie;

        return std::nullopt;
    } else {
        return result.error();
    }
}

std::expected<NScheduler::TJobResourcesWithQuota, EScheduleFailReason> TTask::TryScheduleJob(
    TAllocation& allocation,
    const TSchedulingContext& context,
    TJobId jobId,
    bool treeIsTentative,
    NChunkPools::IChunkPoolOutput::TCookie outputCookie,
    std::optional<EJobCompetitionType> competitionType)
{
    auto abortJob = [&] (EAbortReason abortReason) {
        if (!competitionType) {
            auto chunkPoolOutput = GetChunkPoolOutput();
            chunkPoolOutput->Aborted(outputCookie, abortReason);
        }
    };

    if (treeIsTentative && !TentativeTreeEligibility_.CanScheduleJob(allocation.TreeId, /*tentative*/ true)) {
        abortJob(EAbortReason::SchedulingOther);
        return std::unexpected(EScheduleFailReason::TentativeTreeDeclined);
    }

    auto chunkPoolOutput = GetChunkPoolOutput();
    bool speculative = chunkPoolOutput->GetJobCounter()->GetPending() == 0;
    if (speculative && treeIsTentative) {
        abortJob(EAbortReason::SchedulingOther);
        return std::unexpected(EScheduleFailReason::TentativeSpeculativeForbidden);
    }

    int taskJobIndex = TaskJobIndexGenerator_.Next();
    auto joblet = TaskHost_->CreateJoblet(
        this,
        jobId,
        allocation.TreeId,
        taskJobIndex,
        context.GetPoolPath(),
        treeIsTentative);

    joblet->OutputCookie = outputCookie;
    joblet->CompetitionType = competitionType;

    int sliceCount = chunkPoolOutput->GetStripeListSliceCount(joblet->OutputCookie);
    if (!ValidateChunkCount(sliceCount)) {
        abortJob(EAbortReason::IntermediateChunkLimitExceeded);
        return std::unexpected(EScheduleFailReason::IntermediateChunkLimitExceeded);
    }

    const auto& jobSpecSliceThrottler = TaskHost_->GetJobSpecSliceThrottler();
    if (sliceCount > TaskHost_->GetConfig()->HeavyJobSpecSliceCountThreshold) {
        if (!jobSpecSliceThrottler->TryAcquire(sliceCount)) {
            YT_LOG_DEBUG(
                "Job spec throttling is active (SliceCount: %v)",
                sliceCount);
            abortJob(EAbortReason::SchedulingJobSpecThrottling);
            return std::unexpected(EScheduleFailReason::JobSpecThrottling);
        }
    } else {
        jobSpecSliceThrottler->Acquire(sliceCount);
    }

    joblet->InputStripeList = chunkPoolOutput->GetStripeList(joblet->OutputCookie);

    // TODO(pogorelov): Think about changes in chunk pools for better working with allocations with several jobs.
    auto findIt = ResourceOverdraftedOutputCookieToState_.find(joblet->OutputCookie);
    if (findIt != ResourceOverdraftedOutputCookieToState_.end()) {
        const auto& state = findIt->second;
        joblet->PredecessorJobId = state.LastJobId;
        joblet->PredecessorType = EPredecessorType::ResourceOverdraft;
        if (state.JobProxyStatus != EResourceOverdraftStatus::None) {
            joblet->JobProxyMemoryReserveFactor = state.DedicatedJobProxyMemoryReserveFactor;
        } else {
            joblet->JobProxyMemoryReserveFactor = GetJobProxyMemoryReserveFactor();
        }
        if (HasUserJob()) {
            if (state.UserJobStatus != EResourceOverdraftStatus::None) {
                joblet->UserJobMemoryReserveFactor = state.DedicatedUserJobMemoryReserveFactor;
            } else {
                joblet->UserJobMemoryReserveFactor = *GetUserJobMemoryReserveFactor();
            }
        }
    } else {
        joblet->JobProxyMemoryReserveFactor = GetJobProxyMemoryReserveFactor();
        if (HasUserJob()) {
            joblet->UserJobMemoryReserveFactor = *GetUserJobMemoryReserveFactor();
        }
    }

    auto estimatedResourceUsage = GetNeededResources(joblet);
    joblet->EstimatedResourceUsage = estimatedResourceUsage;

    TaskHost_->UpdateWriteBufferMemoryAlert(
        joblet->JobId,
        estimatedResourceUsage.GetJobProxyMemory(),
        estimatedResourceUsage.GetJobProxyMemoryWithFixedWriteBufferSize());

    TJobResourcesWithQuota neededResources = ApplyMemoryReserve(
        estimatedResourceUsage,
        *joblet->JobProxyMemoryReserveFactor,
        joblet->UserJobMemoryReserveFactor);
    joblet->ResourceLimits = neededResources.ToJobResources();

    auto userJobSpec = GetUserJobSpec();
    if (userJobSpec && userJobSpec->DiskRequest) {
        neededResources.DiskQuota() = CreateDiskQuota(userJobSpec->DiskRequest, TaskHost_->GetMediumDirectory());
        joblet->DiskRequestAccount = userJobSpec->DiskRequest->Account;
        joblet->DiskQuota = neededResources.DiskQuota();
    }

    if (userJobSpec) {
        i64 totalTmpfsSize = 0;
        for (const auto& volume : userJobSpec->TmpfsVolumes) {
            totalTmpfsSize += volume->Size;
        }
        YT_VERIFY(joblet->UserJobMemoryReserveFactor.has_value());

        // Memory reserve should greater than or equal to tmpfs_size (see YT-5518 for more details).
        // This is ensured by adjusting memory reserve factor in user job config as initialization,
        // but just in case we also limit the actual memory_reserve value here.
        joblet->UserJobMemoryReserve = std::max(
            static_cast<i64>(*joblet->UserJobMemoryReserveFactor * estimatedResourceUsage.GetUserJobMemory()),
            totalTmpfsSize);
    }

    // Check the usage against the limits. This is the last chance to give up.
    if (!context.CanSatisfyDemand(neededResources))
    {
        YT_LOG_DEBUG(
            "Actual resource demand is not met (AvailableResources: %v, NeededResources: %v)",
            context.GetResourcesString(TaskHost_->GetMediumDirectory()),
            FormatResources(neededResources));
        CheckResourceDemandSanity(neededResources);
        abortJob(EAbortReason::SchedulingOther);
        // Seems like cached min needed resources are too optimistic.
        ResetCachedMinNeededResources();
        return std::unexpected(EScheduleFailReason::NotEnoughResources);
    }

    for (auto* jobManager : JobManagers_) {
        jobManager->OnJobScheduled(joblet);
    }

    // Job is restarted if LostJobCookieMap contains at least one entry with this output cookie.
    auto it = LostJobCookieMap_.lower_bound(TCookieAndPool(joblet->OutputCookie, nullptr));
    bool restarted = it != LostJobCookieMap_.end() && it->first.first == joblet->OutputCookie;

    auto lostIntermediateChunk = LostIntermediateChunkCookieMap_.lower_bound(TCookieAndPool(joblet->OutputCookie, nullptr));
    bool lostIntermediateChunkIsKnown = lostIntermediateChunk != LostIntermediateChunkCookieMap_.end() && it->first.first == joblet->OutputCookie;

    auto accountBuildingJobSpec = BIND(&ITaskHost::AccountBuildingJobSpecDelta, MakeWeak(TaskHost_));
    accountBuildingJobSpec.Run(+1, +sliceCount);

    // Finally guard allows us not to think about exceptions, cancellation, controller destruction, and other tricky cases.
    auto discountBuildingJobSpecGuard = Finally([=, accountBuildingJobSpec = std::move(accountBuildingJobSpec)] {
        accountBuildingJobSpec.Run(-1, -sliceCount);
    });

    joblet->DebugArtifactsAccount = TaskHost_->GetSpec()->DebugArtifactsAccount;

    AddJobTypeToJoblet(joblet);

    joblet->JobInterruptible = IsJobInterruptible();
    joblet->Restarted = restarted;
    joblet->NodeDescriptor = context.GetNodeDescriptor();

    if (userJobSpec && userJobSpec->Monitoring->Enable) {
        joblet->UserJobMonitoringDescriptor = TaskHost_->RegisterJobForMonitoring(joblet->JobId);
    }

    if (userJobSpec) {
        joblet->ArchiveTtl = userJobSpec->ArchiveTtl;
    }

    joblet->EnabledJobProfiler = SelectProfiler();

    if (userJobSpec && userJobSpec->JobSpeculationTimeout) {
        joblet->JobSpeculationTimeout = userJobSpec->JobSpeculationTimeout;
    } else if (TaskHost_->GetSpec()->JobSpeculationTimeout) {
        joblet->JobSpeculationTimeout = TaskHost_->GetSpec()->JobSpeculationTimeout;
    }

    THashSet<TStringBuf> media;
    for (const auto& streamDescriptor : OutputStreamDescriptors_) {
        media.insert(streamDescriptor->TableWriterOptions->MediumName);
    }

    YT_LOG_DEBUG(
        "Job scheduled (JobId: %v, JobType: %v, Address: %v, JobIndex: %v, OutputCookie: %v, SliceCount: %v (%v local), "
        "Approximate: %v, DataWeight: %v (%v local), RowCount: %v, PartitionTag: %v, Restarted: %v, EstimatedResourceUsage: %v, JobProxyMemoryReserveFactor: %v, "
        "UserJobMemoryReserveFactor: %v, ResourceLimits: %v, CompetitionType: %v, JobSpeculationTimeout: %v, Media: %v, RestartedForLostChunk: %v, "
        "Interruptible: %v)",
        joblet->JobId,
        joblet->JobType,
        context.GetNodeDescriptor().Address,
        joblet->JobIndex,
        joblet->OutputCookie,
        joblet->InputStripeList->TotalChunkCount,
        joblet->InputStripeList->LocalChunkCount,
        joblet->InputStripeList->IsApproximate,
        joblet->InputStripeList->TotalDataWeight,
        joblet->InputStripeList->LocalDataWeight,
        joblet->InputStripeList->TotalRowCount,
        joblet->InputStripeList->PartitionTag,
        restarted,
        FormatResources(estimatedResourceUsage),
        joblet->JobProxyMemoryReserveFactor,
        joblet->UserJobMemoryReserveFactor,
        FormatResources(neededResources),
        joblet->CompetitionType,
        joblet->JobSpeculationTimeout,
        media,
        lostIntermediateChunkIsKnown ? lostIntermediateChunk->second : NullChunkId,
        joblet->JobInterruptible);

    SetStreamDescriptors(joblet);

    for (const auto& streamDescriptor : joblet->OutputStreamDescriptors) {
        int cellTagIndex = RandomNumber<size_t>() % streamDescriptor->CellTags.size();
        auto cellTag = streamDescriptor->CellTags[cellTagIndex];
        joblet->ChunkListIds.push_back(TaskHost_->ExtractOutputChunkList(cellTag));
    }

    if (TaskHost_->StderrTable() && IsStderrTableEnabled()) {
        joblet->StderrTableChunkListId = TaskHost_->ExtractDebugChunkList(TaskHost_->StderrTable()->ExternalCellTag);
    }

    if (TaskHost_->CoreTable() && IsCoreTableEnabled()) {
        joblet->CoreTableChunkListId = TaskHost_->ExtractDebugChunkList(TaskHost_->CoreTable()->ExternalCellTag);
    }

    // Sync part.
    TaskHost_->CustomizeJoblet(joblet);

    TaskHost_->RegisterJoblet(joblet);
    if (!joblet->CompetitionType) {
        TaskHost_->AddValueToEstimatedHistogram(joblet);
        if (EstimatedInputDataWeightHistogram_) {
            EstimatedInputDataWeightHistogram_->AddValue(joblet->InputStripeList->TotalDataWeight);
        }
    }

    OnJobStarted(joblet);

    JobSplitter_->OnJobStarted(joblet->JobId, joblet->InputStripeList, joblet->OutputCookie, joblet->JobInterruptible);

    joblet->JobSpecProtoFuture = BIND([
        weakTaskHost = MakeWeak(TaskHost_),
        joblet,
        scheduleAllocationSpec = context.GetScheduleAllocationSpec()
            ? std::make_optional(*context.GetScheduleAllocationSpec())
            : std::nullopt,
        discountBuildingJobSpecGuard = std::move(discountBuildingJobSpecGuard),
        Logger = Logger
    ] {
        if (auto taskHost = weakTaskHost.Lock()) {
            YT_LOG_DEBUG(
                "Started building job spec (JobId: %v)",
                joblet->JobId);
            TWallTimer timer;
            auto jobSpecProto = taskHost->BuildJobSpecProto(joblet, scheduleAllocationSpec);
            YT_LOG_DEBUG(
                "Job spec built (JobId: %v, TimeElapsed: %v)",
                joblet->JobId,
                timer.GetElapsedTime());
            return jobSpecProto;
        } else {
            THROW_ERROR_EXCEPTION("Operation controller was destroyed");
        }
    })
        .AsyncVia(TaskHost_->GetJobSpecBuildInvoker())
        .Run();

    if (!StartTime_) {
        StartTime_ = TInstant::Now();
        // Start timers explicitly, because when the first job starts, StartTime_ is not set.
        OnPendingJobCountUpdated();
    }

    return neededResources;
}

bool TTask::TryRegisterSpeculativeJob(const TJobletPtr& joblet)
{
    return SpeculativeJobManager_.TryAddCompetitiveJob(joblet);
}

void TTask::BuildTaskYson(TFluentMap fluent) const
{
    static const std::vector<TString> jobManagerNames = {"speculative", "probing", "experiment"};
    YT_VERIFY(jobManagerNames.size() == JobManagers_.size());

    fluent
        .Item("task_name").Value(GetVertexDescriptor())
        .Item("job_type").Value(GetJobType())
        .Item("has_user_job").Value(HasUserJob())
        .Item("job_counter").Value(GetJobCounter())
        .DoFor(Zip(JobManagers_, jobManagerNames), [] (auto fluent, auto jobManager) {
            fluent.Item(std::get<1>(jobManager) + "_job_counter").Value(std::get<0>(jobManager)->GetProgressCounter());
        })
        .Item("input_finished").Value(GetChunkPoolInput() && GetChunkPoolInput()->IsFinished())
        .Item("completed").Value(IsCompleted())
        .Item("min_needed_resources").Value(GetMinNeededResources())
        .Item("job_proxy_memory_reserve_factor").Value(GetJobProxyMemoryReserveFactor())
        .Item("received_output_digest_count").Value(ReceivedDigestCount_)
        .DoIf(HasUserJob(), [&] (TFluentMap fluent) {
            fluent.Item("user_job_memory_reserve_factor").Value(*GetUserJobMemoryReserveFactor());
        })
        .DoIf(static_cast<bool>(StartTime_), [&] (TFluentMap fluent) {
            fluent.Item("start_time").Value(*StartTime_);
        })
        .DoIf(static_cast<bool>(CompletionTime_), [&] (TFluentMap fluent) {
            fluent.Item("completion_time").Value(*CompletionTime_);
        })
        .Item("ready_time").Value(GetReadyTime())
        .Item("exhaust_time").Value(GetExhaustTime())
        .DoIf(static_cast<bool>(EstimatedInputDataWeightHistogram_), [&] (TFluentMap fluent) {
            EstimatedInputDataWeightHistogram_->BuildHistogramView();
            fluent.Item("estimated_input_data_weight_histogram").Value(*EstimatedInputDataWeightHistogram_);
        })
        .DoIf(static_cast<bool>(InputDataWeightHistogram_), [&] (TFluentMap fluent) {
            InputDataWeightHistogram_->BuildHistogramView();
            fluent.Item("input_data_weight_histogram").Value(*InputDataWeightHistogram_);
        })
        .DoIf(static_cast<bool>(JobSplitter_), [&] (TFluentMap fluent) {
            fluent.Item("job_splitter")
                .BeginMap()
                    .Do(BIND(&IJobSplitter::BuildJobSplitterInfo, JobSplitter_.get()))
                .EndMap();
        });
}

void TTask::PropagatePartitions(
    const std::vector<TOutputStreamDescriptorPtr>& streamDescriptors,
    const TChunkStripeListPtr& /*inputStripeList*/,
    std::vector<TChunkStripePtr>* outputStripes)
{
    YT_VERIFY(outputStripes->size() == streamDescriptors.size());
    for (int stripeIndex = 0; stripeIndex < std::ssize(*outputStripes); ++stripeIndex) {
        (*outputStripes)[stripeIndex]->PartitionTag = streamDescriptors[stripeIndex]->PartitionTag;
    }
}

NChunkPools::IChunkPoolOutput::TCookie  TTask::ExtractCookieForAllocation(const TAllocation& allocation)
{
    auto nodeId = HasInputLocality() ? NodeIdFromAllocationId(allocation.Id) : InvalidNodeId;

    return GetChunkPoolOutput()->Extract(nodeId);
}

std::optional<EAbortReason> TTask::ShouldAbortCompletingJob(const TJobletPtr& joblet)
{
    for (auto* jobManager : JobManagers_) {
        if (auto result = jobManager->ShouldAbortCompletingJob(joblet)) {
            return result;
        }
    }
    return std::nullopt;
}

bool TTask::IsCompleted() const
{
    for (const auto* jobManager : JobManagers_) {
        if (!jobManager->IsFinished()) {
            return false;
        }
    }
    return IsActive() && GetChunkPoolOutput()->IsCompleted();
}

bool TTask::IsActive() const
{
    return true;
}

i64 TTask::GetTotalDataWeight() const
{
    return GetChunkPoolOutput()->GetDataWeightCounter()->GetTotal();
}

i64 TTask::GetCompletedDataWeight() const
{
    return GetChunkPoolOutput()->GetDataWeightCounter()->GetCompletedTotal();
}

i64 TTask::GetPendingDataWeight() const
{
    i64 pendingDataWeight = GetChunkPoolOutput()->GetDataWeightCounter()->GetPending();
    for (const auto* jobManager : JobManagers_) {
        pendingDataWeight += jobManager->GetPendingCandidatesDataWeight();
    }
    return pendingDataWeight;
}

i64 TTask::GetInputDataSliceCount() const
{
    return GetChunkPoolOutput()->GetDataSliceCounter()->GetTotal();
}

void TTask::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, TaskHost_);

    PHOENIX_REGISTER_FIELD(2, CachedPendingJobCount_);
    PHOENIX_REGISTER_FIELD(3, CachedTotalJobCount_);

    PHOENIX_REGISTER_FIELD(4, CachedTotalNeededResources_);
    PHOENIX_REGISTER_FIELD(5, CachedMinNeededResources_);

    PHOENIX_REGISTER_FIELD(6, CompletedFired_);

    PHOENIX_REGISTER_FIELD(7, LostJobCookieMap_,
        .template Serializer<
            TMapSerializer<
                TTupleSerializer<TCookieAndPool, 2>,
                TDefaultSerializer,
                TUnsortedTag
            >
        >());

    PHOENIX_REGISTER_FIELD(8, OutputStreamDescriptors_);
    PHOENIX_REGISTER_FIELD(9, InputStreamDescriptors_);

    PHOENIX_REGISTER_FIELD(10, InputVertex_);

    PHOENIX_REGISTER_FIELD(11, TentativeTreeEligibility_);

    PHOENIX_REGISTER_FIELD(12, UserJobMemoryDigest_);
    PHOENIX_REGISTER_FIELD(13, JobProxyMemoryDigest_);

    PHOENIX_REGISTER_FIELD(14, JobSplitter_);

    PHOENIX_REGISTER_FIELD(15, InputChunkMapping_);

    PHOENIX_REGISTER_FIELD(16, TaskJobIndexGenerator_);

    PHOENIX_REGISTER_FIELD(17, SpeculativeJobManager_);
    PHOENIX_REGISTER_FIELD(18, ProbingJobManager_);
    PHOENIX_REGISTER_FIELD(19, ExperimentJobManager_);

    PHOENIX_REGISTER_FIELD(20, StartTime_);
    PHOENIX_REGISTER_FIELD(21, CompletionTime_);

    PHOENIX_REGISTER_FIELD(22, EstimatedInputDataWeightHistogram_);
    PHOENIX_REGISTER_FIELD(23, InputDataWeightHistogram_);

    PHOENIX_REGISTER_FIELD(24, InputReadRangeRegistry_);

    PHOENIX_REGISTER_FIELD(25, IsInput_);

    PHOENIX_REGISTER_FIELD(26, ResourceOverdraftedOutputCookieToState_);

    PHOENIX_REGISTER_FIELD(27, Logger);

    PHOENIX_REGISTER_FIELD(28, ReadyTimer_);
    PHOENIX_REGISTER_FIELD(29, ExhaustTimer_);

    PHOENIX_REGISTER_FIELD(30, AggregatedFinishedJobStatistics_);

    PHOENIX_REGISTER_FIELD(31, UserJobMemoryMultiplier_);
    PHOENIX_REGISTER_FIELD(32, JobProxyMemoryMultiplier_);

    PHOENIX_REGISTER_FIELD(33, JobOutputHash_,
        .template Serializer<
            TMapSerializer<
                TTupleSerializer<TCookieAndPool, 2>,
                TDefaultSerializer,
                TUnsortedTag
            >
        >()
        .SinceVersion(ESnapshotVersion::JobDeterminismValidation));

    PHOENIX_REGISTER_FIELD(34, UnavailableNetworkBandwidthToClustersStartTime_,
        .SinceVersion(ESnapshotVersion::ThrottlingOfRemoteReads));

    PHOENIX_REGISTER_FIELD(35, UnavailableNetworkBandwidthToClustersDuration_,
        .SinceVersion(ESnapshotVersion::ThrottlingOfRemoteReads));

    PHOENIX_REGISTER_FIELD(36, ClusterToNetworkBandwidthAvailability_,
        .SinceVersion(ESnapshotVersion::ThrottlingOfRemoteReads));

    PHOENIX_REGISTER_FIELD(37, CurrentMaxRunnableJobCount_,
        .SinceVersion(ESnapshotVersion::ThrottlingOfRemoteReads));

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        // COMPAT(galtsev)
        if (this_->TaskHost_->GetSpec()->JobExperiment) {
            this_->ExperimentJobManager_.SetJobExperiment(this_->TaskHost_->GetJobExperiment());
        }

        // Restore callback and subscriptions to remote clusters.
        if (this_->TaskHost_->GetSpec()->UseClusterThrottlers) {
            this_->ClusterToNetworkBandwidthAvailabilityUpdatedCallback_ = BIND_NO_PROPAGATE(
                &TTask::UpdateNetworkAndTask,
                MakeWeak(this_))
            .Via(this_->TaskHost_->GetCancelableInvoker());
        }

        // Task has been previously unsubscribed in FinalizeSubscriptions().
        for (const auto& [cluster, _] : this_->ClusterToNetworkBandwidthAvailability_) {
            this_->SubscribeToClusterNetworkBandwidthAvailabilityUpdated(cluster);
        }
    });
}

void TTask::OnJobStarted(TJobletPtr joblet)
{
    TentativeTreeEligibility_.OnJobStarted(joblet->TreeId, joblet->TreeIsTentative);
}

bool TTask::CanLoseJobs() const
{
    return false;
}

void TTask::DoUpdateOutputEdgesForJob(
    const TDataFlowGraph::TVertexDescriptor& vertex,
    const std::vector<NChunkClient::NProto::TDataStatistics>& dataStatistics)
{
    for (int index = 0; index < std::ssize(OutputStreamDescriptors_); ++index) {
        const auto& targetVertex = OutputStreamDescriptors_[index]->TargetDescriptor;
        // If target vertex is unknown it is derived class' responsibility to update statistics.
        if (!targetVertex.empty()) {
            TaskHost_->GetDataFlowGraph()->UpdateEdgeJobDataStatistics(
                vertex,
                targetVertex,
                VectorAtOr(dataStatistics, index));
        }
    }
}

void TTask::UpdateInputEdges(const NChunkClient::NProto::TDataStatistics& dataStatistics, const TJobletPtr& joblet)
{
    // TODO(gritukan): Create a virtual source task and get rid of this hack.
    if (InputVertex_ == TDataFlowGraph::SourceDescriptor) {
        TaskHost_->GetDataFlowGraph()->UpdateEdgeTeleportDataStatistics(InputVertex_, GetVertexDescriptorForJoblet(joblet), dataStatistics);
    }
}

void TTask::UpdateOutputEdgesForTeleport(const NChunkClient::NProto::TDataStatistics& dataStatistics)
{
    TaskHost_->GetDataFlowGraph()->UpdateEdgeTeleportDataStatistics(GetVertexDescriptor(), TDataFlowGraph::SinkDescriptor, dataStatistics);
}

void TTask::UpdateOutputEdgesForJob(
    const std::vector<NChunkClient::NProto::TDataStatistics>& dataStatistics,
    const TJobletPtr& joblet)
{
    DoUpdateOutputEdgesForJob(GetVertexDescriptorForJoblet(joblet), dataStatistics);
}

void TTask::OnChunkTeleported(TInputChunkPtr chunk, std::any /*tag*/)
{
    NChunkClient::NProto::TDataStatistics dataStatistics;

    dataStatistics.set_uncompressed_data_size(chunk->GetUncompressedDataSize());
    dataStatistics.set_compressed_data_size(chunk->GetCompressedDataSize());
    dataStatistics.set_row_count(chunk->GetRowCount());
    dataStatistics.set_chunk_count(1);
    dataStatistics.set_data_weight(chunk->GetDataWeight());

    UpdateInputEdges(dataStatistics, nullptr);
    UpdateOutputEdgesForTeleport(dataStatistics);
}

bool TTask::IsSimpleTask() const
{
    return true;
}

TJobFinishedResult TTask::OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary)
{
    TJobFinishedResult result;

    TentativeTreeEligibility_.OnJobFinished(jobSummary, joblet->TreeId, joblet->TreeIsTentative, &result.NewlyBannedTrees);

    for (auto* jobManager : JobManagers_) {
        jobManager->OnJobCompleted(joblet);
    }

    YT_VERIFY(jobSummary.Statistics);
    const auto& statistics = *jobSummary.Statistics;

    if (!jobSummary.Abandoned) {
        YT_VERIFY(jobSummary.OutputDataStatistics);
        const auto& outputDataStatistics = *jobSummary.OutputDataStatistics;
        for (int index = 0; index < std::ssize(joblet->ChunkListIds); ++index) {
            auto outputStatistics = VectorAtOr(outputDataStatistics, index);
            if (outputStatistics.chunk_count() == 0) {
                if (!joblet->Revived) {
                    TaskHost_->GetOutputChunkListPool()->Reinstall(joblet->ChunkListIds[index]);
                }
                joblet->ChunkListIds[index] = NullChunkListId;
            }
            if (joblet->ChunkListIds[index] && OutputStreamDescriptors_[index]->ImmediatelyUnstageChunkLists) {
                this->TaskHost_->ReleaseChunkTrees({joblet->ChunkListIds[index]}, /*unstageRecursively*/ false);
                joblet->ChunkListIds[index] = NullChunkListId;
            }
        }

        YT_VERIFY(jobSummary.TotalInputDataStatistics);
        YT_VERIFY(jobSummary.TotalOutputDataStatistics);
        const auto& totalInputStatistics = *jobSummary.TotalInputDataStatistics;
        const auto& totalOutputStatistics = *jobSummary.TotalOutputDataStatistics;
        // It's impossible to check row count preservation on interrupted job.
        if (TaskHost_->IsRowCountPreserved() && jobSummary.InterruptionReason == EInterruptionReason::None) {
            YT_LOG_ERROR_IF(totalInputStatistics.row_count() != totalOutputStatistics.row_count(),
                "Input/output row count mismatch in completed job (Input: %v, Output: %v, Task: %v)",
                totalInputStatistics.row_count(),
                totalOutputStatistics.row_count(),
                GetTitle());
            YT_VERIFY(totalInputStatistics.row_count() == totalOutputStatistics.row_count());
        }

        YT_VERIFY(InputVertex_ != "");

        UpdateInputEdges(totalInputStatistics, joblet);
        UpdateOutputEdgesForJob(outputDataStatistics, joblet);

        TaskHost_->RegisterStderr(joblet, jobSummary);
        TaskHost_->RegisterCores(joblet, jobSummary);
    } else {
        auto& chunkListIds = joblet->ChunkListIds;
        // NB: We should release these chunk lists only when information about this job being abandoned
        // gets to the snapshot; otherwise it may revive in different scheduler and continue writing
        // to the released chunk list.
        TaskHost_->ReleaseChunkTrees(chunkListIds, true /*recursive*/, true /*waitForSnapshot*/);
        std::fill(chunkListIds.begin(), chunkListIds.end(), NullChunkListId);
    }

    if (jobSummary.InterruptionReason != EInterruptionReason::None) {
        auto isSplittable = GetChunkPoolOutput()->IsSplittable(joblet->OutputCookie);
        jobSummary.SplitJobCount = isSplittable ? EstimateSplitJobCount(jobSummary, joblet) : 1;
        YT_LOG_DEBUG(
            "Deciding job splitting (JobId: %v, OutputCookie: %v, InterruptionReason: %v, UnreadDataSliceCount: %v, IsSplittable: %v, SplitJobCount: %v)",
            jobSummary.Id,
            joblet->OutputCookie,
            jobSummary.InterruptionReason,
            jobSummary.UnreadInputDataSlices.size(),
            isSplittable,
            jobSummary.SplitJobCount);
    }
    JobSplitter_->OnJobCompleted(jobSummary);

    for (const auto& dataSlice : jobSummary.ReadInputDataSlices) {
        AdjustInputKeyBounds(dataSlice);
    }
    for (const auto& dataSlice : jobSummary.UnreadInputDataSlices) {
        AdjustInputKeyBounds(dataSlice);
    }

    try {
        GetChunkPoolOutput()->Completed(joblet->OutputCookie, jobSummary);
    } catch (const TErrorException& exception) {
        const auto& error = exception.Error();

        if (error.FindMatching(NChunkPools::EErrorCode::DataSliceLimitExceeded) ||
            error.FindMatching(NChunkPools::EErrorCode::MaxDataWeightPerJobExceeded) ||
            error.FindMatching(NChunkPools::EErrorCode::MaxPrimaryDataWeightPerJobExceeded))
        {
            YT_LOG_ERROR(error);

            result.OperationFailedError = error;
            return result;
        } else {
            throw;
        }
    }

    UpdateMaximumUsedTmpfsSizes(statistics);

    if (jobSummary.TotalInputDataStatistics) {
        auto dataWeight = jobSummary.TotalInputDataStatistics->data_weight();
        if (InputDataWeightHistogram_ && dataWeight > 0) {
            InputDataWeightHistogram_->AddValue(dataWeight);
        }
    }

    return result;
}

void TTask::ReinstallJob(std::function<void()> releaseOutputCookie)
{
    releaseOutputCookie();

    UpdateTask();
}

void TTask::ReleaseJobletResources(TJobletPtr joblet, bool waitForSnapshot)
{
    if (!joblet->CompetitionType) {
        TaskHost_->RemoveValueFromEstimatedHistogram(joblet);
        if (EstimatedInputDataWeightHistogram_) {
            EstimatedInputDataWeightHistogram_->RemoveValue(joblet->InputStripeList->TotalDataWeight);
        }
    }
    TaskHost_->ReleaseChunkTrees(joblet->ChunkListIds, /*recursive*/ true, waitForSnapshot);
}

TJobFinishedResult TTask::OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary)
{
    TJobFinishedResult result;

    TentativeTreeEligibility_.OnJobFinished(jobSummary, joblet->TreeId, joblet->TreeIsTentative, &result.NewlyBannedTrees);

    if (jobSummary.JobExecutionCompleted) {
        TaskHost_->RegisterStderr(joblet, jobSummary);
        TaskHost_->RegisterCores(joblet, jobSummary);
    }

    YT_VERIFY(jobSummary.Statistics);
    UpdateMaximumUsedTmpfsSizes(*jobSummary.Statistics);

    ReleaseJobletResources(joblet, /*waitForSnapshot*/ false);
    auto mayReturnCookie = true;
    for (auto* jobManager : JobManagers_) {
        if (!jobManager->OnJobFailed(joblet)) {
            mayReturnCookie = false;
        }
    }
    if (mayReturnCookie) {
        ReinstallJob([&] { GetChunkPoolOutput()->Failed(joblet->OutputCookie); });
    }

    JobSplitter_->OnJobFailed(jobSummary);

    return result;
}

TJobFinishedResult TTask::OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary)
{
    TJobFinishedResult result;

    TentativeTreeEligibility_.OnJobFinished(jobSummary, joblet->TreeId, joblet->TreeIsTentative, &result.NewlyBannedTrees);

    // NB: When job is aborted, you can never be sure that this is forever. Like in marriage. In future life (after
    // revival) it may become completed, and you will bite your elbows if you unstage its chunk lists too early (e.g.
    // job is aborted due to node gone offline, but after revival it happily comes back and job successfully completes).
    // So better keep it simple and wait for the snapshot.

    if (joblet->StderrTableChunkListId) {
        TaskHost_->ReleaseChunkTrees({joblet->StderrTableChunkListId}, true /*unstageRecursively*/, true /*waitForSnapshot*/);
    }
    if (joblet->CoreTableChunkListId) {
        TaskHost_->ReleaseChunkTrees({joblet->CoreTableChunkListId}, true /*unstageRecursively*/, true /*waitForSnapshot*/);
    }

    ReleaseJobletResources(joblet, /*waitForSnapshot*/ true);

    auto mayReturnCookie = true;
    for (auto* jobManager : JobManagers_) {
        if (!jobManager->OnJobAborted(joblet, jobSummary.AbortReason)) {
            mayReturnCookie = false;
        }
    }
    if (mayReturnCookie) {
        ReinstallJob([&] { GetChunkPoolOutput()->Aborted(joblet->OutputCookie, jobSummary.AbortReason); });
    }

    if (jobSummary.AbortReason == EAbortReason::ResourceOverdraft) {
        OnJobResourceOverdraft(joblet, jobSummary);
    }

    JobSplitter_->OnJobAborted(jobSummary);

    return result;
}

void TTask::OnJobRunning(TJobletPtr joblet, const TRunningJobSummary& jobSummary)
{
    auto jobId = joblet->JobId;

    if (joblet->JobSpeculationTimeout &&
        jobSummary.TimeStatistics.PrepareDuration.value_or(TDuration()) + jobSummary.TimeStatistics.ExecDuration.value_or(TDuration()) >= joblet->JobSpeculationTimeout)
    {
        YT_LOG_DEBUG("Speculation timeout expired; trying to launch speculative job (ExpiredJobId: %v)", jobId);
        if (TryRegisterSpeculativeJob(joblet)) {
            UpdateTask();
        }
    }

    if (jobSummary.Statistics) {
        JobSplitter_->OnJobRunning(jobSummary);
        if (HasNoPendingJobs()) {
            auto verdict = JobSplitter_->ExamineJob(jobId);
            if (verdict == EJobSplitterVerdict::Split) {
                YT_LOG_DEBUG("Job is going to be split (JobId: %v)", jobId);
                TaskHost_->InterruptJob(jobId, EInterruptionReason::JobSplit);
            } else if (verdict == EJobSplitterVerdict::LaunchSpeculative) {
                YT_LOG_DEBUG("Job can be speculated (JobId: %v)", jobId);
                if (TryRegisterSpeculativeJob(joblet)) {
                    UpdateTask();
                }
            }
        }
    }
}

void TTask::OnJobLost(TCompletedJobPtr completedJob, TChunkId chunkId)
{
    auto cookieAndPoll = TCookieAndPool(completedJob->OutputCookie, completedJob->DestinationPool);
    YT_VERIFY(LostJobCookieMap_.emplace(
        cookieAndPoll,
        completedJob->InputCookie).second);
    LostIntermediateChunkCookieMap_.emplace(cookieAndPoll, chunkId);
    for (auto* jobManager : JobManagers_) {
        jobManager->OnJobLost(completedJob->OutputCookie);
    }
}

void TTask::OnStripeRegistrationFailed(
    TError error,
    IChunkPoolInput::TCookie /*cookie*/,
    const TChunkStripePtr& /*stripe*/,
    const TOutputStreamDescriptorPtr& /*streamDescriptor*/)
{
    // NB: This method can be called during processing OnJob* event,
    // aborting all joblets are unsafe in this situation.
    TaskHost_->OnOperationFailed(error
        << TErrorAttribute("task_title", GetTitle()),
        /*flush*/ false,
        /*abortAllJoblets*/ false);
}

void TTask::OnTaskCompleted()
{
    StopTiming();

    ExperimentJobManager_.GenerateAlertIfNeeded(TaskHost_, GetVertexDescriptor());

    YT_LOG_DEBUG("Task completed");
}

void TTask::StopTiming()
{
    if (CompletionTime_) {
        return;
    }

    ReadyTimer_.Stop();
    ExhaustTimer_.Stop();
    CompletionTime_ = TInstant::Now();
}

std::optional<EScheduleFailReason> TTask::GetScheduleFailReason(const TSchedulingContext& /*context*/)
{
    return std::nullopt;
}

void TTask::DoCheckResourceDemandSanity(const TJobResources& neededResources)
{
    if (TaskHost_->ShouldSkipSanityCheck()) {
        return;
    }

    if (!Dominates(*TaskHost_->CachedMaxAvailableExecNodeResources(), neededResources)) {
        // It seems nobody can satisfy the demand.
        TaskHost_->OnOperationFailed(
            TError(
                EErrorCode::NoOnlineNodeToScheduleAllocation,
                "No online node can satisfy the resource demand")
                << TErrorAttribute("task_name", GetTitle())
                << TErrorAttribute("needed_resources", neededResources));
    }
}

void TTask::CheckResourceDemandSanity(const TJobResources& neededResources)
{
    auto maxAvailableExecNodeResources = TaskHost_->CachedMaxAvailableExecNodeResources();
    if (maxAvailableExecNodeResources && !Dominates(*maxAvailableExecNodeResources, neededResources)) {
        // Schedule check in controller thread.
        TaskHost_->GetCancelableInvoker()->Invoke(BIND(
            &TTask::DoCheckResourceDemandSanity,
            MakeWeak(this),
            neededResources));
    }
}

IDigest* TTask::GetUserJobMemoryDigest() const
{
    if (!UserJobMemoryDigest_) {
        const auto& userJobSpec = GetUserJobSpec();
        YT_VERIFY(userJobSpec);

        auto config = New<TLogDigestConfig>();
        config->LowerBound = userJobSpec->UserJobMemoryDigestLowerBound;
        config->DefaultValue = userJobSpec->UserJobMemoryDigestDefaultValue;
        config->UpperBound = 1.0;
        config->RelativePrecision = TaskHost_->GetConfig()->UserJobMemoryDigestPrecision;
        UserJobMemoryDigest_ = CreateLogDigest(std::move(config));
    }

    return UserJobMemoryDigest_.Get();
}

IDigest* TTask::GetJobProxyMemoryDigest() const
{
    if (!JobProxyMemoryDigest_) {
        if (const auto& userJobSpec = GetUserJobSpec(); userJobSpec && userJobSpec->JobProxyMemoryDigest) {
            JobProxyMemoryDigest_ = CreateLogDigest(userJobSpec->JobProxyMemoryDigest);
        } else {
            JobProxyMemoryDigest_ = CreateLogDigest(TaskHost_->GetSpec()->JobProxyMemoryDigest);
        }
    }

    return JobProxyMemoryDigest_.Get();
}

void TTask::AddSequentialInputSpec(
    TJobSpec* jobSpec,
    TJobletPtr joblet,
    TComparator comparator)
{
    YT_ASSERT_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

    auto* jobSpecExt = jobSpec->MutableExtension(TJobSpecExt::job_spec_ext);
    auto nodeDirectoryBuilderFactory = TNodeDirectoryBuilderFactory(
        jobSpecExt,
        TaskHost_->GetInputManager(),
        TaskHost_->GetOperationType());
    auto* inputSpec = jobSpecExt->add_input_table_specs();
    const auto& list = joblet->InputStripeList;
    for (const auto& stripe : list->Stripes) {
        AddChunksToInputSpec(
            IsInput_ ? nodeDirectoryBuilderFactory.GetNodeDirectoryBuilder(stripe).get() : nullptr,
            inputSpec,
            stripe,
            comparator);
    }
    UpdateInputSpecTotals(jobSpec, joblet);
}

void TTask::AddParallelInputSpec(
    TJobSpec* jobSpec,
    TJobletPtr joblet,
    TComparator comparator)
{
    YT_ASSERT_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

    auto* jobSpecExt = jobSpec->MutableExtension(TJobSpecExt::job_spec_ext);
    auto directoryBuilderFactory = TNodeDirectoryBuilderFactory(
        jobSpecExt,
        TaskHost_->GetInputManager(),
        TaskHost_->GetOperationType());
    const auto& list = joblet->InputStripeList;
    for (const auto& stripe : list->Stripes) {
        auto* inputSpec = stripe->Foreign
            ? jobSpecExt->add_foreign_input_table_specs()
            : jobSpecExt->add_input_table_specs();
        AddChunksToInputSpec(
            IsInput_ ? directoryBuilderFactory.GetNodeDirectoryBuilder(stripe).get() : nullptr,
            inputSpec,
            stripe,
            comparator);
    }
    UpdateInputSpecTotals(jobSpec, joblet);
}

void TTask::AddChunksToInputSpec(
    TNodeDirectoryBuilder* directoryBuilder,
    TTableInputSpec* inputSpec,
    TChunkStripePtr stripe,
    TComparator comparator)
{
    YT_ASSERT_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

    stripe = GetChunkMapping()->GetMappedStripe(stripe);

    for (const auto& dataSlice : stripe->DataSlices) {
        YT_VERIFY(!dataSlice->IsLegacy);
        AdjustOutputKeyBounds(dataSlice);
        YT_VERIFY(!dataSlice->IsLegacy);

        inputSpec->add_chunk_spec_count_per_data_slice(dataSlice->ChunkSlices.size());
        inputSpec->add_virtual_row_index_per_data_slice(dataSlice->VirtualRowIndex.value_or(-1));
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            auto* newChunkSpec = inputSpec->add_chunk_specs();
            YT_LOG_TRACE(
                "Serializing chunk slice (LowerLimit: %v, UpperLimit: %v)",
                chunkSlice->LowerLimit().KeyBound,
                chunkSlice->UpperLimit().KeyBound);

            // This is a dirty hack. Comparator is needed in ToProto to overcome YT-14023.
            // Issue happens only for (operation) input data slices in sorted controller.
            // So, we have to pass comparator only if we are an input task and comparator is actually
            // present on the input table.
            if (IsInput_) {
                const auto& inputTable = TaskHost_->GetInputTable(dataSlice->GetTableIndex());
                // For input tasks comparator is inferred from the input table schema.
                // For non-input tasks comparator is passed from the controller. Actually
                // it's now used for sorted merge task in sort controller only.
                comparator = inputTable->Comparator;
                newChunkSpec->set_use_proxying_data_node_service(inputTable->UseReadViaExecNode() || TaskHost_->GetSpec()->ReadViaExecNode);
            }

            if (comparator) {
                chunkSlice->LowerLimit().MergeLower(dataSlice->LowerLimit(), comparator);
                chunkSlice->UpperLimit().MergeUpper(dataSlice->UpperLimit(), comparator);
            }

            ToProto(newChunkSpec, chunkSlice, comparator, dataSlice->Type);
            if (dataSlice->Tag) {
                newChunkSpec->set_data_slice_tag(*dataSlice->Tag);
            }

            if (directoryBuilder) {
                auto replicas = chunkSlice->GetInputChunk()->GetReplicaList();
                directoryBuilder->Add(replicas);
            }
        }
    }
}

void TTask::UpdateInputSpecTotals(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    const auto& list = joblet->InputStripeList;
    auto* jobSpecExt = jobSpec->MutableExtension(TJobSpecExt::job_spec_ext);
    jobSpecExt->set_input_data_weight(
        jobSpecExt->input_data_weight() +
        list->TotalDataWeight);
    jobSpecExt->set_input_row_count(
        jobSpecExt->input_row_count() +
        list->TotalRowCount);
}

TString TTask::GetOrCacheSerializedSchema(const TTableSchemaPtr& schema)
{
    YT_ASSERT_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

    {
        auto guard = ReaderGuard(TableSchemaToProtobufTableSchemaLock_);
        auto it = TableSchemaToProtobufTableSchema_.find(schema);
        if (it != TableSchemaToProtobufTableSchema_.end()) {
            return it->second;
        }
    }
    auto serializedSchema = SerializeToWireProto(schema);
    {
        auto guard = WriterGuard(TableSchemaToProtobufTableSchemaLock_);
        auto it = TableSchemaToProtobufTableSchema_.find(schema);
        if (it == TableSchemaToProtobufTableSchema_.end()) {
            YT_VERIFY(TableSchemaToProtobufTableSchema_.emplace(schema, serializedSchema).second);
            return serializedSchema;
        } else {
            return it->second;
        }
    }
}

void TTask::AddOutputTableSpecs(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    YT_ASSERT_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

    const auto& outputStreamDescriptors = joblet->OutputStreamDescriptors;
    YT_VERIFY(joblet->ChunkListIds.size() == outputStreamDescriptors.size());
    auto* jobSpecExt = jobSpec->MutableExtension(TJobSpecExt::job_spec_ext);
    for (int index = 0; index < std::ssize(outputStreamDescriptors); ++index) {
        const auto& streamDescriptor = outputStreamDescriptors[index];
        auto* outputSpec = jobSpecExt->add_output_table_specs();
        outputSpec->set_table_writer_options(ConvertToYsonString(streamDescriptor->TableWriterOptions).ToString());
        if (streamDescriptor->TableWriterConfig) {
            outputSpec->set_table_writer_config(streamDescriptor->TableWriterConfig.ToString());
        }
        const auto& outputTableSchema = streamDescriptor->TableUploadOptions.TableSchema.Get();
        auto schemaId = streamDescriptor->TableUploadOptions.SchemaId;
        outputSpec->set_table_schema(GetOrCacheSerializedSchema(outputTableSchema));
        ToProto(outputSpec->mutable_schema_id(), schemaId);
        ToProto(outputSpec->mutable_chunk_list_id(), joblet->ChunkListIds[index]);
        if (streamDescriptor->Timestamp != NullTimestamp) {
            outputSpec->set_timestamp(streamDescriptor->Timestamp);
        }
        outputSpec->set_dynamic(streamDescriptor->IsOutputTableDynamic);
        for (const auto& streamSchema : streamDescriptor->StreamSchemas) {
            outputSpec->add_stream_schemas(GetOrCacheSerializedSchema(streamSchema));
        }
    }

    YT_LOG_DEBUG("Adding input stream schemas (Task: %v, Count: %v)",
        joblet->Task->GetTitle(),
        joblet->InputStreamDescriptors.size());
    const auto& inputStreamDescriptors = joblet->InputStreamDescriptors;
    for (int index = 0; index < std::ssize(inputStreamDescriptors); ++index) {
        const auto& streamDescriptor = inputStreamDescriptors[index];
        for (const auto& streamSchema : streamDescriptor->StreamSchemas) {
            jobSpecExt->add_input_stream_schemas(GetOrCacheSerializedSchema(streamSchema));
        }
    }
}

TInputChunkMappingPtr TTask::GetChunkMapping() const
{
    return InputChunkMapping_;
}

void TTask::ResetCachedMinNeededResources()
{
    CachedMinNeededResources_.reset();
}

void TTask::UpdateMemoryDigests(const TJobletPtr& joblet, bool resourceOverdraft)
{
    const auto& statistics = *joblet->JobStatistics;
    if (auto userJobMaxMemoryUsage = FindNumericValue(statistics, "/user_job/max_memory"_SP)) {
        auto* digest = GetUserJobMemoryDigest();
        YT_VERIFY(digest);
        double actualFactor = static_cast<double>(*userJobMaxMemoryUsage) / joblet->EstimatedResourceUsage.GetUserJobMemory();
        if (resourceOverdraft && actualFactor >= 1.0) {
            // During resource overdraft actual max memory values may be outdated,
            // since statistics are updated periodically. To ensure that digest converge to large enough
            // values we introduce additional factor.
            actualFactor = std::max(actualFactor, *joblet->UserJobMemoryReserveFactor * TaskHost_->GetConfig()->MemoryDigestResourceOverdraftFactor);
        }
        YT_LOG_DEBUG("Adding sample to the user job memory digest (Sample: %v, JobId: %v, ResourceOverdraft: %v)",
            actualFactor,
            joblet->JobId,
            resourceOverdraft);
        digest->AddSample(actualFactor);
    }

    if (auto jobProxyMaxMemoryUsage = FindNumericValue(statistics, "/job_proxy/max_memory"_SP)) {
        auto* digest = GetJobProxyMemoryDigest();
        YT_VERIFY(digest);
        double actualFactor = static_cast<double>(*jobProxyMaxMemoryUsage) /
            (joblet->EstimatedResourceUsage.GetJobProxyMemory() + joblet->EstimatedResourceUsage.GetFootprintMemory());
        if (resourceOverdraft && actualFactor >= 1.0) {
            actualFactor = std::max(actualFactor, *joblet->JobProxyMemoryReserveFactor * TaskHost_->GetConfig()->MemoryDigestResourceOverdraftFactor);
        }
        YT_LOG_DEBUG("Adding sample to the job proxy memory digest (Sample: %v, JobId: %v, ResourceOverdraft: %v)",
            actualFactor,
            joblet->JobId,
            resourceOverdraft);
        digest->AddSample(actualFactor);
    }
}

std::optional<TDuration> TTask::InferWaitingForResourcesTimeout(
    const NScheduler::NProto::TScheduleAllocationSpec& allocationSpec) const
{
    if (TaskHost_->GetSpec()->WaitingJobTimeout && allocationSpec.has_waiting_for_resources_on_node_timeout()) {
        return std::max(*TaskHost_->GetSpec()->WaitingJobTimeout, FromProto<TDuration>(allocationSpec.waiting_for_resources_on_node_timeout()));
    }
    if (TaskHost_->GetSpec()->WaitingJobTimeout) {
        return *TaskHost_->GetSpec()->WaitingJobTimeout;
    }
    if (allocationSpec.waiting_for_resources_on_node_timeout()) {
        return FromProto<TDuration>(allocationSpec.waiting_for_resources_on_node_timeout());
    }
    return std::nullopt;
}

TJobResources TTask::ApplyMemoryReserve(
    const TExtendedJobResources& jobResources,
    double jobProxyMemoryReserveFactor,
    std::optional<double> userJobMemoryReserveFactor) const
{
    TJobResources result;
    result.SetCpu(jobResources.GetCpu());
    result.SetGpu(jobResources.GetGpu());
    result.SetUserSlots(jobResources.GetUserSlots());
    i64 memory = jobResources.GetFootprintMemory();
    memory += jobResources.GetJobProxyMemory() * jobProxyMemoryReserveFactor;
    if (HasUserJob()) {
        YT_VERIFY(userJobMemoryReserveFactor);
        memory += jobResources.GetUserJobMemory() * *userJobMemoryReserveFactor;
    } else {
        YT_VERIFY(jobResources.GetUserJobMemory() == 0);
    }
    result.SetMemory(memory);
    result.SetNetwork(jobResources.GetNetwork());
    return result;
}

void TTask::OnJobResourceOverdraft(TJobletPtr joblet, const TAbortedJobSummary& jobSummary)
{
    auto Logger = this->Logger
        .WithTag("JobId: %v", joblet->JobId);

    auto& state = ResourceOverdraftedOutputCookieToState_[joblet->OutputCookie];
    const auto& userJobSpec = GetUserJobSpec();

    state.LastJobId = joblet->JobId;

    double userJobMemoryReserveUpperBound = 1.0;
    double jobProxyMemoryReserveUpperBound = TaskHost_->GetSpec()->JobProxyMemoryDigest->UpperBound;

    auto jobProxyMaxMemory = FindNumericValue(*jobSummary.Statistics, "/job_proxy/max_memory"_SP).value_or(0);
    auto jobProxyDedicatedMemory = joblet->EstimatedResourceUsage.GetJobProxyMemory() * joblet->JobProxyMemoryReserveFactor.value();
    bool hasJobProxyMemoryOverdraft = jobProxyMaxMemory > jobProxyDedicatedMemory;

    i64 userJobMaxMemory = FindNumericValue(*jobSummary.Statistics, "/user_job/max_memory"_SP).value_or(0);
    bool hasUserJobMemoryOverdraft = userJobSpec
        ? userJobMaxMemory > joblet->UserJobMemoryReserve
        : false;

    // NB: In case of outdated statistics we suppose that memory overdraft happened
    // for the process with smallest gap between max memory and reserved memory.
    if (!hasJobProxyMemoryOverdraft && !hasUserJobMemoryOverdraft) {
        YT_LOG_DEBUG(
            "Job was aborted with resource overdraft, but user job and job proxy memory does not exceed the limit; "
            "choose overdrafted process by free memory gap "
            "(JobProxyMaxMemory: %v, JobProxyDedicatedMemory: %v, UserJobMaxMemory: %v, UserJobMemoryReserve: %v)",
            jobProxyMaxMemory,
            jobProxyDedicatedMemory,
            userJobSpec ? std::make_optional(userJobMaxMemory) : std::nullopt,
            userJobSpec ? std::make_optional(joblet->UserJobMemoryReserve) : std::nullopt);
        auto jobProxyMemoryGap = jobProxyDedicatedMemory - jobProxyMaxMemory;
        auto userJobMemoryGap = joblet->UserJobMemoryReserve - userJobMaxMemory;
        if (userJobSpec && userJobMemoryGap < jobProxyMemoryGap) {
            hasUserJobMemoryOverdraft = true;
        } else {
            hasJobProxyMemoryOverdraft = true;
        }
    }

    if (hasJobProxyMemoryOverdraft) {
        if (state.JobProxyStatus == EResourceOverdraftStatus::None && JobProxyMemoryMultiplier_) {
            state.JobProxyStatus = EResourceOverdraftStatus::Once;
            state.DedicatedJobProxyMemoryReserveFactor = std::min(
                joblet->JobProxyMemoryReserveFactor.value() * JobProxyMemoryMultiplier_.value(),
                jobProxyMemoryReserveUpperBound);
        } else {
            state.JobProxyStatus = EResourceOverdraftStatus::MultipleTimes;
            state.DedicatedJobProxyMemoryReserveFactor = jobProxyMemoryReserveUpperBound;
        }
    }

    if (hasUserJobMemoryOverdraft) {
        if (state.UserJobStatus == EResourceOverdraftStatus::None && UserJobMemoryMultiplier_) {
            state.UserJobStatus = EResourceOverdraftStatus::Once;
            state.DedicatedUserJobMemoryReserveFactor = std::min(
                joblet->UserJobMemoryReserveFactor.value() * UserJobMemoryMultiplier_.value(),
                userJobMemoryReserveUpperBound);
        } else {
            state.UserJobStatus = EResourceOverdraftStatus::MultipleTimes;
            state.DedicatedUserJobMemoryReserveFactor = userJobMemoryReserveUpperBound;
        }
    }

    YT_LOG_DEBUG(
        "Job was aborted with resource overdraft "
        "(HasUserJobMemoryOverdraft: %v, HasJobProxyMemoryOverdraft: %v, UserJobOverdraftStatus: %v, JobProxyOverdraftStatus: %v)",
        hasUserJobMemoryOverdraft,
        hasJobProxyMemoryOverdraft,
        state.UserJobStatus,
        state.JobProxyStatus);
}

void TTask::UpdateMaximumUsedTmpfsSizes(const TStatistics& statistics)
{
    if (!IsSimpleTask()) {
        return;
    }

    auto userJobSpec = GetUserJobSpec();
    if (!userJobSpec) {
        return;
    }

    for (int index = 0; index < std::ssize(userJobSpec->TmpfsVolumes); ++index) {
        auto maxUsedTmpfsSize = FindNumericValue(
            statistics,
            SlashedStatisticPath(Format("/user_job/tmpfs_volumes/%v/max_size", index)).ValueOrThrow());
        if (!maxUsedTmpfsSize) {
            continue;
        }

        auto& maxTmpfsSize = MaximumUsedTmpfsSizes_[index];
        if (!maxTmpfsSize || *maxTmpfsSize < *maxUsedTmpfsSize) {
            maxTmpfsSize = *maxUsedTmpfsSize;
        }
    }
}

void TTask::FinishTaskInput(const TTaskPtr& task) const
{
    task->RegisterInGraph(GetVertexDescriptor());
    task->FinishInput();
}

void TTask::SetStreamDescriptors(TJobletPtr joblet) const
{
    joblet->OutputStreamDescriptors = OutputStreamDescriptors_;
    joblet->InputStreamDescriptors = InputStreamDescriptors_;
}

bool TTask::IsInputDataWeightHistogramSupported() const
{
    return true;
}

TSharedRef TTask::BuildJobSpecProto(TJobletPtr joblet, const std::optional<NScheduler::NProto::TScheduleAllocationSpec>& scheduleAllocationSpec)
{
    YT_ASSERT_INVOKER_AFFINITY(TaskHost_->GetJobSpecBuildInvoker());

    auto jobSpec = ObjectPool<TJobSpec>().Allocate();

    BuildJobSpec(joblet, jobSpec.get());
    jobSpec->set_version(GetJobSpecVersion());
    TaskHost_->CustomizeJobSpec(joblet, jobSpec.get());

    auto* jobSpecExt = jobSpec->MutableExtension(TJobSpecExt::job_spec_ext);
    if (TaskHost_->GetSpec()->JobProxyMemoryOvercommitLimit) {
        jobSpecExt->set_job_proxy_memory_overcommit_limit(*TaskHost_->GetSpec()->JobProxyMemoryOvercommitLimit);
    }
    jobSpecExt->set_job_proxy_ref_counted_tracker_log_period(ToProto(TaskHost_->GetSpec()->JobProxyRefCountedTrackerLogPeriod));
    jobSpecExt->set_abort_job_if_account_limit_exceeded(TaskHost_->GetSpec()->SuspendOperationIfAccountLimitExceeded);
    jobSpecExt->set_allow_idle_cpu_policy(TaskHost_->IsIdleCpuPolicyAllowedInTree(joblet->TreeId));
    if (TaskHost_->GetSpec()->ForceJobProxyTracing) {
        jobSpecExt->set_is_traced(true);
    }

    if (scheduleAllocationSpec) {
        if (auto waitingJobTimeout = InferWaitingForResourcesTimeout(*scheduleAllocationSpec)) {
            jobSpecExt->set_waiting_job_timeout(ToProto(*waitingJobTimeout));
        }
    }

    // Adjust sizes if approximation flag is set.
    if (joblet->InputStripeList->IsApproximate) {
        jobSpecExt->set_input_data_weight(static_cast<i64>(
            jobSpecExt->input_data_weight() *
            ApproximateSizesBoostFactor));
        jobSpecExt->set_input_row_count(static_cast<i64>(
            jobSpecExt->input_row_count() *
            ApproximateSizesBoostFactor));
    }

    jobSpecExt->set_job_cpu_monitor_config(ConvertToYsonString(TaskHost_->GetSpec()->JobCpuMonitor).ToString());

    if (jobSpecExt->input_data_weight() > TaskHost_->GetSpec()->MaxDataWeightPerJob) {
        auto error = TError(
            NChunkPools::EErrorCode::MaxDataWeightPerJobExceeded,
            "Maximum allowed data weight per job exceeds the limit: %v > %v",
            jobSpecExt->input_data_weight(),
            TaskHost_->GetSpec()->MaxDataWeightPerJob);
        TaskHost_->GetCancelableInvoker()->Invoke(BIND(
            &ITaskHost::OnOperationFailed,
            MakeWeak(TaskHost_),
            error,
            /*flush*/ true,
            /*abortAllJoblets*/ true));
        THROW_ERROR(error);
    }

    ToProto(jobSpecExt->mutable_job_competition_id(), joblet->CompetitionIds[EJobCompetitionType::Speculative]);
    ToProto(jobSpecExt->mutable_probing_job_competition_id(), joblet->CompetitionIds[EJobCompetitionType::Probing]);

    jobSpecExt->set_task_name(GetVertexDescriptor());
    jobSpecExt->set_tree_id(joblet->TreeId);
    jobSpecExt->set_authenticated_user(ToProto(TaskHost_->GetAuthenticatedUser()));

    auto ioTags = CreateEphemeralAttributes();
    if (joblet->PoolPath) {
        const auto& poolPath = *joblet->PoolPath;
        AddTagToBaggage(ioTags, EAggregateIOTag::Pool, DirNameAndBaseName(poolPath).second);
        AddTagToBaggage(ioTags, EAggregateIOTag::PoolPath, poolPath);
    }
    AddTagToBaggage(ioTags, EAggregateIOTag::OperationType, FormatEnum(GetTaskHost()->GetOperationType()));
    AddTagToBaggage(ioTags, EAggregateIOTag::TaskName, GetVertexDescriptor());
    AddTagToBaggage(ioTags, EAggregateIOTag::PoolTree, joblet->TreeId);
    ToProto(jobSpecExt->mutable_io_tags(), *ioTags);

    jobSpecExt->set_interruptible(joblet->JobInterruptible);

    jobSpecExt->set_ignore_yt_variables_in_shell_environment(TaskHost_->GetSpec()->IgnoreYTVariablesInShellEnvironment);

    return SerializeProtoToRefWithEnvelope(*jobSpec, TaskHost_->GetConfig()->JobSpecCodec);
}

bool TTask::IsJobInterruptible() const
{
    return TaskHost_->CanInterruptJobs();
}

void TTask::AddFootprintAndUserJobResources(TExtendedJobResources& jobResources) const
{
    jobResources.SetFootprintMemory(TaskHost_->GetConfig()->FootprintMemory.value_or(GetFootprintMemorySize()));
    auto userJobSpec = GetUserJobSpec();
    if (userJobSpec) {
        jobResources.SetUserJobMemory(userJobSpec->MemoryLimit);
        jobResources.SetGpu(userJobSpec->GpuLimit);
    }
}

void TTask::RegisterOutput(
    TCompletedJobSummary& completedJobSummary,
    const std::vector<NChunkClient::TChunkListId>& chunkListIds,
    TJobletPtr joblet,
    const NChunkPools::TChunkStripeKey& key,
    bool processEmptyStripes)
{
    if (completedJobSummary.Abandoned) {
        return;
    }

    auto& jobResultExt = completedJobSummary.GetJobResultExt();

    auto outputStripes = BuildOutputChunkStripes(
        jobResultExt,
        chunkListIds,
        jobResultExt.output_boundary_keys());
    PropagatePartitions(
        joblet->OutputStreamDescriptors,
        joblet->InputStripeList,
        &outputStripes);

    const auto& streamDescriptors = joblet->OutputStreamDescriptors;
    for (int tableIndex = 0; tableIndex < std::ssize(streamDescriptors); ++tableIndex) {
        if (outputStripes[tableIndex]) {
            const auto& streamDescriptor = streamDescriptors[tableIndex];
            for (const auto& dataSlice : outputStripes[tableIndex]->DataSlices) {
                TaskHost_->RegisterLivePreviewChunk(
                    GetVertexDescriptor(),
                    streamDescriptor->LivePreviewIndex,
                    dataSlice->GetSingleUnversionedChunk());
            }

            std::optional<TMD5Hash> hash;
            if (tableIndex < jobResultExt.output_digests_size()) {
                FromProto(&hash, jobResultExt.output_digests()[tableIndex]);
            }

            RegisterStripe(
                std::move(outputStripes[tableIndex]),
                streamDescriptor,
                joblet,
                key,
                processEmptyStripes,
                hash);
        }
    }
}

TJobResourcesWithQuota TTask::GetMinNeededResources() const
{
    if (!CachedMinNeededResources_) {
        // NB: Don't call GetMinNeededResourcesHeavy if there are no pending jobs.
        if (HasNoPendingJobs()) {
            return TJobResourcesWithQuota{};
        }
        CachedMinNeededResources_ = GetMinNeededResourcesHeavy();
    }
    auto result = ApplyMemoryReserve(
        *CachedMinNeededResources_,
        GetJobProxyMemoryReserveFactor(),
        GetUserJobMemoryReserveFactor());
    if (result.GetUserSlots() > 0 && result.GetMemory() == 0) {
        YT_LOG_WARNING("Found min needed resources of task with non-zero user slots and zero memory");
    }
    auto resultWithQuota = TJobResourcesWithQuota(result);
    if (auto userJobSpec = GetUserJobSpec()) {
        if (userJobSpec->DiskRequest) {
            resultWithQuota.DiskQuota() = CreateDiskQuota(userJobSpec->DiskRequest, TaskHost_->GetMediumDirectory());
        }
    }
    return resultWithQuota;
}

void TTask::RegisterStripe(
    TChunkStripePtr stripe,
    const TOutputStreamDescriptorPtr& streamDescriptor,
    TJobletPtr joblet,
    TChunkStripeKey key,
    bool processEmptyStripes,
    const std::optional<TMD5Hash>& digest)
{
    if (stripe->DataSlices.empty() && !stripe->ChunkListId) {
        return;
    }

    if (stripe->DataSlices.empty() && !processEmptyStripes) {
        return;
    }

    if (digest) {
        ++ReceivedDigestCount_;
    }

    YT_VERIFY(joblet);

    const auto& destinationPool = streamDescriptor->DestinationPool;
    if (streamDescriptor->RequiresRecoveryInfo) {
        const auto& chunkMapping = streamDescriptor->ChunkMapping;
        YT_VERIFY(chunkMapping);

        YT_LOG_DEBUG("Registering stripe in a direction that requires recovery info (JobId: %v, Restarted: %v, JobType: %v)",
            joblet->JobId,
            joblet->Restarted,
            joblet->JobType);

        IChunkPoolInput::TCookie inputCookie = IChunkPoolInput::NullCookie;
        TCookieAndPool outputCookie{joblet->OutputCookie, streamDescriptor->DestinationPool};

        if (digest) {
            auto digestIt = JobOutputHash_.find(outputCookie);
            if (digestIt != JobOutputHash_.end() && digestIt->second != digest) {
                TaskHost_->SetOperationAlert(EOperationAlertType::JobIsNotDeterministic,
                    TError("Restarted job produced dissimilar output; "
                           "this may lead to inconsistent operation results; "
                           "consider setting enable_intermediate_output_recalculation=%%false.")
                        << TErrorAttribute("task_name", joblet->Task->GetVertexDescriptor())
                        << TErrorAttribute("job_id", joblet->JobId));
            }
            if (digestIt == JobOutputHash_.end()) {
                JobOutputHash_.emplace(outputCookie, *digest);
            }
        }

        auto lostIt = LostJobCookieMap_.find(outputCookie);
        if (lostIt == LostJobCookieMap_.end()) {
            // NB: If job is not restarted, we should not add its output for the
            // second time to the destination pools that did not trigger the replay.
            if (!joblet->Restarted) {
                inputCookie = destinationPool->AddWithKey(stripe, key);
                if (inputCookie != IChunkPoolInput::NullCookie) {
                    chunkMapping->Add(inputCookie, stripe);
                }
            }
        } else {
            inputCookie = lostIt->second;
            YT_VERIFY(inputCookie != IChunkPoolInput::NullCookie);
            try {
                chunkMapping->OnStripeRegenerated(inputCookie, stripe);
                YT_LOG_DEBUG("Successfully registered recovered stripe in chunk mapping (JobId: %v, JobType: %v, InputCookie: %v)",
                    joblet->JobId,
                    joblet->JobType,
                    inputCookie);
            } catch (const std::exception& ex) {
                auto error = TError("Failure while registering result stripe of a restarted job in a chunk mapping")
                    << ex
                    << TErrorAttribute("input_cookie", inputCookie);
                YT_LOG_ERROR(error);
                OnStripeRegistrationFailed(error, lostIt->second, stripe, streamDescriptor);
            }

            destinationPool->Resume(inputCookie);

            LostJobCookieMap_.erase(lostIt);
        }

        // If destination pool decides not to do anything with this data,
        // then there is no need to store any recovery info.
        if (inputCookie == IChunkPoolInput::NullCookie) {
            return;
        }

        // Store recovery info.
        auto completedJob = New<TCompletedJob>();
        completedJob->JobId = joblet->JobId;
        completedJob->SourceTask = this;
        completedJob->OutputCookie = joblet->OutputCookie;
        completedJob->DataWeight = joblet->InputStripeList->TotalDataWeight;
        completedJob->DestinationPool = destinationPool;
        completedJob->InputCookie = inputCookie;
        completedJob->Restartable = CanLoseJobs();
        completedJob->InputStripe = stripe;
        completedJob->NodeDescriptor = joblet->NodeDescriptor;

        TaskHost_->RegisterRecoveryInfo(
            completedJob,
            stripe);
    } else {
        if (!joblet->Restarted) {
            destinationPool->AddWithKey(stripe, key);
        }
    }
}

std::vector<TChunkStripePtr> TTask::BuildChunkStripes(
    google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs,
    int tableCount)
{
    std::vector<TChunkStripePtr> stripes(tableCount);
    for (int index = 0; index < tableCount; ++index) {
        stripes[index] = New<TChunkStripe>();
    }

    i64 currentTableRowIndex = 0;
    for (int index = 0; index < chunkSpecs->size(); ++index) {
        auto inputChunk = New<TInputChunk>(std::move(*chunkSpecs->Mutable(index)));
        // NB(max42): Having correct table row indices on intermediate data is important for
        // some chunk pools. This affects the correctness of sort operation with sorted
        // merge phase over several intermediate chunks.
        inputChunk->SetTableRowIndex(currentTableRowIndex);
        currentTableRowIndex += inputChunk->GetRowCount();
        auto chunkSlice = CreateInputChunkSlice(std::move(inputChunk));
        chunkSlice->SetSliceIndex(index);
        auto dataSlice = CreateUnversionedInputDataSlice(std::move(chunkSlice));
        // TODO(max42): revisit this.
        dataSlice->TransformToNewKeyless();
        // NB(max42): This heavily relies on the property of intermediate data being deterministic
        // (i.e. it may be reproduced with exactly the same content divided into chunks with exactly
        // the same boundary keys when the job output is lost).
        dataSlice->Tag = index;
        int tableIndex = inputChunk->GetTableIndex();
        if (tableIndex == -1) {
            tableIndex = 0;
        }
        dataSlice->SetInputStreamIndex(tableIndex);
        YT_VERIFY(tableIndex >= 0);
        YT_VERIFY(tableIndex < tableCount);
        stripes[tableIndex]->DataSlices.emplace_back(std::move(dataSlice));
    }
    return stripes;
}

TChunkStripePtr TTask::BuildIntermediateChunkStripe(
    google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs)
{
    auto stripes = BuildChunkStripes(chunkSpecs, 1 /*tableCount*/);
    return std::move(stripes[0]);
}

std::vector<TChunkStripePtr> TTask::BuildOutputChunkStripes(
    TJobResultExt& jobResultExt,
    const std::vector<NChunkClient::TChunkTreeId>& chunkTreeIds,
    google::protobuf::RepeatedPtrField<NControllerAgent::NProto::TOutputResult> boundaryKeysPerTable)
{
    auto stripes = BuildChunkStripes(jobResultExt.mutable_output_chunk_specs(), chunkTreeIds.size());
    // Some stream descriptors do not require boundary keys to be returned,
    // so they are skipped in `boundaryKeysPerTable`.
    int boundaryKeysIndex = 0;
    for (int tableIndex = 0; tableIndex < std::ssize(chunkTreeIds); ++tableIndex) {
        stripes[tableIndex]->ChunkListId = chunkTreeIds[tableIndex];
        if (OutputStreamDescriptors_[tableIndex]->TableWriterOptions->ReturnBoundaryKeys) {
            // TODO(max42): do not send empty or unsorted boundary keys, this is meaningless.
            if (boundaryKeysIndex < boundaryKeysPerTable.size() &&
                !boundaryKeysPerTable.Get(boundaryKeysIndex).empty() &&
                boundaryKeysPerTable.Get(boundaryKeysIndex).sorted())
            {
                stripes[tableIndex]->BoundaryKeys = BuildBoundaryKeysFromOutputResult(
                    boundaryKeysPerTable.Get(boundaryKeysIndex),
                    OutputStreamDescriptors_[tableIndex],
                    TaskHost_->GetRowBuffer());
            }
            ++boundaryKeysIndex;
        }
    }
    return stripes;
}

void TTask::SetupCallbacks()
{
    GetJobCounter()->SubscribePendingUpdated(BIND(&TTask::OnPendingJobCountUpdated, MakeWeak(this)));
}

std::vector<TString> TTask::FindAndBanSlowTentativeTrees()
{
    return TentativeTreeEligibility_.FindAndBanSlowTentativeTrees();
}

void TTask::LogTentativeTreeStatistics() const
{
    return TentativeTreeEligibility_.LogTentativeTreeStatistics();
}

void TTask::AsyncAbortJob(TJobId jobId, EAbortReason reason)
{
    GetTaskHost()->AsyncAbortJob(jobId, reason);
}

void TTask::AbortJob(TJobId jobId, EAbortReason reason)
{
    GetTaskHost()->AbortJob(jobId, reason);
}

void TTask::OnSecondaryJobScheduled(const TJobletPtr& joblet, EJobCompetitionType competitionType)
{
    GetTaskHost()->OnCompetitiveJobScheduled(joblet, competitionType);
}

double TTask::GetJobProxyMemoryReserveFactor() const
{
    return GetJobProxyMemoryDigest()->GetQuantile(TaskHost_->GetConfig()->JobProxyMemoryReserveQuantile);
}

std::optional<double> TTask::GetUserJobMemoryReserveFactor() const
{
    if (!HasUserJob()) {
        return std::nullopt;
    }

    return GetUserJobMemoryDigest()->GetQuantile(TaskHost_->GetConfig()->UserJobMemoryReserveQuantile);
}

int TTask::EstimateSplitJobCount(const TCompletedJobSummary& jobSummary, const TJobletPtr& joblet)
{
    YT_VERIFY(jobSummary.TotalInputDataStatistics);
    const auto& inputDataStatistics = *jobSummary.TotalInputDataStatistics;

    // We don't estimate unread row count based on unread slices,
    // because foreign slices are not passed back to scheduler.
    // Instead, we take the difference between estimated row count and actual read row count.
    i64 unreadRowCount = joblet->InputStripeList->TotalRowCount - inputDataStatistics.row_count();

    if (unreadRowCount <= 0) {
        // This is almost impossible, still we don't want to fail operation in this case.
        YT_LOG_WARNING("Estimated unread row count is negative (JobId: %v, UnreadRowCount: %v)", jobSummary.Id, unreadRowCount);
        unreadRowCount = 1;
    }

    auto splitJobCount = JobSplitter_->EstimateJobCount(jobSummary, unreadRowCount);

    if (!HasNoPendingJobs()) {
        splitJobCount = 1;
    }

    if (jobSummary.InterruptionReason == EInterruptionReason::JobSplit) {
        // If we interrupted job on our own decision, (from JobSplitter), we should at least try to split it into 2 pieces.
        // Otherwise, the whole splitting thing makes to sense.
        splitJobCount = std::max(2, splitJobCount);
    }

    return splitJobCount;
}

TJobProfilerSpecPtr TTask::SelectProfiler()
{
    const auto& spec = TaskHost_->GetSpec();
    const auto* profilers = &spec->Profilers;
    if (HasUserJob()) {
        const auto& userJobSpec = GetUserJobSpec();
        if (userJobSpec->Profilers) {
            profilers = &*userJobSpec->Profilers;
        }
    }

    auto p = RandomNumber<double>();

    for (const auto& profiler : *profilers) {
        if (p < profiler->ProfilingProbability) {
            return profiler;
        }

        p -= profiler->ProfilingProbability;
    }

    return {};
}

void TTask::BuildFeatureYson(TFluentAny fluent) const
{
    fluent.Value(ControllerFeatures_);
}

INodePtr TTask::BuildStatisticsNode() const
{
    return BuildYsonNodeFluently()
        .Do([this] (TFluentAny fluent) {
            AggregatedFinishedJobStatistics_.SerializeCustom(
                fluent.GetConsumer(),
                [] (const TAggregatedJobStatistics::TTaggedSummaries& summaries, IYsonConsumer* consumer) {
                    THashMap<EJobState, TSummary> groupedByJobState;
                    for (const auto& [tags, summary] : summaries) {
                        groupedByJobState[tags.JobState].Merge(summary);
                    }

                    BuildYsonFluently(consumer).Value(groupedByJobState);
                });
        });
}

void TTask::FinalizeFeatures()
{
    ControllerFeatures_.AddTag("authenticated_user", TaskHost_->GetAuthenticatedUser());
    ControllerFeatures_.AddTag("total_input_data_weight", GetChunkPoolOutput()->GetDataWeightCounter()->GetTotal());
    ControllerFeatures_.AddTag("total_input_row_count", GetChunkPoolOutput()->GetRowCounter()->GetTotal());
    ControllerFeatures_.AddTag("total_job_count", GetChunkPoolOutput()->GetJobCounter()->GetTotal());
    ControllerFeatures_.AddTag("task_name", GetVertexDescriptor());
    ControllerFeatures_.AddSingular("job_counter", BuildYsonNodeFluently().Value(GetJobCounter()));
    ControllerFeatures_.AddSingular("ready_time", GetReadyTime().MilliSeconds());
    ControllerFeatures_.AddSingular("wall_time", GetWallTime().MilliSeconds());
    ControllerFeatures_.AddSingular("exhaust_time", GetExhaustTime().MilliSeconds());
    ControllerFeatures_.AddSingular("job_statistics", BuildStatisticsNode());

    ControllerFeatures_.CalculateJobSatisticsAverage();
}

void TTask::OnPendingJobCountUpdated()
{
    if (!StartTime_ || CompletionTime_) {
        return;
    }
    if (GetJobCounter()->GetPending() == 0) {
        ReadyTimer_.Stop();
        ExhaustTimer_.StartIfNotActive();
    } else {
        ExhaustTimer_.Stop();
        ReadyTimer_.StartIfNotActive();
    }
}

TDuration TTask::GetWallTime() const
{
    if (!StartTime_) {
        return TDuration::Zero();
    }
    if (!CompletionTime_) {
        return TInstant::Now() - *StartTime_;
    }
    return *CompletionTime_ - *StartTime_;
}

TDuration TTask::GetReadyTime() const
{
    return ReadyTimer_.GetElapsedTime();
}

TDuration TTask::GetExhaustTime() const
{
    return ExhaustTimer_.GetElapsedTime();
}

void TTask::UpdateAggregatedFinishedJobStatistics(const TJobletPtr& joblet, const TJobSummary& jobSummary)
{
    i64 statisticsLimit = TaskHost_->GetOptions()->CustomStatisticsCountLimit;
    bool isLimitExceeded = false;

    SafeUpdateAggregatedJobStatistics(
        TaskHost_,
        AggregatedFinishedJobStatistics_,
        joblet->GetAggregationTags(jobSummary.State),
        *joblet->JobStatistics,
        *joblet->ControllerStatistics,
        statisticsLimit,
        &isLimitExceeded);

    if (isLimitExceeded) {
        TaskHost_->SetOperationAlert(EOperationAlertType::CustomStatisticsLimitExceeded,
            TError("Limit for number of custom statistics exceeded for task, so they are truncated")
                << TErrorAttribute("limit", statisticsLimit)
                << TErrorAttribute("task_name", joblet->Task->GetVertexDescriptor()));
    }
}

TDuration TTask::GetTotalDuration() const
{
    if (!StartTime_) {
        return TDuration::Zero();
    }
    return TInstant::Now() - *StartTime_;
}

TDuration TTask::GetUnavailableNetworkBandwidthDuration() const
{
    auto lastUnavailableNetworkBandwidthToClustersDuration = TDuration::Zero();
    if (UnavailableNetworkBandwidthToClustersStartTime_) {
        lastUnavailableNetworkBandwidthToClustersDuration = TInstant::Now() - UnavailableNetworkBandwidthToClustersStartTime_.value();
    }

    return UnavailableNetworkBandwidthToClustersDuration_ + lastUnavailableNetworkBandwidthToClustersDuration;
}

THashMap<TClusterName, bool> TTask::GetClusterToNetworkBandwidthAvailability() const
{
    auto guard = ReaderGuard(ClusterToNetworkBandwidthAvailabilityLock_);
    return ClusterToNetworkBandwidthAvailability_;
}

bool TTask::IsNetworkBandwidthToClustersAvailable() const
{
    auto guard = ReaderGuard(ClusterToNetworkBandwidthAvailabilityLock_);
    for (const auto& [clusterName, isAvailable] : ClusterToNetworkBandwidthAvailability_) {
        if (!isAvailable) {
            return false;
        }
    }

    return true;
}

void TTask::SubscribeToClusterNetworkBandwidthAvailabilityUpdated(const TClusterName& clusterName)
{
    YT_LOG_DEBUG("Subscribe task to remote cluster network bandwidth availability updates (ClusterName: %v)",
        clusterName);

    TaskHost_->SubscribeToClusterNetworkBandwidthAvailabilityUpdated(
        clusterName,
        ClusterToNetworkBandwidthAvailabilityUpdatedCallback_);
}

void TTask::UnsubscribeFromClusterNetworkBandwidthAvailabilityUpdated(const TClusterName& clusterName)
{
    YT_LOG_DEBUG("Unsubscribe task from remote cluster network bandwidth availability updates (ClusterName: %v)",
        clusterName);

    TaskHost_->UnsubscribeFromClusterNetworkBandwidthAvailabilityUpdated(
        clusterName,
        ClusterToNetworkBandwidthAvailabilityUpdatedCallback_);
}

void TTask::FinalizeSubscriptions()
{
    if (TaskHost_->GetSpec()->UseClusterThrottlers) {
        auto guard = WriterGuard(ClusterToNetworkBandwidthAvailabilityLock_);
        for (const auto& [clusterName, _] : ClusterToNetworkBandwidthAvailability_) {
            UnsubscribeFromClusterNetworkBandwidthAvailabilityUpdated(clusterName);
        }
        ClusterToNetworkBandwidthAvailability_.clear();
    }
}

void TTask::UpdateNetworkAndTask()
{
    // Update network bandwidth availability first.
    UpdateClusterToNetworkBandwidthAvailability();
    UpdateTask();
}

void TTask::UpdateClusterToNetworkBandwidthAvailability()
{
    auto clusterToNetworkBandwidthAvailability = TaskHost_->GetClusterToNetworkBandwidthAvailability();

    auto guard = WriterGuard(ClusterToNetworkBandwidthAvailabilityLock_);
    for (auto& [clusterName, isAvailable] : ClusterToNetworkBandwidthAvailability_) {
        if (!clusterToNetworkBandwidthAvailability) {
            isAvailable = true;
            continue;
        }

        auto it = clusterToNetworkBandwidthAvailability->find(clusterName);
        if (it != clusterToNetworkBandwidthAvailability->end()) {
            isAvailable = it->second;
            continue;
        }

        isAvailable = true;
    }
}

void TTask::UpdateClusterToNetworkBandwidthAvailability(const TClusterName& clusterName, bool isAvailable)
{
    auto guard = WriterGuard(ClusterToNetworkBandwidthAvailabilityLock_);
    UpdateClusterToNetworkBandwidthAvailabilityLocked(clusterName, isAvailable);
}

void TTask::UpdateClusterToNetworkBandwidthAvailabilityLocked(const NScheduler::TClusterName& clusterName, bool isAvailable)
{
    YT_VERIFY(ClusterToNetworkBandwidthAvailabilityLock_.IsLockedByWriter());
    ClusterToNetworkBandwidthAvailability_[clusterName] = isAvailable;
}

PHOENIX_DEFINE_TYPE(TTask);

////////////////////////////////////////////////////////////////////////////////

void TTask::TResourceOverdraftState::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, UserJobStatus);
    PHOENIX_REGISTER_FIELD(2, JobProxyStatus);
    PHOENIX_REGISTER_FIELD(3, DedicatedUserJobMemoryReserveFactor);
    PHOENIX_REGISTER_FIELD(4, DedicatedJobProxyMemoryReserveFactor);
}

PHOENIX_DEFINE_TYPE(TTask::TResourceOverdraftState);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
