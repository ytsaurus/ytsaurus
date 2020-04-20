#include "task.h"

#include "chunk_list_pool.h"
#include "config.h"
#include "job_info.h"
#include "job_splitter.h"
#include "task_host.h"
#include "scheduling_context.h"

#include <yt/server/controller_agent/job_memory.h>

#include <yt/server/lib/chunk_pools/helpers.h>

#include <yt/server/lib/scheduler/config.h>

#include <yt/ytlib/chunk_client/chunk_slice.h>

#include <yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT::NControllerAgent {

using namespace NChunkClient;
using namespace NChunkPools;
using namespace NJobTrackerClient;
using namespace NJobTrackerClient::NProto;
using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NTableClient;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;
using NScheduler::NProto::TSchedulerJobSpecExt;
using NScheduler::NProto::TSchedulerJobResultExt;
using NScheduler::NProto::TTableInputSpec;

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

TTask::TTask()
    : Logger(ControllerLogger)
    , CachedPendingJobCount_(-1)
    , CachedTotalJobCount_(-1)
    , CompletedFired_(false)
    , CompetitiveJobManager_(
        std::bind(&TTask::OnSpeculativeJobScheduled, this, _1),
        std::bind(&TTask::AbortJobViaScheduler, this, _1, _2),
        Logger,
        0)
{ }

TTask::TTask(ITaskHostPtr taskHost, std::vector<TEdgeDescriptor> edgeDescriptors)
    : Logger(ControllerLogger)
    , TaskHost_(taskHost.Get())
    , EdgeDescriptors_(std::move(edgeDescriptors))
    , TentativeTreeEligibility_(taskHost->GetSpec()->TentativeTreeEligibility)
    , CachedPendingJobCount_(0)
    , CachedTotalJobCount_(0)
    , CompletedFired_(false)
    , InputChunkMapping_(New<TInputChunkMapping>())
    , CompetitiveJobManager_(
        std::bind(&TTask::OnSpeculativeJobScheduled, this, _1),
        std::bind(&TTask::AbortJobViaScheduler, this, _1, _2),
        Logger,
        taskHost->GetSpec()->MaxSpeculativeJobCountPerTask)
{ }

TTask::TTask(ITaskHostPtr taskHost)
    : TTask(taskHost, taskHost->GetStandardEdgeDescriptors())
{ }

void TTask::Initialize()
{
    auto operationId = TaskHost_->GetOperationId();
    auto taskTitle = GetTitle();

    Logger.AddTag("OperationId: %v", operationId);
    Logger.AddTag("Task: %v", taskTitle);

    TentativeTreeEligibility_.Initialize(operationId, taskTitle);

    SetupCallbacks();

    if (IsSimpleTask()) {
        if (auto userJobSpec = GetUserJobSpec()) {
            MaximumUsedTmpfsSizes_.resize(userJobSpec->TmpfsVolumes.size());
        }
    }
}

TString TTask::GetTitle() const
{
    return ToString(GetJobType());
}

TDataFlowGraph::TVertexDescriptor TTask::GetVertexDescriptor() const
{
    return FormatEnum(GetJobType());
}

int TTask::GetPendingJobCount() const
{
    return GetChunkPoolOutput()->GetPendingJobCount() + CompetitiveJobManager_.GetPendingSpeculativeJobCount();
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
    return GetChunkPoolOutput()->GetTotalJobCount() + CompetitiveJobManager_.GetTotalSpeculativeJobCount();
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
    return count == 0 ? TJobResources() : GetMinNeededResources().ToJobResources() * count;
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
    YT_LOG_DEBUG("Task input finished");

    // GetChunkPoolInput() may return nullptr on tasks that do not require input, such as for vanilla operation.
    if (const auto& chunkPoolInput = GetChunkPoolInput()) {
        chunkPoolInput->Finish();
    }
    auto progressCounter = GetChunkPoolOutput()->GetJobCounter();
    if (!progressCounter->Parent()) {
        TaskHost_->GetDataFlowGraph()
            ->RegisterCounter(GetVertexDescriptor(), progressCounter, GetJobType());
    }

    TaskHost_->GetDataFlowGraph()->RegisterCounter(
        GetVertexDescriptor(),
        CompetitiveJobManager_.GetProgressCounter(),
        GetJobType());
    AddPendingHint();
    CheckCompleted();
}

void TTask::FinishInput(TDataFlowGraph::TVertexDescriptor inputVertex)
{
    SetInputVertex(inputVertex);

    FinishInput();
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

ITaskHost* TTask::GetTaskHost()
{
    return TaskHost_;
}

bool TTask::ValidateChunkCount(int /* chunkCount */)
{
    return true;
}

void TTask::ScheduleJob(
    ISchedulingContext* context,
    const TJobResourcesWithQuota& jobLimits,
    const TString& treeId,
    bool treeIsTentative,
    TControllerScheduleJobResult* scheduleJobResult)
{
    if (auto failReason = GetScheduleFailReason(context)) {
        scheduleJobResult->RecordFail(*failReason);
        return;
    }

    if (treeIsTentative && !TentativeTreeEligibility_.CanScheduleJob(treeId, treeIsTentative)) {
        scheduleJobResult->RecordFail(EScheduleJobFailReason::TentativeTreeDeclined);
        return;
    }

    auto* chunkPoolOutput = GetChunkPoolOutput();
    bool speculative = chunkPoolOutput->GetPendingJobCount() == 0;
    if (speculative && treeIsTentative) {
        scheduleJobResult->RecordFail(EScheduleJobFailReason::TentativeSpeculativeForbidden);
        return;
    }

    int jobIndex = TaskHost_->NextJobIndex();
    int taskJobIndex = TaskJobIndexGenerator_.Next();
    auto joblet = New<TJoblet>(this, jobIndex, taskJobIndex, treeId, treeIsTentative);
    joblet->StartTime = TInstant::Now();

    const auto& nodeResourceLimits = context->ResourceLimits();
    auto nodeId = context->GetNodeDescriptor().Id;
    const auto& address = context->GetNodeDescriptor().Address;

    if (speculative) {
        joblet->Speculative = true;
        joblet->OutputCookie = CompetitiveJobManager_.PeekSpeculativeCandidate();
    } else {
        auto localityNodeId = HasInputLocality() ? nodeId : InvalidNodeId;
        joblet->OutputCookie = chunkPoolOutput->Extract(localityNodeId);
        if (joblet->OutputCookie == IChunkPoolOutput::NullCookie) {
            YT_LOG_DEBUG("Job input is empty");
            scheduleJobResult->RecordFail(EScheduleJobFailReason::EmptyInput);
            return;
        }
    }

    auto abortJob = [&] (EScheduleJobFailReason jobFailReason, EAbortReason abortReason) {
        if (!joblet->Speculative) {
            chunkPoolOutput->Aborted(joblet->OutputCookie, abortReason);
        }
        scheduleJobResult->RecordFail(jobFailReason);
    };

    int sliceCount = chunkPoolOutput->GetStripeListSliceCount(joblet->OutputCookie);

    if (!ValidateChunkCount(sliceCount)) {
        abortJob(EScheduleJobFailReason::IntermediateChunkLimitExceeded, EAbortReason::IntermediateChunkLimitExceeded);
        return;
    }

    const auto& jobSpecSliceThrottler = TaskHost_->GetJobSpecSliceThrottler();
    if (sliceCount > TaskHost_->GetConfig()->HeavyJobSpecSliceCountThreshold) {
        if (!jobSpecSliceThrottler->TryAcquire(sliceCount)) {
            YT_LOG_DEBUG("Job spec throttling is active (SliceCount: %v)",
                sliceCount);
            abortJob(EScheduleJobFailReason::JobSpecThrottling, EAbortReason::SchedulingJobSpecThrottling);
            return;
        }
    } else {
        jobSpecSliceThrottler->Acquire(sliceCount);
    }

    joblet->InputStripeList = chunkPoolOutput->GetStripeList(joblet->OutputCookie);

    auto estimatedResourceUsage = GetNeededResources(joblet);
    TJobResourcesWithQuota neededResources = ApplyMemoryReserve(estimatedResourceUsage);

    joblet->EstimatedResourceUsage = estimatedResourceUsage;
    joblet->ResourceLimits = neededResources.ToJobResources();
    if (auto userJobSpec = GetUserJobSpec()) {
        if (userJobSpec->DiskRequest) {
            neededResources.SetDiskQuota(CreateDiskQuota(userJobSpec->DiskRequest, TaskHost_->GetMediumDirectory()));
        }
    }

    // Check the usage against the limits. This is the last chance to give up.
    if (!Dominates(jobLimits, neededResources)) {
        YT_LOG_DEBUG("Job actual resource demand is not met (Limits: %v, Demand: %v)",
            FormatResources(jobLimits, TaskHost_->GetMediumDirectory()),
            FormatResources(neededResources, TaskHost_->GetMediumDirectory()));
        CheckResourceDemandSanity(nodeResourceLimits, neededResources);
        abortJob(EScheduleJobFailReason::NotEnoughResources, EAbortReason::SchedulingOther);
        // Seems like cached min needed resources are too optimistic.
        ResetCachedMinNeededResources();
        return;
    }

    joblet->JobId = context->GetJobId();

    CompetitiveJobManager_.OnJobScheduled(joblet);

    // Job is restarted if LostJobCookieMap contains at least one entry with this output cookie.
    auto it = LostJobCookieMap.lower_bound(TCookieAndPool(joblet->OutputCookie, nullptr));
    bool restarted = it != LostJobCookieMap.end() && it->first.first == joblet->OutputCookie;

    joblet->Account = TaskHost_->GetSpec()->JobNodeAccount;
    joblet->JobSpecProtoFuture = BIND([weakTaskHost = MakeWeak(TaskHost_), joblet] {
        if (auto taskHost = weakTaskHost.Lock()) {
            return taskHost->BuildJobSpecProto(joblet);
        } else {
            THROW_ERROR_EXCEPTION("Operation controller was destroyed");
        }
    })
        .AsyncVia(TaskHost_->GetCancelableInvoker(TaskHost_->GetConfig()->BuildJobSpecControllerQueue))
        .Run();

    auto jobType = GetJobType();
    scheduleJobResult->StartDescriptor.emplace(
        joblet->JobId,
        jobType,
        neededResources,
        IsJobInterruptible());

    joblet->Restarted = restarted;
    joblet->JobType = jobType;
    joblet->NodeDescriptor = context->GetNodeDescriptor();
    joblet->JobProxyMemoryReserveFactor = GetJobProxyMemoryDigest()->GetQuantile(
        TaskHost_->GetConfig()->JobProxyMemoryReserveQuantile);
    auto userJobSpec = GetUserJobSpec();
    if (userJobSpec) {
        joblet->UserJobMemoryReserveFactor = GetUserJobMemoryDigest()->GetQuantile(
            TaskHost_->GetConfig()->UserJobMemoryReserveQuantile);
    }

    if (userJobSpec && userJobSpec->JobSpeculationTimeout) {
        joblet->JobSpeculationTimeout = userJobSpec->JobSpeculationTimeout;
    } else if (TaskHost_->GetSpec()->JobSpeculationTimeout) {
        joblet->JobSpeculationTimeout = TaskHost_->GetSpec()->JobSpeculationTimeout;
    }

    YT_LOG_DEBUG(
        "Job scheduled (JobId: %v, OperationId: %v, JobType: %v, Address: %v, JobIndex: %v, OutputCookie: %v, SliceCount: %v (%v local), "
        "Approximate: %v, DataWeight: %v (%v local), RowCount: %v, Splittable: %v, Restarted: %v, EstimatedResourceUsage: %v, JobProxyMemoryReserveFactor: %v, "
        "UserJobMemoryReserveFactor: %v, ResourceLimits: %v, Speculative: %v, JobSpeculationTimeout: %v)",
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
        joblet->InputStripeList->IsSplittable,
        restarted,
        FormatResources(estimatedResourceUsage),
        joblet->JobProxyMemoryReserveFactor,
        joblet->UserJobMemoryReserveFactor,
        FormatResources(neededResources, TaskHost_->GetMediumDirectory()),
        joblet->Speculative,
        joblet->JobSpeculationTimeout);

    for (const auto& edgeDescriptor : EdgeDescriptors_) {
        joblet->ChunkListIds.push_back(TaskHost_->ExtractOutputChunkList(edgeDescriptor.CellTag));
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
    if (!joblet->Speculative) {
        TaskHost_->AddValueToEstimatedHistogram(joblet);
    }

    OnJobStarted(joblet);

    if (TaskHost_->GetJobSplitter()) {
        TaskHost_->GetJobSplitter()->OnJobStarted(joblet->JobId, joblet->InputStripeList, IsJobInterruptible());
    }
}

bool TTask::TryRegisterSpeculativeJob(const TJobletPtr& joblet)
{
    return CompetitiveJobManager_.TryRegisterSpeculativeCandidate(joblet);
}

bool TTask::IsJobInterruptible() const
{
    return TaskHost_->IsJobInterruptible();
}

std::optional<EAbortReason> TTask::ShouldAbortJob(const TJobletPtr& joblet)
{
    return CompetitiveJobManager_.ShouldAbortJob(joblet);
}

bool TTask::IsCompleted() const
{
    return IsActive() && GetChunkPoolOutput()->IsCompleted() && CompetitiveJobManager_.IsFinished();
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
    return GetChunkPoolOutput()->GetPendingDataWeight() + CompetitiveJobManager_.GetPendingCandidatesDataWeight();
}

i64 TTask::GetInputDataSliceCount() const
{
    return GetChunkPoolOutput()->GetDataSliceCount();
}

void TTask::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, TaskHost_);

    Persist(context, CachedPendingJobCount_);
    Persist(context, CachedTotalJobCount_);

    Persist(context, CachedTotalNeededResources_);
    Persist(context, CachedMinNeededResources_);

    Persist(context, CompletedFired_);

    Persist<
        TMapSerializer<
            TTupleSerializer<TCookieAndPool, 2>,
            TDefaultSerializer,
            TUnsortedTag
        >
    >(context, LostJobCookieMap);

    Persist(context, EdgeDescriptors_);
    Persist(context, InputVertex_);

    Persist(context, TentativeTreeEligibility_);

    Persist(context, UserJobMemoryDigest_);
    Persist(context, JobProxyMemoryDigest_);

    Persist(context, InputChunkMapping_);

    Persist(context, TaskJobIndexGenerator_);

    Persist(context, CompetitiveJobManager_);
}

void TTask::OnJobStarted(TJobletPtr joblet)
{
    TentativeTreeEligibility_.OnJobStarted(joblet->TreeId, joblet->TreeIsTentative);
}

bool TTask::CanLoseJobs() const
{
    return false;
}

bool TTask::IsSimpleTask() const
{
    return true;
}

TJobFinishedResult TTask::OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary)
{
    auto result = TentativeTreeEligibility_.OnJobFinished(jobSummary, joblet->TreeId, joblet->TreeIsTentative);

    CompetitiveJobManager_.OnJobCompleted(joblet);

    YT_VERIFY(jobSummary.Statistics);
    const auto& statistics = *jobSummary.Statistics;

    if (!jobSummary.Abandoned) {
        auto outputStatisticsMap = GetOutputDataStatistics(statistics);
        for (int index = 0; index < static_cast<int>(joblet->ChunkListIds.size()); ++index) {
            YT_VERIFY(outputStatisticsMap.find(index) != outputStatisticsMap.end());
            auto outputStatistics = outputStatisticsMap[index];
            if (outputStatistics.chunk_count() == 0) {
                if (!joblet->Revived) {
                    TaskHost_->GetOutputChunkListPool()->Reinstall(joblet->ChunkListIds[index]);
                }
                joblet->ChunkListIds[index] = NullChunkListId;
            }
            if (joblet->ChunkListIds[index] && EdgeDescriptors_[index].ImmediatelyUnstageChunkLists) {
                this->TaskHost_->ReleaseChunkTrees({joblet->ChunkListIds[index]}, false /* unstageRecursively */);
                joblet->ChunkListIds[index] = NullChunkListId;
            }
        }

        auto inputStatistics = GetTotalInputDataStatistics(statistics);
        auto outputStatistics = GetTotalOutputDataStatistics(statistics);
        // It's impossible to check row count preservation on interrupted job.
        if (TaskHost_->IsRowCountPreserved() && jobSummary.InterruptReason == EInterruptReason::None) {
            YT_LOG_ERROR_IF(inputStatistics.row_count() != outputStatistics.row_count(),
                "Input/output row count mismatch in completed job (Input: %v, Output: %v, Task: %v)",
                inputStatistics.row_count(),
                outputStatistics.row_count(),
                GetTitle());
            YT_VERIFY(inputStatistics.row_count() == outputStatistics.row_count());
        }

        YT_VERIFY(InputVertex_ != "");

        auto vertex = GetVertexDescriptor();
        TaskHost_->GetDataFlowGraph()->UpdateEdgeStatistics(InputVertex_, vertex, inputStatistics);
        // TODO(max42): rewrite this properly one day.
        for (int index = 0; index < EdgeDescriptors_.size(); ++index) {
            if (EdgeDescriptors_[index].IsFinalOutput) {
                TaskHost_->GetDataFlowGraph()->UpdateEdgeStatistics(
                    vertex,
                    TDataFlowGraph::SinkDescriptor,
                    outputStatisticsMap[index]);
            }
        }
    } else {
        auto& chunkListIds = joblet->ChunkListIds;
        // NB: we should release these chunk lists only when information about this job being abandoned
        // gets to the snapshot; otherwise it may revive in different scheduler and continue writing
        // to the released chunk list.
        TaskHost_->ReleaseChunkTrees(chunkListIds, true /* recursive */, true /* waitForSnapshot */);
        std::fill(chunkListIds.begin(), chunkListIds.end(), NullChunkListId);
    }
    GetChunkPoolOutput()->Completed(joblet->OutputCookie, jobSummary);

    TaskHost_->RegisterStderr(joblet, jobSummary);
    TaskHost_->RegisterCores(joblet, jobSummary);

    UpdateMaximumUsedTmpfsSizes(statistics);

    return result;
}

void TTask::ReinstallJob(TJobletPtr joblet, std::function<void()> releaseOutputCookie)
{
    releaseOutputCookie();

    if (HasInputLocality()) {
        for (const auto& stripe : joblet->InputStripeList->Stripes) {
            TaskHost_->AddTaskLocalityHint(stripe, this);
        }
    }
    AddPendingHint();
}

void TTask::ReleaseJobletResources(TJobletPtr joblet, bool waitForSnapshot)
{
    TaskHost_->RemoveValueFromEstimatedHistogram(joblet);
    TaskHost_->ReleaseChunkTrees(joblet->ChunkListIds, /* recursive */ true, waitForSnapshot);
}

TJobFinishedResult TTask::OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary)
{
    auto result = TentativeTreeEligibility_.OnJobFinished(jobSummary, joblet->TreeId, joblet->TreeIsTentative);

    TaskHost_->RegisterStderr(joblet, jobSummary);
    TaskHost_->RegisterCores(joblet, jobSummary);

    YT_VERIFY(jobSummary.Statistics);
    UpdateMaximumUsedTmpfsSizes(*jobSummary.Statistics);

    ReleaseJobletResources(joblet, /* waitForSnapshot */ false);
    bool returnCookie = CompetitiveJobManager_.OnJobFailed(joblet);
    if (returnCookie) {
        ReinstallJob(joblet, BIND([=] {GetChunkPoolOutput()->Failed(joblet->OutputCookie);}));
    }

    return result;
}

TJobFinishedResult TTask::OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary)
{
    auto result = TentativeTreeEligibility_.OnJobFinished(jobSummary, joblet->TreeId, joblet->TreeIsTentative);

    // NB: when job is aborted, you can never be sure that this is forever. Like in marriage. In future life (after
    // revival) it may become completed, and you will bite your elbows if you unstage its chunk lists too early (e.g.
    // job is aborted due to node gone offline, but after revival it happily comes back and job successfully completes).
    // So better keep it simple and wait for the snapshot.

    if (joblet->StderrTableChunkListId) {
        TaskHost_->ReleaseChunkTrees({joblet->StderrTableChunkListId}, true /* unstageRecursively */, true /* waitForSnapshot */);
    }
    if (joblet->CoreTableChunkListId) {
        TaskHost_->ReleaseChunkTrees({joblet->CoreTableChunkListId}, true /* unstageRecursively */, true /* waitForSnapshot */);
    }

    ReleaseJobletResources(joblet, /* waitForSnapshot */ true);

    bool returnCookie = CompetitiveJobManager_.OnJobAborted(joblet, jobSummary.AbortReason);
    if (returnCookie) {
        ReinstallJob(joblet, BIND([=] { GetChunkPoolOutput()->Aborted(joblet->OutputCookie, jobSummary.AbortReason); }));
    }

    return result;
}

void TTask::OnJobLost(TCompletedJobPtr completedJob)
{
    YT_VERIFY(LostJobCookieMap.insert(std::make_pair(
        TCookieAndPool(completedJob->OutputCookie, completedJob->DestinationPool),
        completedJob->InputCookie)).second);
}

void TTask::OnStripeRegistrationFailed(
    TError error,
    IChunkPoolInput::TCookie /* cookie */,
    const TChunkStripePtr& /* stripe */,
    const TEdgeDescriptor& /* edgeDescriptor */)
{
    TaskHost_->OnOperationFailed(error
        << TErrorAttribute("task_title", GetTitle()));
}

void TTask::OnTaskCompleted()
{
    YT_VERIFY(CompetitiveJobManager_.GetProgressCounter()->GetTotal() == 0);
    YT_LOG_DEBUG("Task completed");
}

std::optional<EScheduleJobFailReason> TTask::GetScheduleFailReason(ISchedulingContext* context)
{
    return std::nullopt;
}

void TTask::DoCheckResourceDemandSanity(
    const TJobResourcesWithQuota& neededResources)
{
    if (TaskHost_->ShouldSkipSanityCheck()) {
        return;
    }

    if (!Dominates(*TaskHost_->CachedMaxAvailableExecNodeResources(), neededResources.ToJobResources())) {
        // It seems nobody can satisfy the demand.
        TaskHost_->OnOperationFailed(
            TError(
                EErrorCode::NoOnlineNodeToScheduleJob,
                "No online node can satisfy the resource demand")
                << TErrorAttribute("task_name", GetTitle())
                << TErrorAttribute("needed_resources", neededResources.ToJobResources()));
    }
}

void TTask::CheckResourceDemandSanity(
    const TJobResourcesWithQuota& nodeResourceLimits,
    const TJobResourcesWithQuota& neededResources)
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

    return UserJobMemoryDigest_.get();
}

IDigest* TTask::GetJobProxyMemoryDigest() const
{
    if (!JobProxyMemoryDigest_) {
        JobProxyMemoryDigest_ = CreateLogDigest(TaskHost_->GetSpec()->JobProxyMemoryDigest);
    }

    return JobProxyMemoryDigest_.get();
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
        AddChunksToInputSpec(directoryBuilder.get(), inputSpec, GetChunkMapping()->GetMappedStripe(stripe));
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
        AddChunksToInputSpec(directoryBuilder.get(), inputSpec, GetChunkMapping()->GetMappedStripe(stripe));
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

void TTask::AddOutputTableSpecs(
    TJobSpec* jobSpec,
    TJobletPtr joblet)
{
    YT_VERIFY(joblet->ChunkListIds.size() == EdgeDescriptors_.size());
    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    for (int index = 0; index < EdgeDescriptors_.size(); ++index) {
        const auto& edgeDescriptor = EdgeDescriptors_[index];
        auto* outputSpec = schedulerJobSpecExt->add_output_table_specs();
        outputSpec->set_table_writer_options(ConvertToYsonString(edgeDescriptor.TableWriterOptions).GetData());
        if (edgeDescriptor.TableWriterConfig) {
            outputSpec->set_table_writer_config(edgeDescriptor.TableWriterConfig.GetData());
        }
        ToProto(outputSpec->mutable_table_schema(), edgeDescriptor.TableUploadOptions.TableSchema);
        ToProto(outputSpec->mutable_chunk_list_id(), joblet->ChunkListIds[index]);
        if (edgeDescriptor.Timestamp) {
            outputSpec->set_timestamp(*edgeDescriptor.Timestamp);
        }
        outputSpec->set_dynamic(edgeDescriptor.IsOutputTableDynamic);
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

TJobResources TTask::ApplyMemoryReserve(const TExtendedJobResources& jobResources) const
{
    TJobResources result;
    result.SetCpu(jobResources.GetCpu());
    result.SetGpu(jobResources.GetGpu());
    result.SetUserSlots(jobResources.GetUserSlots());
    i64 memory = jobResources.GetFootprintMemory();
    memory += jobResources.GetJobProxyMemory() * GetJobProxyMemoryDigest()
        ->GetQuantile(TaskHost_->GetConfig()->JobProxyMemoryReserveQuantile);
    if (GetUserJobSpec()) {
        memory += jobResources.GetUserJobMemory() * GetUserJobMemoryDigest()
            ->GetQuantile(TaskHost_->GetConfig()->UserJobMemoryReserveQuantile);
    } else {
        YT_VERIFY(jobResources.GetUserJobMemory() == 0);
    }
    result.SetMemory(memory);
    result.SetNetwork(jobResources.GetNetwork());
    return result;
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

    for (int index = 0; index < userJobSpec->TmpfsVolumes.size(); ++index) {
        auto maxUsedTmpfsSize = FindNumericValue(
            statistics,
            Format("/user_job/tmpfs_volumes/%v/max_size", index));
        if (!maxUsedTmpfsSize) {
            continue;
        }

        auto& maxTmpfsSize = MaximumUsedTmpfsSizes_[index];
        if (!maxTmpfsSize || *maxTmpfsSize < *maxUsedTmpfsSize) {
            maxTmpfsSize = *maxUsedTmpfsSize;
        }
    }
}

void TTask::FinishTaskInput(const TTaskPtr& task)
{
    task->FinishInput(GetVertexDescriptor() /* inputVertex */);
}

TSharedRef TTask::BuildJobSpecProto(TJobletPtr joblet)
{
    auto jobSpec = ObjectPool<NJobTrackerClient::NProto::TJobSpec>().Allocate();

    BuildJobSpec(joblet, jobSpec.get());
    jobSpec->set_version(GetJobSpecVersion());
    TaskHost_->CustomizeJobSpec(joblet, jobSpec.get());

    auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    if (TaskHost_->GetSpec()->JobProxyMemoryOvercommitLimit) {
        schedulerJobSpecExt->set_job_proxy_memory_overcommit_limit(*TaskHost_->GetSpec()->JobProxyMemoryOvercommitLimit);
    }
    schedulerJobSpecExt->set_job_proxy_ref_counted_tracker_log_period(ToProto<i64>(TaskHost_->GetSpec()->JobProxyRefCountedTrackerLogPeriod));
    schedulerJobSpecExt->set_abort_job_if_account_limit_exceeded(TaskHost_->GetSpec()->SuspendOperationIfAccountLimitExceeded);

    if (TaskHost_->GetSpec()->WaitingJobTimeout) {
        schedulerJobSpecExt->set_waiting_job_timeout(ToProto<i64>(*TaskHost_->GetSpec()->WaitingJobTimeout));
    }

    // Adjust sizes if approximation flag is set.
    if (joblet->InputStripeList->IsApproximate) {
        schedulerJobSpecExt->set_input_data_weight(static_cast<i64>(
            schedulerJobSpecExt->input_data_weight() *
            ApproximateSizesBoostFactor));
        schedulerJobSpecExt->set_input_row_count(static_cast<i64>(
            schedulerJobSpecExt->input_row_count() *
            ApproximateSizesBoostFactor));
    }

    schedulerJobSpecExt->set_job_cpu_monitor_config(ConvertToYsonString(TaskHost_->GetSpec()->JobCpuMonitor).GetData());

    if (schedulerJobSpecExt->input_data_weight() > TaskHost_->GetSpec()->MaxDataWeightPerJob) {
        TaskHost_->OnOperationFailed(TError(
            NChunkPools::EErrorCode::MaxDataWeightPerJobExceeded,
            "Maximum allowed data weight per job exceeds the limit: %v > %v",
            schedulerJobSpecExt->input_data_weight(),
            TaskHost_->GetSpec()->MaxDataWeightPerJob));
    }

    YT_VERIFY(joblet->JobCompetitionId);
    ToProto(schedulerJobSpecExt->mutable_job_competition_id(), joblet->JobCompetitionId);

    return SerializeProtoToRefWithEnvelope(*jobSpec, TaskHost_->GetConfig()->JobSpecCodec);
}

void TTask::AddFootprintAndUserJobResources(TExtendedJobResources& jobResources) const
{
    jobResources.SetFootprintMemory(GetFootprintMemorySize());
    auto userJobSpec = GetUserJobSpec();
    if (userJobSpec) {
        jobResources.SetUserJobMemory(userJobSpec->MemoryLimit);
        jobResources.SetGpu(userJobSpec->GpuLimit);
    }
}

void TTask::RegisterOutput(
    NJobTrackerClient::NProto::TJobResult* jobResult,
    const std::vector<NChunkClient::TChunkListId>& chunkListIds,
    TJobletPtr joblet,
    const NChunkPools::TChunkStripeKey& key)
{
    auto* schedulerJobResultExt = jobResult->MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
    auto outputStripes = BuildOutputChunkStripes(
        schedulerJobResultExt,
        chunkListIds,
        schedulerJobResultExt->output_boundary_keys());
    for (int tableIndex = 0; tableIndex < EdgeDescriptors_.size(); ++tableIndex) {
        if (outputStripes[tableIndex]) {
            for (const auto& dataSlice : outputStripes[tableIndex]->DataSlices) {
                TaskHost_->RegisterLivePreviewChunk(
                    GetVertexDescriptor(),
                    EdgeDescriptors_[tableIndex].LivePreviewIndex,
                    dataSlice->GetSingleUnversionedChunkOrThrow());
            }

            RegisterStripe(
                std::move(outputStripes[tableIndex]),
                EdgeDescriptors_[tableIndex],
                joblet,
                key);
        }
    }
}

TJobResourcesWithQuota TTask::GetMinNeededResources() const
{
    if (!CachedMinNeededResources_) {
        YT_VERIFY(GetPendingJobCount() > 0);
        CachedMinNeededResources_ = GetMinNeededResourcesHeavy();
    }
    auto result = ApplyMemoryReserve(*CachedMinNeededResources_);
    if (result.GetUserSlots() > 0 && result.GetMemory() == 0) {
        YT_LOG_WARNING("Found min needed resources of task with non-zero user slots and zero memory");
    }
    auto resultWithQuota = TJobResourcesWithQuota(result);
    if (auto userJobSpec = GetUserJobSpec()) {
        if (userJobSpec->DiskRequest) {
            resultWithQuota.SetDiskQuota(CreateDiskQuota(userJobSpec->DiskRequest, TaskHost_->GetMediumDirectory()));
        }
    }
    return resultWithQuota;
}

void TTask::RegisterStripe(
    TChunkStripePtr stripe,
    const TEdgeDescriptor& edgeDescriptor,
    TJobletPtr joblet,
    TChunkStripeKey key)
{
    if (stripe->DataSlices.empty() && !stripe->ChunkListId) {
        return;
    }

    auto* destinationPool = edgeDescriptor.DestinationPool;
    if (edgeDescriptor.RequiresRecoveryInfo) {
        YT_VERIFY(joblet);

        const auto& chunkMapping = edgeDescriptor.ChunkMapping;
        YT_VERIFY(chunkMapping);

        YT_LOG_DEBUG("Registering stripe in a direction that requires recovery info (JobId: %v, Restarted: %v, JobType: %v)",
            joblet->JobId,
            joblet->Restarted,
            joblet->JobType);

        IChunkPoolInput::TCookie inputCookie = IChunkPoolInput::NullCookie;
        auto lostIt = LostJobCookieMap.find(TCookieAndPool(joblet->OutputCookie, edgeDescriptor.DestinationPool));
        if (lostIt == LostJobCookieMap.end()) {
            // NB: if job is not restarted, we should not add its output for the
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
                OnStripeRegistrationFailed(error, lostIt->second, stripe, edgeDescriptor);
            }

            destinationPool->Resume(inputCookie);

            LostJobCookieMap.erase(lostIt);
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
        destinationPool->AddWithKey(stripe, key);
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
        auto dataSlice = CreateUnversionedInputDataSlice(std::move(chunkSlice));
        // NB(max42): This heavily relies on the property of intermediate data being deterministic
        // (i.e. it may be reproduced with exactly the same content divided into chunks with exactly
        // the same boundary keys when the job output is lost).
        dataSlice->Tag = index;
        int tableIndex = inputChunk->GetTableIndex();
        YT_VERIFY(tableIndex >= 0);
        YT_VERIFY(tableIndex < tableCount);
        stripes[tableIndex]->DataSlices.emplace_back(std::move(dataSlice));
    }
    return stripes;
}

TChunkStripePtr TTask::BuildIntermediateChunkStripe(
    google::protobuf::RepeatedPtrField<NChunkClient::NProto::TChunkSpec>* chunkSpecs)
{
    auto stripes = BuildChunkStripes(chunkSpecs, 1 /* tableCount */);
    return std::move(stripes[0]);
}

std::vector<TChunkStripePtr> TTask::BuildOutputChunkStripes(
    TSchedulerJobResultExt* schedulerJobResultExt,
    const std::vector<NChunkClient::TChunkTreeId>& chunkTreeIds,
    google::protobuf::RepeatedPtrField<NScheduler::NProto::TOutputResult> boundaryKeysPerTable)
{
    auto stripes = BuildChunkStripes(schedulerJobResultExt->mutable_output_chunk_specs(), chunkTreeIds.size());
    // Some edge descriptors do not require boundary keys to be returned,
    // so they are skipped in `boundaryKeysPerTable`.
    int boundaryKeysIndex = 0;
    for (int tableIndex = 0; tableIndex < chunkTreeIds.size(); ++tableIndex) {
        stripes[tableIndex]->ChunkListId = chunkTreeIds[tableIndex];
        if (EdgeDescriptors_[tableIndex].TableWriterOptions->ReturnBoundaryKeys) {
            // TODO(max42): do not send empty or unsorted boundary keys, this is meaningless.
            if (boundaryKeysIndex < boundaryKeysPerTable.size() &&
                !boundaryKeysPerTable.Get(boundaryKeysIndex).empty() &&
                boundaryKeysPerTable.Get(boundaryKeysIndex).sorted())
            {
                stripes[tableIndex]->BoundaryKeys = BuildBoundaryKeysFromOutputResult(
                    boundaryKeysPerTable.Get(boundaryKeysIndex),
                    EdgeDescriptors_[tableIndex],
                    TaskHost_->GetRowBuffer());
            }
            ++boundaryKeysIndex;
        }
    }
    return stripes;
}

void TTask::SetupCallbacks()
{ }

std::vector<TString> TTask::FindAndBanSlowTentativeTrees()
{
    return TentativeTreeEligibility_.FindAndBanSlowTentativeTrees();
}

void TTask::LogTentativeTreeStatistics() const
{
    return TentativeTreeEligibility_.LogTentativeTreeStatistics();
}

void TTask::AbortJobViaScheduler(TJobId jobId, EAbortReason reason)
{
    GetTaskHost()->AbortJobViaScheduler(jobId, reason);
}

void TTask::OnSpeculativeJobScheduled(const TJobletPtr& joblet)
{
    GetTaskHost()->OnSpeculativeJobScheduled(joblet);
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

} // namespace NYT::NControllerAgent
