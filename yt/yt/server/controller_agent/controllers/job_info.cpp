#include "job_info.h"

#include "job_helpers.h"
#include "task.h"
#include "task_host.h"

#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/server/lib/scheduler/job_metrics.h>

#include <yt/yt/core/profiling/timing.h>

#include <util/generic/cast.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;
using namespace NJobTrackerClient;
using namespace NProfiling;
using namespace NScheduler;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TJobNodeDescriptor::TJobNodeDescriptor(const TExecNodeDescriptor& other)
    : Id(other.Id)
    , Address(other.Address)
    , IOWeight(other.IOWeight)
{ }

void TJobNodeDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Id);
    Persist(context, Address);
    Persist(context, IOWeight);
}

////////////////////////////////////////////////////////////////////////////////

TJobInfo::TJobInfo(const TJobInfoBase& jobInfoBase)
    : TJobInfoBase(jobInfoBase)
{ }

////////////////////////////////////////////////////////////////////////////////

TJoblet::TJoblet()
    : JobIndex(-1)
    , StartRowIndex(-1)
    , OutputCookie(-1)
{ }

TJoblet::TJoblet(TTask* task, int jobIndex, int taskJobIndex, const TString& treeId, bool treeIsTentative)
    : Task(task)
    , JobIndex(jobIndex)
    , TaskJobIndex(taskJobIndex)
    , TreeId(treeId)
    , TreeIsTentative(treeIsTentative)
    , OutputCookie(IChunkPoolOutput::NullCookie)
{ }

TJobMetrics TJoblet::UpdateJobMetrics(const TJobSummary& jobSummary, bool isJobFinished, bool* monotonicityViolated)
{
    const auto Logger = ControllerLogger.WithTag("JobId: %v", JobId);

    // Statistics is always presented in job summary.
    // Therefore looking at StatisticsYson is the only way to check that
    // job has actual non-zero statistics received from node.
    if (!jobSummary.StatisticsYson) {
        // Return empty delta if job has no statistics.
        return TJobMetrics();
    }

    const auto newJobMetrics = TJobMetrics::FromJobStatistics(
        *jobSummary.Statistics,
        jobSummary.State,
        Task->GetTaskHost()->GetConfig()->CustomJobMetrics,
        /*considerNonMonotonicMetrics*/ isJobFinished);

    if (!(*monotonicityViolated) && !Dominates(newJobMetrics, JobMetrics)) {
        YT_LOG_WARNING("Job metrics monotonicity violated (Previous: %v, Current: %v)",
            ConvertToYsonString(JobMetrics, EYsonFormat::Text),
            ConvertToYsonString(newJobMetrics, EYsonFormat::Text));
        *monotonicityViolated = true;
    }

    auto updatedJobMetrics = Max(newJobMetrics, JobMetrics);

    auto delta = updatedJobMetrics - JobMetrics;
    YT_VERIFY(Dominates(delta, TJobMetrics()));
    JobMetrics = updatedJobMetrics;

    return delta;
}

void TJoblet::Persist(const TPersistenceContext& context)
{
    TJobInfoBase::Persist(context);

    using NYT::Persist;
    Persist(context, Task);
    Persist(context, TaskJobIndex);
    Persist(context, JobIndex);
    Persist(context, StartRowIndex);
    Persist(context, Restarted);
    Persist(context, InputStripeList);
    Persist(context, OutputCookie);
    Persist(context, EstimatedResourceUsage);
    Persist(context, JobProxyMemoryReserveFactor);
    Persist(context, UserJobMemoryReserveFactor);
    Persist(context, ResourceLimits);
    Persist(context, ChunkListIds);
    Persist(context, StderrTableChunkListId);
    Persist(context, CoreTableChunkListId);
    Persist(context, JobMetrics);
    Persist(context, TreeId);
    Persist(context, TreeIsTentative);
    Persist(context, Speculative);
    Persist(context, JobSpeculationTimeout);
    Persist(context, StreamDescriptors);
   
    if (context.IsSave() || context.GetVersion() >= ESnapshotVersion::AccountInDiskRequest) {
        Persist(context, DiskQuota);
    }

    if (context.IsLoad()) {
        Revived = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

TFinishedJobInfo::TFinishedJobInfo(
    const TJobletPtr& joblet,
    TJobSummary summary)
    : TJobInfo(TJobInfoBase(*joblet))
    , Summary(std::move(summary))
{ }

////////////////////////////////////////////////////////////////////////////////

void TJobInfoBase::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, JobId);
    Persist(context, JobType);
    Persist(context, NodeDescriptor);
    Persist(context, StartTime);
    Persist(context, FinishTime);
    Persist(context, DebugArtifactsAccount);
    Persist(context, Suspicious);
    Persist(context, LastActivityTime);
    Persist(context, BriefStatistics);
    Persist(context, Progress);
    Persist(context, StderrSize);
    // NB(max42): JobStatistics is not persisted intentionally since
    // it can increase the size of snapshot significantly.
    Persist(context, Phase);
    Persist(context, JobCompetitionId);
    Persist(context, HasCompetitors);
    Persist(context, TaskName);
}

////////////////////////////////////////////////////////////////////////////////

void TFinishedJobInfo::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Summary);
    TJobInfoBase::Persist(context);
}

////////////////////////////////////////////////////////////////////////////////

void TCompletedJob::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Suspended);
    Persist(context, UnavailableChunks);
    Persist(context, JobId);
    Persist(context, SourceTask);
    Persist(context, OutputCookie);
    Persist(context, DataWeight);
    Persist(context, DestinationPool);
    Persist(context, InputCookie);
    Persist(context, InputStripe);
    Persist(context, NodeDescriptor);
    Persist(context, Restartable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
