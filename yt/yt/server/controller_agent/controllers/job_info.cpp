#include "job_info.h"

#include "job_helpers.h"
#include "task.h"
#include "task_host.h"

#include <yt/yt/server/controller_agent/config.h>

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

TJobNodeDescriptor::TJobNodeDescriptor(const TExecNodeDescriptorPtr& other)
    : Id(other->Id)
    , Address(other->Address)
    , IOWeight(other->IOWeight)
{ }

void TJobNodeDescriptor::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Id);
    Persist(context, Address);
    Persist(context, IOWeight);
}

////////////////////////////////////////////////////////////////////////////////

TJoblet::TJoblet(
    TTask* task,
    int jobIndex,
    int taskJobIndex,
    const TString& treeId,
    bool treeIsTentative)
    : Task(task)
    , JobIndex(jobIndex)
    , TaskJobIndex(taskJobIndex)
    , TreeId(treeId)
    , TreeIsTentative(treeIsTentative)
{ }

TJobMetrics TJoblet::UpdateJobMetrics(const TJobSummary& jobSummary, bool isJobFinished)
{
    const auto Logger = ControllerLogger.WithTag("JobId: %v", JobId);

    if (!jobSummary.Statistics) {
        // Return empty delta if job has no statistics.
        return TJobMetrics();
    }

    auto newJobMetrics = TJobMetrics::FromJobStatistics(
        *JobStatistics,
        *ControllerStatistics,
        jobSummary.TimeStatistics,
        jobSummary.State,
        Task->GetTaskHost()->GetConfig()->CustomJobMetrics,
        /*considerNonMonotonicMetrics*/ isJobFinished);

    bool monotonicityViolated = !Dominates(newJobMetrics, JobMetrics);
    if (monotonicityViolated) {
        if (!HasLoggedJobMetricsMonotonicityViolation) {
            YT_LOG_WARNING("Job metrics monotonicity violated (Previous: %v, Current: %v)",
                ConvertToYsonString(JobMetrics, EYsonFormat::Text),
                ConvertToYsonString(newJobMetrics, EYsonFormat::Text));

            HasLoggedJobMetricsMonotonicityViolation = true;
        }

        newJobMetrics = Max(newJobMetrics, JobMetrics);
        // See YT-17927.
        if (jobSummary.State == EJobState::Completed) {
            newJobMetrics.Values()[EJobMetricName::TotalTimeCompleted] = newJobMetrics.Values()[EJobMetricName::TotalTime];
        } else if (jobSummary.State == EJobState::Aborted) {
            newJobMetrics.Values()[EJobMetricName::TotalTimeAborted] = newJobMetrics.Values()[EJobMetricName::TotalTime];
        }
    }

    auto delta = newJobMetrics - JobMetrics;
    YT_VERIFY(Dominates(delta, TJobMetrics()));
    JobMetrics = std::move(newJobMetrics);

    return delta;
}

TStatistics TJoblet::BuildCombinedStatistics() const
{
    auto statistics = *JobStatistics;
    statistics.MergeWithOverride(*ControllerStatistics);
    return statistics;
}

TJobStatisticsTags TJoblet::GetAggregationTags(EJobState state)
{
    // NB: Completed restarted job is considered as lost in statistics.
    // Actually we have lost previous incarnation of this job, but it was already considered as completed in statistics.
    auto statisticsState = Restarted && state == EJobState::Completed
        ? EJobState::Lost
        : state;
    return {
        .JobState = statisticsState,
        .JobType = Task->GetVertexDescriptorForJoblet(MakeStrong(this)),
        .PoolTree = TreeId,
    };
}

bool TJoblet::ShouldLogFinishedEvent() const
{
    // We should log finished event only for started jobs.
    // But there is a situation when job has been saved to snapshot before
    // OnJobStarted, after that it has been actually started, but we failed to recognize it after revive.
    // In case of revived job we cannot definitely recognize whether job has been actually started or not, but want to log finished event.
    return Revived || IsStarted();
}

bool TJoblet::IsStarted() const noexcept
{
    return JobState.has_value();
}

bool TJoblet::IsJobStartedOnNode() const noexcept
{
    return NodeJobStartTime != TInstant();
}

void TJoblet::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, JobId);
    Persist(context, JobType);
    Persist(context, NodeDescriptor);
    Persist(context, StartTime);
    Persist(context, FinishTime);
    // COMPAT(arkady-e1ppa)
    if (context.GetVersion() >= ESnapshotVersion::NodeJobStartTimeInJoblet) {
        Persist(context, NodeJobStartTime);
    }
    // COMPAT(pogorelov)
    if (context.GetVersion() < ESnapshotVersion::JobStateInJoblet) {
        bool isStarted;
        Persist(context, isStarted);

        if (isStarted) {
            JobState = EJobState::Waiting;
        }
    } else {
        Persist(context, JobState);
    }
    Persist(context, DebugArtifactsAccount);
    Persist(context, Suspicious);
    Persist(context, LastActivityTime);
    Persist(context, BriefStatistics);
    Persist(context, Progress);
    Persist(context, StderrSize);
    // NB(max42): JobStatistics is not persisted intentionally since
    // it can increase the size of snapshot significantly.
    Persist(context, Phase);
    Persist(context, CompetitionIds);
    Persist(context, HasCompetitors);
    Persist(context, TaskName);
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
    Persist(context, UserJobMemoryReserve);
    Persist(context, PredecessorType);
    Persist(context, PredecessorJobId);
    Persist(context, ResourceLimits);
    Persist(context, ChunkListIds);
    Persist(context, StderrTableChunkListId);
    Persist(context, CoreTableChunkListId);
    Persist(context, JobMetrics);
    Persist(context, TreeId);
    Persist(context, TreeIsTentative);
    Persist(context, CompetitionType);
    Persist(context, JobSpeculationTimeout);
    Persist(context, DiskQuota);
    Persist(context, DiskRequestAccount);
    Persist(context, EnabledJobProfiler);
    Persist(context, OutputStreamDescriptors);
    Persist(context, InputStreamDescriptors);

    if (context.IsLoad()) {
        Revived = true;
    }
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
