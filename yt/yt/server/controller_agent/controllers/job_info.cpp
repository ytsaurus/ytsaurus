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
    , Addresses(other->Addresses)
    , IOWeight(other->IOWeight)
{ }

void TJobNodeDescriptor::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Id);
    PHOENIX_REGISTER_FIELD(2, Addresses);
    PHOENIX_REGISTER_FIELD(3, IOWeight);
    // COMPAT(aleksandr.gaev): index 4 is reserved for deleted field `Address`
}

PHOENIX_DEFINE_TYPE(TJobNodeDescriptor);

////////////////////////////////////////////////////////////////////////////////

void TAllocation::TLastJobInfo::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, JobId);
    PHOENIX_REGISTER_FIELD(2, CompetitionType);
}

PHOENIX_DEFINE_TYPE(TAllocation::TLastJobInfo);

void TAllocation::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Id);
    PHOENIX_REGISTER_FIELD(2, Joblet);
    PHOENIX_REGISTER_FIELD(3, NodeDescriptor);
    PHOENIX_REGISTER_FIELD(4, Resources);
    PHOENIX_REGISTER_FIELD(5, TreeId);
    PHOENIX_REGISTER_FIELD(6, PoolPath);
    PHOENIX_REGISTER_FIELD(7, Task);
    PHOENIX_REGISTER_FIELD(8, LastJobInfo);
    PHOENIX_REGISTER_FIELD(9, NewJobsForbiddenReason);
}

PHOENIX_DEFINE_TYPE(TAllocation);

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
    const auto Logger = ControllerLogger().WithTag("JobId: %v", JobId);

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

void TJoblet::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, JobId);
    PHOENIX_REGISTER_FIELD(2, JobType);
    PHOENIX_REGISTER_FIELD(3, NodeDescriptor);
    PHOENIX_REGISTER_FIELD(4, StartTime);
    PHOENIX_REGISTER_FIELD(5, FinishTime);
    PHOENIX_REGISTER_FIELD(6, NodeJobStartTime);
    PHOENIX_REGISTER_FIELD(7, WaitingForResourcesDuration);
    PHOENIX_REGISTER_FIELD(8, JobState);
    PHOENIX_REGISTER_FIELD(9, InterruptionReason);
    PHOENIX_REGISTER_FIELD(10, DebugArtifactsAccount);
    PHOENIX_REGISTER_FIELD(11, Suspicious);
    PHOENIX_REGISTER_FIELD(12, LastActivityTime);
    PHOENIX_REGISTER_FIELD(13, BriefStatistics);
    PHOENIX_REGISTER_FIELD(14, Progress);
    PHOENIX_REGISTER_FIELD(15, StderrSize);
    PHOENIX_REGISTER_FIELD(16, Phase);
    PHOENIX_REGISTER_FIELD(17, CompetitionIds);
    PHOENIX_REGISTER_FIELD(18, HasCompetitors);
    PHOENIX_REGISTER_FIELD(19, TaskName);
    PHOENIX_REGISTER_FIELD(20, Task);
    PHOENIX_REGISTER_FIELD(21, TaskJobIndex);
    PHOENIX_REGISTER_FIELD(22, JobIndex);
    PHOENIX_REGISTER_FIELD(23, StartRowIndex);
    PHOENIX_REGISTER_FIELD(24, Restarted);
    PHOENIX_REGISTER_FIELD(25, InputStripeList);
    PHOENIX_REGISTER_FIELD(26, OutputCookie);
    PHOENIX_REGISTER_FIELD(27, EstimatedResourceUsage);
    PHOENIX_REGISTER_FIELD(28, JobProxyMemoryReserveFactor);
    PHOENIX_REGISTER_FIELD(29, UserJobMemoryReserveFactor);
    PHOENIX_REGISTER_FIELD(30, UserJobMemoryReserve);
    PHOENIX_REGISTER_FIELD(31, PredecessorType);
    PHOENIX_REGISTER_FIELD(32, PredecessorJobId);
    PHOENIX_REGISTER_FIELD(33, ResourceLimits);
    PHOENIX_REGISTER_FIELD(34, ChunkListIds);
    PHOENIX_REGISTER_FIELD(35, StderrTableChunkListId);
    PHOENIX_REGISTER_FIELD(36, CoreTableChunkListId);
    PHOENIX_REGISTER_FIELD(37, JobMetrics);
    PHOENIX_REGISTER_FIELD(38, TreeId);
    PHOENIX_REGISTER_FIELD(39, TreeIsTentative);
    PHOENIX_REGISTER_FIELD(40, CompetitionType);
    PHOENIX_REGISTER_FIELD(41, JobSpeculationTimeout);
    PHOENIX_REGISTER_FIELD(42, DiskQuota);
    PHOENIX_REGISTER_FIELD(43, DiskRequestAccount);
    PHOENIX_REGISTER_FIELD(44, EnabledJobProfiler);
    PHOENIX_REGISTER_FIELD(45, OutputStreamDescriptors);
    PHOENIX_REGISTER_FIELD(46, InputStreamDescriptors);
    PHOENIX_REGISTER_FIELD(47, UserJobMonitoringDescriptor);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
       this_->Revived = true;
    });
}

PHOENIX_DEFINE_TYPE(TJoblet);

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
