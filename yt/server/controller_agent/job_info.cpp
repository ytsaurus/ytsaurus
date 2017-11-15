#include "job_info.h"

#include "job_helpers.h"
#include "job_metrics_updater.h"
#include "task_host.h"

#include <yt/server/chunk_pools/chunk_pool.h>

#include <yt/server/scheduler/job_metrics.h>

#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NControllerAgent {

using namespace NChunkPools;
using namespace NJobTrackerClient;
using namespace NProfiling;
using namespace NScheduler;

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

TJoblet::TJoblet(std::unique_ptr<TJobMetricsUpdater> jobMetricsUpdater, TTask* task, int jobIndex)
    : Task(std::move(task))
    , JobIndex(jobIndex)
    , StartRowIndex(-1)
    , OutputCookie(IChunkPoolOutput::NullCookie)
    , JobMetricsUpdater_(std::move(jobMetricsUpdater))
{ }

void TJoblet::SendJobMetrics(const NScheduler::TJobSummary& jobSummary, bool flush)
{
    YCHECK(JobMetricsUpdater_);
    const auto timestamp = jobSummary.Statistics->GetTimestamp().Get(GetInstant());
    const auto jobMetrics = TJobMetrics::FromJobTrackerStatistics(
        *jobSummary.Statistics,
        jobSummary.State);

    JobMetricsUpdater_->Update(timestamp, jobMetrics);
    if (flush) {
        JobMetricsUpdater_->Flush();
    }
}

void TJoblet::Persist(const TPersistenceContext& context)
{
    TJobInfoBase::Persist(context);

    using NYT::Persist;
    Persist(context, Task);
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

    if (context.IsLoad()) {
        JobMetricsUpdater_ = Task->GetTaskHost()->CreateJobMetricsUpdater();
        Revived = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

TFinishedJobInfo::TFinishedJobInfo(
    const TJobletPtr& joblet,
    NScheduler::TJobSummary summary,
    NYson::TYsonString inputPaths)
    : TJobInfo(TJobInfoBase(*joblet))
    , Summary(std::move(summary))
    , InputPaths(std::move(inputPaths))
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
    Persist(context, Account);
    Persist(context, Suspicious);
    Persist(context, LastActivityTime);
    Persist(context, BriefStatistics);
    Persist(context, Progress);
    // NB(max42): JobStatistics is not persisted intentionally since
    // it can increase the size of snapshot significantly.
}

////////////////////////////////////////////////////////////////////////////////

void TFinishedJobInfo::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Summary);
    Persist(context, InputPaths);

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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
