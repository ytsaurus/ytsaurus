#include "job_info.h"

#include "job_helpers.h"
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

TJoblet::TJoblet(TTask* task, int jobIndex, int taskJobIndex, const TString& treeId, bool treeIsTentative)
    : Task(std::move(task))
    , JobIndex(jobIndex)
    , TaskJobIndex(taskJobIndex)
    , TreeId(treeId)
    , TreeIsTentative(treeIsTentative)
    , OutputCookie(IChunkPoolOutput::NullCookie)
{ }

TJobMetrics TJoblet::UpdateJobMetrics(const TJobSummary& jobSummary)
{
    const auto jobMetrics = TJobMetrics::FromJobTrackerStatistics(
        *jobSummary.Statistics,
        jobSummary.State);

    auto delta = jobMetrics - JobMetrics;
    JobMetrics = jobMetrics;
    return delta;
}

void TJoblet::Persist(const TPersistenceContext& context)
{
    TJobInfoBase::Persist(context);

    using NYT::Persist;
    Persist(context, Task);

    // COMPAT(max42)
    if (context.IsSave() || context.GetVersion() >= 202195) {
        Persist(context, TaskJobIndex);
    }

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

    if (context.IsLoad()) {
        Revived = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

TFinishedJobInfo::TFinishedJobInfo(
    const TJobletPtr& joblet,
    TJobSummary summary,
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
    Persist(context, StderrSize);
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
    Persist(context, Restartable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
