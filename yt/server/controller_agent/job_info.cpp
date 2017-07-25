#include "job_info.h"

#include "job_helpers.h"
#include "job_metrics_updater.h"

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

TJoblet::TJoblet(std::unique_ptr<TJobMetricsUpdater> jobMetricsUpdater, TOperationControllerBase::TTaskPtr task, int jobIndex)
    : Task(std::move(task))
    , JobIndex(jobIndex)
    , StartRowIndex(-1)
    , OutputCookie(IChunkPoolOutput::NullCookie)
    , JobMetricsUpdater_(std::move(jobMetricsUpdater))
{ }

void TJoblet::SendJobMetrics(const TStatistics& jobStatistics, bool flush)
{
    // NOTE: after snapshot is loaded JobMetricsUpdater_ can be missing.
    if (JobMetricsUpdater_) {
        const auto timestamp = jobStatistics.GetTimestamp().Get(CpuInstantToInstant(GetCpuInstant()));
        const auto jobMetrics = TJobMetrics::FromJobTrackerStatistics(jobStatistics);
        JobMetricsUpdater_->Update(timestamp, jobMetrics);
        if (flush) {
            JobMetricsUpdater_->Flush();
        }
    }
}

void TJoblet::Persist(const TPersistenceContext& context)
{
    // NB: Every joblet is aborted after snapshot is loaded.
    // Here we only serialize a subset of members required for ReinstallJob to work
    // properly.
    using NYT::Persist;
    Persist(context, Task);
    Persist(context, InputStripeList);
    Persist(context, OutputCookie);

    TJobInfoBase::Persist(context);
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

} // namespace NControllerAgent
} // namespace NYT