#include "scheduling_context_detail.h"
#include "exec_node.h"
#include "job.h"
#include "config.h"

#include <yt/ytlib/object_client/helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////

TSchedulingContextBase::TSchedulingContextBase(
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs,
    TCellTag cellTag)
    : ResourceUsageDiscount_(ZeroJobResources())
    , ResourceUsage_(node->ResourceUsage())
    , ResourceLimits_(node->ResourceLimits())
    , RunningJobs_(runningJobs)
    , Config_(config)
    , CellTag_(cellTag)
    , Node_(node)
    , NodeDescriptor_(Node_->BuildDescriptor())
{ }

const TExecNodeDescriptor& TSchedulingContextBase::GetNodeDescriptor() const
{
    return NodeDescriptor_;
}

TJobPtr TSchedulingContextBase::GetStartedJob(const TJobId& jobId) const
{
    // TODO(acid): Is it worth making it more efficient?
    for (const auto& job : StartedJobs_) {
        if (job->GetId() == jobId) {
            return job;
        }
    }
    YUNREACHABLE();
}

bool TSchedulingContextBase::CanStartMoreJobs() const
{
    if (!Node_->HasSpareResources(ResourceUsageDiscount())) {
        return false;
    }

    auto maxJobStarts = Config_->MaxStartedJobsPerHeartbeat;
    if (maxJobStarts && StartedJobs_.size() >= maxJobStarts.Get()) {
        return false;
    }

    return true;
}

bool TSchedulingContextBase::CanSchedule(const TNullable<Stroka>& tag) const
{
    return Node_->CanSchedule(tag);
}

TJobId TSchedulingContextBase::StartJob(
    TOperationPtr operation,
    EJobType type,
    const TJobResources& resourceLimits,
    bool restarted,
    TJobSpecBuilder specBuilder)
{
    auto startTime = GetNow();
    auto id = MakeRandomId(EObjectType::SchedulerJob, CellTag_);
    auto job = New<TJob>(
        id,
        type,
        operation,
        Node_,
        startTime,
        resourceLimits,
        restarted,
        specBuilder);
    StartedJobs_.push_back(job);
    return id;
}

void TSchedulingContextBase::PreemptJob(TJobPtr job)
{
    YCHECK(job->GetNode() == Node_);
    PreemptedJobs_.push_back(job);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
