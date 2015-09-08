#include "scheduling_context.h"
#include "config.h"

#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/object_client/helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////

TSchedulingContextBase::TSchedulingContextBase(
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs,
    TCellTag cellTag)
    : Node_(node)
    , ResourceUsageDiscount_(ZeroNodeResources())
    , RunningJobs_(runningJobs)
    , Config_(config)
    , CellTag_(cellTag)
{ }

Stroka TSchedulingContextBase::GetAddress() const
{
    return Node_->GetDefaultAddress();
}

const TNodeResources& TSchedulingContextBase::ResourceLimits() const
{
    return Node_->ResourceLimits();
}

TJobPtr TSchedulingContextBase::FindStartedJob(const TJobId& jobId) const
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

TJobId TSchedulingContextBase::StartJob(
    TOperationPtr operation,
    EJobType type,
    const TNodeResources& resourceLimits,
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

TInstant TSchedulingContextBase::GetNow() const
{
    return TInstant::Now();
}

////////////////////////////////////////////////////////////////////

class TSchedulingContext
    : public TSchedulingContextBase
{
public:
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Address);
    DEFINE_BYREF_RO_PROPERTY(TNodeResources, ResourceLimits);

public:
    TSchedulingContext(
        TSchedulerConfigPtr config,
        TExecNodePtr node,
        const std::vector<TJobPtr>& runningJobs,
        TCellTag cellTag)
        : TSchedulingContextBase(
            config,
            node,
            runningJobs,
            cellTag)
        , Address_(Node_->GetDefaultAddress())
        , ResourceLimits_(Node_->ResourceLimits())
    { }
};

////////////////////////////////////////////////////////////////////

std::unique_ptr<ISchedulingContext> CreateSchedulingContext(
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs,
    TCellTag cellTag)
{
    return std::unique_ptr<ISchedulingContext>(new TSchedulingContext(
        config,
        node,
        runningJobs,
        cellTag));
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
