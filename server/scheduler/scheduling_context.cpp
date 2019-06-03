#include "scheduling_context.h"
#include "scheduling_context_detail.h"

namespace NYT::NScheduler {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TSchedulingContext
    : public TSchedulingContextBase
{
public:
    TSchedulingContext(
        int nodeShardId,
        TSchedulerConfigPtr config,
        TExecNodePtr node,
        const std::vector<TJobPtr>& runningJobs)
        : TSchedulingContextBase(
            nodeShardId,
            std::move(config),
            std::move(node),
            runningJobs)
    { }

    virtual NProfiling::TCpuInstant GetNow() const override
    {
        return NProfiling::GetCpuInstant();
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchedulingContextPtr CreateSchedulingContext(
    int nodeShardId,
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs)
{
    return New<TSchedulingContext>(
        nodeShardId,
        std::move(config),
        std::move(node),
        runningJobs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
