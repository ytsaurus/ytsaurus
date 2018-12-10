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
        TSchedulerConfigPtr config,
        TExecNodePtr node,
        const std::vector<TJobPtr>& runningJobs)
        : TSchedulingContextBase(
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
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs)
{
    return New<TSchedulingContext>(
        std::move(config),
        std::move(node),
        runningJobs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
