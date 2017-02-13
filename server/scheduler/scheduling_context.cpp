#include "scheduling_context.h"
#include "scheduling_context_detail.h"

namespace NYT {
namespace NScheduler {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////

class TSchedulingContext
    : public TSchedulingContextBase
{
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
    { }

    virtual TInstant GetNow() const override
    {
        return TInstant::Now();
    }
};

////////////////////////////////////////////////////////////////////

ISchedulingContextPtr CreateSchedulingContext(
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs,
    TCellTag cellTag)
{
    return New<TSchedulingContext>(config, node, runningJobs, cellTag);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
