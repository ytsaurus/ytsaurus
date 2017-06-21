#include "scheduling_context.h"
#include "scheduling_context_detail.h"

namespace NYT {
namespace NScheduler {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TSchedulingContext
    : public TSchedulingContextBase
{
public:
    TSchedulingContext(
        TSchedulerConfigPtr config,
        TExecNodePtr node,
        NConcurrency::IThroughputThrottlerPtr jobSpecSliceThrottler,
        const std::vector<TJobPtr>& runningJobs,
        TCellTag cellTag)
        : TSchedulingContextBase(
            std::move(config),
            std::move(node),
            runningJobs,
            cellTag)
        , JobSpecSliceThrottler_(std::move(jobSpecSliceThrottler))
    { }

    virtual NProfiling::TCpuInstant GetNow() const override
    {
        return NProfiling::GetCpuInstant();
    }

    virtual const NConcurrency::IThroughputThrottlerPtr& GetJobSpecSliceThrottler() const override
    {
        return JobSpecSliceThrottler_;
    }

private:
    const NConcurrency::IThroughputThrottlerPtr JobSpecSliceThrottler_;

};

////////////////////////////////////////////////////////////////////////////////

ISchedulingContextPtr CreateSchedulingContext(
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    NConcurrency::IThroughputThrottlerPtr jobSpecSliceThrottler,
    const std::vector<TJobPtr>& runningJobs,
    TCellTag cellTag)
{
    return New<TSchedulingContext>(
        std::move(config),
        std::move(node),
        std::move(jobSpecSliceThrottler),
        runningJobs,
        cellTag);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
