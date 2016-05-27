#include "null_strategy.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"
#include "operation_controller.h"
#include "scheduler_strategy.h"

namespace NYT {
namespace NScheduler {

using namespace NYson;
using namespace NJobTrackerClient;

////////////////////////////////////////////////////////////////////

class TNullStrategy
    : public ISchedulerStrategy
{
public:
    virtual void ScheduleJobs(const ISchedulingContextPtr& /*context*/) override
    { }

    virtual void StartPeriodicActivity() override
    { }

    virtual void OnFairShareUpdateAt(TInstant now) override
    { }

    virtual void OnFairShareLoggingAt(TInstant now) override
    { }

    virtual void ResetState() override
    { }

    virtual TError CanAddOperation(TOperationPtr operation) override
    {
        return TError();
    }

    virtual TStatistics GetOperationTimeStatistics(const TOperationId& operationId) override
    {
        return TStatistics();
    }

    virtual void BuildOperationAttributes(const TOperationId& /*operationId*/, IYsonConsumer* /*consumer*/) override
    { }

    virtual void BuildOperationProgress(const TOperationId& /*operationId*/, IYsonConsumer* /*consumer*/) override
    { }

    virtual void BuildBriefOperationProgress(const TOperationId& /*operationId*/, IYsonConsumer* /*consumer*/) override
    { }

    virtual Stroka GetOperationLoggingProgress(const TOperationId& /*operationId*/) override
    {
        return "";
    }

    virtual void BuildOrchid(IYsonConsumer* /*consumer*/) override
    { }

    virtual void BuildBriefSpec(const TOperationId& /*operationId*/, IYsonConsumer* /*consumer*/) override
    { }

};

std::unique_ptr<ISchedulerStrategy> CreateNullStrategy(ISchedulerStrategyHost* host)
{
    Y_UNUSED(host);
    return std::unique_ptr<ISchedulerStrategy>(new TNullStrategy());
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

