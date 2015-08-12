#include "stdafx.h"
#include "null_strategy.h"
#include "scheduler_strategy.h"
#include "operation.h"
#include "exec_node.h"
#include "job.h"
#include "operation_controller.h"

namespace NYT {
namespace NScheduler {

using namespace NYson;

////////////////////////////////////////////////////////////////////

class TNullStrategy
    : public ISchedulerStrategy
{
public:
    virtual void ScheduleJobs(ISchedulingContext* /*context*/) override
    { }

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
    UNUSED(host);
    return std::unique_ptr<ISchedulerStrategy>(new TNullStrategy());
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

