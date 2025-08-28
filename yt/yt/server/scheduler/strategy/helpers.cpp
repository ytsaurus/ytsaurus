#include "helpers.h"

#include "operation.h"

#include <yt/yt/ytlib/scheduler/config.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

TOperationPoolTreeRuntimeParametersPtr GetSchedulingOptionsPerPoolTree(const IOperationPtr& operation, const TString& treeId)
{
    return GetOrCrash(operation->GetRuntimeParameters()->SchedulingOptionsPerPoolTree, treeId);
}

////////////////////////////////////////////////////////////////////////////////

const std::vector<TSchedulerTreeAlertDescriptor>& GetSchedulerTreeAlertDescriptors()
{
    static const std::vector<TSchedulerTreeAlertDescriptor> SchedulerTreeAlertDescriptors = {
        TSchedulerTreeAlertDescriptor{
            .Type = ESchedulerAlertType::ManageSchedulingSegments,
            .Message = "Found errors during node scheduling segments management",
        },
        TSchedulerTreeAlertDescriptor{
            .Type = ESchedulerAlertType::UnrecognizedPoolTreeConfigOptions,
            .Message = "Pool tree configs contain unrecognized options",
        },
        TSchedulerTreeAlertDescriptor{
            .Type = ESchedulerAlertType::NodesWithInsufficientResourceLimits,
            .Message = "Found nodes with insufficient resource limits",
        },
    };

    return SchedulerTreeAlertDescriptors;
}

bool IsSchedulerTreeAlertType(ESchedulerAlertType alertType)
{
    for (const auto& [type, _] : GetSchedulerTreeAlertDescriptors()) {
        if (type == alertType) {
            return true;
        }
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

TJobResources ComputeAvailableResources(
    const TJobResources& resourceLimits,
    const TJobResources& resourceUsage,
    const TJobResources& resourceDiscount)
{
    return resourceLimits - resourceUsage + resourceDiscount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
