#pragma once

#include "public.h"
#include "assignment_plan_update.h"

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

class TAssignmentPlanContextBase
    : public IAssignmentPlanUpdateContext
{
public:
    explicit TAssignmentPlanContextBase(NLogging::TLogger logger);

    void AddPlannedAssignment(
        std::string allocationGroupName,
        TJobResourcesWithQuota resourceUsage,
        TOperation* operation,
        TNode* node,
        bool preemptible = false) override;

    void PreemptAssignment(
        const TAssignmentPtr& assignment,
        EAllocationPreemptionReason preemptionReason,
        std::string preemptionDescription) override;

private:
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
