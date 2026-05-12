#pragma once

#include "public.h"
#include "assignment_plan_update.h"

#include <yt/yt/server/scheduler/strategy/policy/attributes_list.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicAttributes
{
    TJobResources AssignedResourceUsage;
};

using TDynamicAttributesList = TAttributesList<TDynamicAttributes>;

////////////////////////////////////////////////////////////////////////////////

class TAssignmentHandler
{
public:
    explicit TAssignmentHandler(NLogging::TLogger logger);

    void AddPlannedAssignment(
        std::string allocationGroupName,
        TJobResourcesWithQuota resourceUsage,
        TOperation* operation,
        TNode* node,
        bool preemptible = false) const;

    void PreemptAssignment(
        const TAssignmentPtr& assignment,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription,
        std::optional<TOperationId> preemptedForOperationId = {}) const;

    void RemoveAssignment(const TAssignmentPtr& assignment, bool strict = true) const;

private:
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

class TAssignmentPlanUpdateContext
    : public IAssignmentPlanUpdateContext
{
public:
    TAssignmentPlanUpdateContext(
        NLogging::TLogger logger,
        const TOperationMap& operations,
        const TNodeMap& nodes,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TAssignmentHandler& planner,
        EGpuSchedulingPolicyMode policyMode = EGpuSchedulingPolicyMode::DryRun);

    const TOperationMap& Operations() const override;
    const TNodeMap& Nodes() const override;
    const TGpuPlanUpdateStatisticsPtr& GetStatistics() const override;

    void AddPlannedAssignment(
        std::string allocationGroupName,
        TJobResourcesWithQuota resourceUsage,
        TOperation* operation,
        TNode* node,
        bool preemptible = false) override;

    void PreemptAssignment(
        const TAssignmentPtr& assignment,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription,
        std::optional<TOperationId> preemptedForOperationId = {}) override;

    TJobResources GetAvailableOperationLimits(const TOperationPtr& operation) const override;
    std::optional<TString> FindLimitViolatingParentId(const TPoolTreeElement* element) const;

    void UpdatePreemptionStatuses() const;
    void FillOperationUsage();
    void PreemptLimitViolatingOperations();

    void UpdateOperationResources(const TOperationPtr& operation) const;
    void ResetOperationResources(const TOperationPtr& operation) const;

private:
    const NLogging::TLogger Logger;

    const TOperationMap& Operations_;
    const TNodeMap& Nodes_;
    const TGpuPlanUpdateStatisticsPtr Statistics_;

    const TPoolTreeSnapshotPtr TreeSnapshot_;

    const TAssignmentHandler& AssignmentHandler_;

    const EGpuSchedulingPolicyMode PolicyMode_;

    TDynamicAttributesList AttributesList_;

    const TOperationMap SchedulableOperations_;

    TOperationMap FilterOperationsWithElement(const TOperationMap& operations) const;

    TPoolTreeOperationElement* FindOperationElement(const TOperationPtr& operation) const;

    void UpdatePreemptionStatus(
        const TOperationPtr& operation,
        const TPoolTreeOperationElement* operationElement) const;

    TAllocationGroupResourcesMap GetGroupedNeededResources(
        const TOperationPtr& operation,
        const TPoolTreeOperationElement* operationElement) const;

    void IncreaseOperationUsage(const TOperationPtr& operation, const TJobResources& resourceDelta = {});

    void PreemptAllOperationAssignments(
        const TOperationPtr& operation,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
