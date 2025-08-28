#pragma once

#include "gpu_allocation_scheduler_structs.h"

#include <library/cpp/yt/string/string_builder.h>

#include <library/cpp/yt/yson/public.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

using TPreemptionPenalty = i64;

////////////////////////////////////////////////////////////////////////////////

class TModuleState
{
public:
    // NB(eshcherbin): This vector can and will be sorted in-place.
    DEFINE_BYREF_RW_PROPERTY(std::vector<TGpuSchedulerNode*>, AvailableNodes);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TGpuSchedulerOperation*>, FullHostBoundOperations);

public:
    int GetNodeCount() const;
    int GetUnreservedNodeCount() const;

    void AddFullHostBoundOperation(const TGpuSchedulerOperationPtr& operation);
    void RemoveFullHostBoundOperation(const TGpuSchedulerOperationPtr& operation);

private:
    int ReservedNodeCount_ = 0;
};

void FormatValue(TStringBuilderBase* builder, const TModuleState& state, TStringBuf spec);
void Serialize(const TModuleState& state, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TOperationModuleBindingOutcome
{
    const int RemainingUnreservedNodeCount = 0;

    const int TotalEvictionPenalty = 0;
    const std::vector<TGpuSchedulerOperation*> OperationsToEvict;
};

bool operator <(const TOperationModuleBindingOutcome& lhs, const TOperationModuleBindingOutcome& rhs);

void FormatValue(TStringBuilderBase* builder, const TOperationModuleBindingOutcome& outcome, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TGpuAllocationAssignmentPlanUpdateExecutor
{
public:
    TGpuAllocationAssignmentPlanUpdateExecutor(
        const TGpuSchedulerOperationMap& operations,
        const TGpuSchedulerNodeMap& nodes,
        TInstant now,
        TGpuAllocationSchedulerConfigPtr config,
        NLogging::TLogger logger);

    void Run();

private:
    const TGpuSchedulerOperationMap& Operations_;
    const TGpuSchedulerNodeMap& Nodes_;
    const TInstant Now_;

    const TGpuAllocationSchedulerConfigPtr Config_;
    const NLogging::TLogger Logger;

    // NB(eshcherbin): This vector can and will be sorted in-place.
    // TODO(eshcherbin): Optimize by using set or heap instead of sorting the vector every time.
    std::vector<TGpuSchedulerNode*> SchedulableNodes_;
    THashMap<std::string, NDetail::TModuleState> ModuleStates_;

    void InitializeModuleStates();

    //! Full-host module-bound operations planning.
    void ProcessFullHostModuleBoundOperations();
    void PlanFullHostModuleBoundOperations(
        std::vector<TGpuSchedulerOperationPtr>& operationsToPlan,
        bool priorityModuleBinding = false);
    void SortFullHostModuleBoundOperations(std::vector<TGpuSchedulerOperationPtr>& operations);

    bool ShouldUseFullHostAggressivePreemption(const TGpuSchedulerOperationPtr& operation) const;
    bool ShouldUsePriorityModuleBinding(const TGpuSchedulerOperationPtr& operation) const;

    bool BindFullHostOperationToModule(const TGpuSchedulerOperationPtr& operation, bool priorityModuleBinding);

    std::optional<NDetail::TOperationModuleBindingOutcome> ConsiderModuleForFullHostOperation(
        const TGpuSchedulerOperationPtr& operation,
        const std::string& module,
        bool priorityModuleBinding) const;
    bool FindOperationsToEvict(
        const std::vector<TGpuSchedulerOperation*>& availableOperations,
        int neededNodeCount,
        std::vector<TGpuSchedulerOperation*>* operationsToEvict,
        int* freedNodeCount) const;

    //! Other operations planning.
    void ProcessRegularOperations();

    //! General assignment planning.
    void AddAssignment(const TGpuSchedulerAssignmentPtr& assignment);
    void PreemptAssignment(
        const TGpuSchedulerAssignmentPtr& assignment,
        EAllocationPreemptionReason preemptionReason,
        std::string preemptionDescription);

    void PreemptAllOperationAssignments(
        const TGpuSchedulerOperationPtr& operation,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription);

    NDetail::TPreemptionPenalty GetAssignmentPreemptionPenalty(const TGpuSchedulerAssignmentPtr& assignment) const;

    //! NB: These methods sort |availableNodes| in-place.
    void PlanAllocationGroup(
        const TGpuSchedulerOperationPtr& operation,
        const std::string& allocationGroupName,
        TAllocationGroupResources allocationGroupResources,
        std::vector<TGpuSchedulerNode*>* availableNodes);
    void PlanAllocationGroupWithPreemption(
        const TGpuSchedulerOperationPtr& operation,
        const std::string& allocationGroupName,
        TAllocationGroupResources allocationGroupResources,
        std::vector<TGpuSchedulerNode*>* availableNodes,
        bool useFullHostAggressivePreemption = false);

    class TAllocationGroupPlannerBase
    {
    public:
        DEFINE_BYVAL_RO_PROPERTY(int, PlannedAssignmentCount);

    public:
        TAllocationGroupPlannerBase(
            const TGpuSchedulerOperationPtr& operation,
            const std::string& allocationGroupName,
            const TAllocationGroupResources& allocationGroupResources,
            TGpuAllocationAssignmentPlanUpdateExecutor* host);

        virtual ~TAllocationGroupPlannerBase() = default;

        void Run();

    protected:
        const TGpuSchedulerOperationPtr& Operation_;
        const std::string& AllocationGroupName_;
        const TAllocationGroupResources& AllocationGroupResources_;
        TGpuAllocationAssignmentPlanUpdateExecutor* const Host_;

        bool CanAddAssignmentToNode(TGpuSchedulerNode* node, const TJobResources& discount = {}) const;
        virtual void AddAssignmentToNode(TGpuSchedulerNode* node);

    private:
        //! Returns |nullptr| if there are no available nodes.
        virtual TGpuSchedulerNode* GetBestAvailableNode() = 0;
    };

    class TAllocationGroupPlanner
        : public TAllocationGroupPlannerBase
    {
    public:
        TAllocationGroupPlanner(
            const TGpuSchedulerOperationPtr& operation,
            const std::string& allocationGroupName,
            const TAllocationGroupResources& allocationGroupResources,
            std::vector<TGpuSchedulerNode*>* availableNodes,
            TGpuAllocationAssignmentPlanUpdateExecutor* host);

    private:
        std::vector<TGpuSchedulerNode*>* AvailableNodes_;
        std::vector<TGpuSchedulerNode*>::iterator NextNodeIt_;

        virtual TGpuSchedulerNode* GetBestAvailableNode() override;
    };

    class TPreemptiveAllocationGroupPlanner
        : public TAllocationGroupPlannerBase
    {
    public:
        DEFINE_BYVAL_RO_PROPERTY(int, PreemptedAssignmentCount);

    public:
        using TBase = TAllocationGroupPlannerBase;

        TPreemptiveAllocationGroupPlanner(
            const TGpuSchedulerOperationPtr& operation,
            const std::string& allocationGroupName,
            const TAllocationGroupResources& allocationGroupResources,
            std::vector<TGpuSchedulerNode*>* availableNodes,
            bool useFullHostAggressivePreemption,
            TGpuAllocationAssignmentPlanUpdateExecutor* host);

    private:
        const bool UseFullHostAggressivePreemption_;

        const EAllocationPreemptionReason PreemptionReason_;
        const std::string PreemptionDescription_;

        struct TNodeWithPenalty
        {
            TGpuSchedulerNode* Node = {};
            NDetail::TPreemptionPenalty Penalty = 0;
        };
        std::vector<TNodeWithPenalty> NodeHeap_;

        struct TNodeState
        {
            TJobResources PreemptibleResourceUsage;
            std::vector<TGpuSchedulerAssignmentPtr> PreemptibleAssignments;
        };
        THashMap<TGpuSchedulerNode*, TNodeState> NodeStates_;

        //! Returns the penalty for adding one more assignment to |node|.
        NDetail::TPreemptionPenalty GetNextPreemptionPenaltyForNode(TGpuSchedulerNode* node) const;

        void AddAssignmentToNode(TGpuSchedulerNode* node) override;

        TGpuSchedulerNode* GetBestAvailableNode() override;
    };
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
