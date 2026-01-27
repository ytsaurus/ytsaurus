#pragma once

#include "structs.h"

#include <library/cpp/yt/string/string_builder.h>

#include <library/cpp/yt/yson/public.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

using TPreemptionPenalty = i64;

////////////////////////////////////////////////////////////////////////////////

class TModuleState
{
public:
    // NB(eshcherbin): This vector can and will be sorted in-place.
    DEFINE_BYREF_RW_PROPERTY(std::vector<TNode*>, AvailableNodes);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TOperation*>, FullHostBoundOperations);

public:
    int GetNodeCount() const;
    int GetUnreservedNodeCount() const;

    void AddFullHostBoundOperation(const TOperationPtr& operation);
    void RemoveFullHostBoundOperation(const TOperationPtr& operation);

private:
    int ReservedNodeCount_ = 0;
};

using TModuleStateMap = THashMap<std::string, TModuleState>;

void FormatValue(TStringBuilderBase* builder, const TModuleState& state, TStringBuf spec);
void Serialize(const TModuleState& state, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TOperationModuleBindingOutcome
{
    const int RemainingUnreservedNodeCount = 0;

    const int TotalEvictionPenalty = 0;
    const std::vector<TOperation*> OperationsToEvict;
};

bool operator<(const TOperationModuleBindingOutcome& lhs, const TOperationModuleBindingOutcome& rhs);

void FormatValue(TStringBuilderBase* builder, const TOperationModuleBindingOutcome& outcome, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

struct IAssignmentPlanUpdateContext
{
    virtual ~IAssignmentPlanUpdateContext() = default;

    virtual const TOperationMap& Operations() const = 0;
    virtual const TNodeMap& Nodes() const = 0;

    virtual void AddPlannedAssignment(
        std::string allocationGroupName,
        TJobResourcesWithQuota resourceUsage,
        TOperation* operation,
        TNode* node,
        bool preemptible = false) = 0;

    virtual void PreemptAssignment(
        const TAssignmentPtr& assignment,
        EAllocationPreemptionReason preemptionReason,
        std::string preemptionDescription) = 0;

    virtual TGpuPlanUpdateStatisticsPtr Statistics() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Rename to TAssignmentPlanner (?) and config TAssignmentPlannerConfig (???)
class TGpuAllocationAssignmentPlanUpdateExecutor
{
public:
    TGpuAllocationAssignmentPlanUpdateExecutor(
        IAssignmentPlanUpdateContext* context,
        TInstant now,
        TGpuSchedulingPolicyConfigPtr config,
        NLogging::TLogger logger);

    void Run();

private:
    IAssignmentPlanUpdateContext* const Context_;
    const TOperationMap& Operations_;
    const TNodeMap& Nodes_;
    const TInstant Now_;

    const TGpuSchedulingPolicyConfigPtr Config_;
    const NLogging::TLogger Logger;

    // NB(eshcherbin): This vector can and will be sorted in-place.
    // TODO(eshcherbin): Optimize by using set or heap instead of sorting the vector every time.
    std::vector<TNode*> SchedulableNodes_;
    NDetail::TModuleStateMap ModuleStates_;

    void InitializeModuleStates();

    //! Full-host module-bound operations planning.
    void ProcessFullHostModuleBoundOperations();
    void PlanFullHostModuleBoundOperations(
        std::vector<TOperationPtr>& operationsToPlan,
        bool priorityModuleBinding = false);
    void SortFullHostModuleBoundOperations(std::vector<TOperationPtr>& operations);

    bool ShouldUseFullHostAggressivePreemption(const TOperationPtr& operation) const;
    bool ShouldUsePriorityModuleBinding(const TOperationPtr& operation) const;

    bool BindFullHostOperationToModule(const TOperationPtr& operation, bool priorityModuleBinding);

    std::optional<NDetail::TOperationModuleBindingOutcome> ConsiderModuleForFullHostOperation(
        const TOperationPtr& operation,
        const std::string& module,
        bool priorityModuleBinding) const;
    bool FindOperationsToEvict(
        const std::vector<TOperation*>& availableOperations,
        int neededNodeCount,
        std::vector<TOperation*>* operationsToEvict,
        int* freedNodeCount) const;

    //! Other operations planning.
    void ProcessRegularOperations();
    void ProcessRegularOperationsWithExtraResources();

    //! General assignment planning.
    void PreemptAllOperationAssignments(
        const TOperationPtr& operation,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription);

    NDetail::TPreemptionPenalty GetAssignmentPreemptionPenalty(const TAssignmentPtr& assignment) const;

    //! NB: These methods sort |availableNodes| in-place.
    void PlanAllocationGroup(
        const TOperationPtr& operation,
        const std::string& allocationGroupName,
        TAllocationGroupResources allocationGroupResources,
        std::vector<TNode*>* availableNodes);
    void PlanAllocationGroupWithPreemption(
        const TOperationPtr& operation,
        const std::string& allocationGroupName,
        TAllocationGroupResources allocationGroupResources,
        std::vector<TNode*>* availableNodes,
        bool useFullHostAggressivePreemption = false);
    void PlanPreemptibleAllocationGroup(
        const TOperationPtr& operation,
        const std::string& allocationGroupName,
        TAllocationGroupResources allocationGroupResources,
        std::vector<TNode*>* availableNodes);

    void DumpModuleStatistics() const;

    class TAllocationGroupPlannerBase
    {
    public:
        DEFINE_BYVAL_RO_PROPERTY(int, PlannedAssignmentCount);

    public:
        TAllocationGroupPlannerBase(
            const TOperationPtr& operation,
            const std::string& allocationGroupName,
            const TAllocationGroupResources& allocationGroupResources,
            TGpuAllocationAssignmentPlanUpdateExecutor* host);

        virtual ~TAllocationGroupPlannerBase() = default;

        void Run();

    protected:
        const TOperationPtr& Operation_;
        const std::string& AllocationGroupName_;
        const TAllocationGroupResources& AllocationGroupResources_;
        TGpuAllocationAssignmentPlanUpdateExecutor* const Host_;

        bool CanAddAssignmentToNode(
            TNode* node,
            const TJobResources& discount = {}) const;
        virtual void AddAssignmentToNode(TNode* node);

    private:
        //! Returns |nullptr| if there are no available nodes.
        virtual TNode* FindBestAvailableNode() = 0;

        virtual bool ShouldConsiderDiskUsage() const;

        bool CanSatisfyResourceRequest(TNode* node, const TJobResources& discount) const;
        bool CanSatisfyDiskRequest(TNode* node) const;
    };

    class TAllocationGroupPlanner
        : public TAllocationGroupPlannerBase
    {
    public:
        TAllocationGroupPlanner(
            const TOperationPtr& operation,
            const std::string& allocationGroupName,
            const TAllocationGroupResources& allocationGroupResources,
            std::vector<TNode*>* availableNodes,
            TGpuAllocationAssignmentPlanUpdateExecutor* host,
            bool preemptible = false);

    private:
        std::vector<TNode*>* AvailableNodes_;
        std::vector<TNode*>::iterator NextNodeIt_;
        const bool Preemptible_;

        void AddAssignmentToNode(TNode* node) override;
        TNode* FindBestAvailableNode() override;
    };

    class TPreemptiveAllocationGroupPlanner
        : public TAllocationGroupPlannerBase
    {
    public:
        DEFINE_BYVAL_RO_PROPERTY(int, PreemptedAssignmentCount);

    public:
        using TBase = TAllocationGroupPlannerBase;

        TPreemptiveAllocationGroupPlanner(
            const TOperationPtr& operation,
            const std::string& allocationGroupName,
            const TAllocationGroupResources& allocationGroupResources,
            std::vector<TNode*>* availableNodes,
            bool useFullHostAggressivePreemption,
            TGpuAllocationAssignmentPlanUpdateExecutor* host);

    private:
        const bool UseFullHostAggressivePreemption_;

        const EAllocationPreemptionReason PreemptionReason_;
        const std::string PreemptionDescription_;

        struct TNodeWithPenalty
        {
            TNode* Node = {};
            NDetail::TPreemptionPenalty Penalty = 0;
        };
        std::vector<TNodeWithPenalty> NodeHeap_;

        struct TNodeState
        {
            TJobResources PreemptibleResourceUsage;
            std::vector<TAssignmentPtr> PreemptibleAssignments;
        };
        THashMap<TNode*, TNodeState> NodeStates_;

        //! Returns the penalty for adding one more assignment to |node|.
        NDetail::TPreemptionPenalty GetNextPreemptionPenaltyForNode(TNode* node) const;

        void AddAssignmentToNode(TNode* node) override;

        TNode* FindBestAvailableNode() override;

        bool ShouldConsiderDiskUsage() const override;
    };
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
