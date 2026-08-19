#pragma once

#include "config_wrapper.h"
#include "structs.h"

#include <library/cpp/yt/string/string_builder.h>

#include <library/cpp/yt/yson/public.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TModuleReservation final
{
public:
    using TReservationItem = std::variant<TOperationPtr, TAssignmentPtr>;
    DEFINE_BYVAL_RO_PROPERTY(TReservationItem, Item);
    DEFINE_BYVAL_RO_PROPERTY(int, NodeCount);

public:
    explicit TModuleReservation(const TOperationPtr& operation);
    explicit TModuleReservation(const TAssignmentPtr& assignment);

    bool IsPriorityModuleBoundOperation() const;
};

using TModuleReservationPtr = TIntrusivePtr<TModuleReservation>;

void FormatValue(TStringBuilderBase* builder, const TModuleReservation& reservation, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

class TModuleState
{
public:
    // NB(eshcherbin): This vector can and will be sorted in-place.
    DEFINE_BYREF_RW_PROPERTY(std::vector<TNode*>, AvailableNodes);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TModuleReservationPtr>, ModuleReservations);

    using TOperationToReservationMap = THashMap<TOperation*, TModuleReservationPtr>;
    DEFINE_BYREF_RO_PROPERTY(TOperationToReservationMap, FullHostBoundOperationReservations);
    DEFINE_BYVAL_RO_PROPERTY(int, FullHostNonGangAssignmentCount);

public:
    int GetNodeCount() const;
    int GetUnreservedNodeCount() const;

    void AddFullHostBoundOperation(const TOperationPtr& operation);
    void AddAssignment(const TAssignmentPtr& assignment);
    void RemoveReservation(const TModuleReservationPtr& reservation);

private:
    int ReservedNodeCount_ = 0;

    void AddReservation(TModuleReservationPtr reservation);
};

using TModuleStateMap = THashMap<std::string, TModuleState>;

void FormatValue(TStringBuilderBase* builder, const TModuleState& state, TStringBuf spec);
void Serialize(const TModuleState& state, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TOperationModuleBindingOutcome
{
    const int RemainingUnreservedNodeCount = 0;

    const int TotalEvictionPenalty = 0;
    const std::vector<TModuleReservation*> ReservationsToEvict;
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
    virtual const TGpuPlanUpdateStatisticsPtr& GetStatistics() const = 0;

    virtual TAssignmentPtr AddPlannedAssignment(
        std::string allocationGroupName,
        TJobResourcesWithQuota resourceUsage,
        TOperation* operation,
        TNode* node,
        bool preemptible = false) = 0;

    virtual void PreemptAssignment(
        const TAssignmentPtr& assignment,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription,
        TOperationId preemptedForOperationId = {}) = 0;

    virtual TJobResources GetAvailableOperationLimits(const TOperationPtr& operation) const = 0;

    virtual bool IsDetailedLoggingEnabled(const TOperationPtr& operation) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Rename to TAssignmentPlanner (?) and config TAssignmentPlannerConfig (???)
class TGpuAllocationAssignmentPlanUpdateExecutor
{
public:
    TGpuAllocationAssignmentPlanUpdateExecutor(
        IAssignmentPlanUpdateContext* context,
        TInstant now,
        TGpuSchedulingPolicyConfigWrapper config,
        NLogging::TLogger logger);

    void Run();

private:
    IAssignmentPlanUpdateContext* const Context_;
    const TOperationMap& Operations_;
    const TNodeMap& Nodes_;
    const TInstant Now_;

    const TGpuSchedulingPolicyConfigWrapper Config_;
    const NLogging::TLogger Logger;

    // NB(eshcherbin): This vector can and will be sorted in-place.
    // TODO(eshcherbin): Optimize by using set or heap instead of sorting the vector every time.
    std::vector<TNode*> SchedulableNodes_;
    NDetail::TModuleStateMap ModuleStates_;

    void InitializeModuleStates();

    //! Full-host module-bound operations planning.
    void ProcessFullHostModuleBoundOperations();
    void ProcessFullHostNonGangOperations();
    void PlanFullHostModuleBoundOperations(
        std::vector<TOperationPtr>& operationsToPlan,
        bool priorityModuleBinding = false);
    void SortFullHostModuleBoundOperations(std::vector<TOperationPtr>& operations);

    bool ShouldUseFullHostAggressivePreemption(const TOperationPtr& operation) const;
    bool ShouldUsePriorityModuleBinding(const TOperationPtr& operation) const;

    bool ShouldResetModule(const TOperationPtr& operation) const;
    void EvictReservation(
        const NDetail::TModuleReservationPtr& reservation,
        const std::string& preemptionDescription,
        const std::string& evictionModule);
    void EvictOperationFromSchedulingModule(const TOperationPtr& operation, const std::string& preemptionDescription);
    bool BindFullHostOperationToModule(const TOperationPtr& operation, bool priorityModuleBinding);

    //! Recomputes |operation->NetworkPriority()| based on the operation's node-share on its bound module.
    //! Must be called only for full-host module-bound operations with a non-null SchedulingModule().
    void UpdateNetworkPriority(const TOperationPtr& operation);

    std::optional<NDetail::TOperationModuleBindingOutcome> ConsiderModuleForFullHostOperation(
        const TOperationPtr& operation,
        const std::string& module,
        bool priorityModuleBinding) const;
    bool FindReservationsToEvict(
        const std::vector<NDetail::TModuleReservation*>& availableReservations,
        int neededNodeCount,
        std::vector<NDetail::TModuleReservation*>* reservationsToEvict,
        int* freedNodeCount) const;
    THashMap<std::string, int> DistributeAssignmentCountBetweenModules(const TAllocationGroupResources& resources) const;

    //! Other operations planning.
    void ProcessRegularOperations();
    void ProcessRegularOperationsWithExtraResources();

    //! General assignment planning.
    void PreemptAllOperationAssignments(
        const TOperationPtr& operation,
        EAllocationPreemptionReason preemptionReason,
        const std::string& preemptionDescription);

    int GetLimitedAllocationCount(
        const TOperationPtr& operation,
        const std::string& allocationGroupName,
        const TAllocationGroupResources& allocationGroupResources) const;

    //! NB: These methods sort |availableNodes| in-place.
    //! NB: AllocationGroupResources are taken by copy, because planning may modify them.
    std::vector<TAssignmentPtr> PlanAllocationGroup(
        const TOperationPtr& operation,
        const std::string& allocationGroupName,
        TAllocationGroupResources allocationGroupResources,
        std::vector<TNode*>* availableNodes,
        EGpuAssignmentPlanningStage stage);
    std::vector<TAssignmentPtr> PlanAllocationGroupWithPreemption(
        const TOperationPtr& operation,
        const std::string& allocationGroupName,
        TAllocationGroupResources allocationGroupResources,
        std::vector<TNode*>* availableNodes,
        EGpuAssignmentPlanningStage stage,
        bool useFullHostAggressivePreemption = false);
    std::vector<TAssignmentPtr> PlanPreemptibleAllocationGroup(
        const TOperationPtr& operation,
        const std::string& allocationGroupName,
        TAllocationGroupResources allocationGroupResources,
        std::vector<TNode*>* availableNodes,
        EGpuAssignmentPlanningStage stage);

    void DumpModuleStatistics() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
