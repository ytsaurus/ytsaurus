#pragma once

#include "public.h"
#include "pool_tree_snapshot_state.h"

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/server/scheduler/strategy/policy/public.h>
#include <yt/yt/server/scheduler/strategy/policy/scheduling_heartbeat_context.h>
#include <yt/yt/server/scheduler/strategy/policy/structs.h>

#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <library/cpp/yt/compact_containers/compact_set.h>
#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

inline constexpr int MaxNodeGpuCount = 8;

////////////////////////////////////////////////////////////////////////////////

// TODO(yaishenka): Change to class.
struct TAssignment final
{
    const std::string AllocationGroupName;
    TOperation* const Operation;
    TNode* const Node;
    const TOperationId OperationId;
    const TInstant CreationTime;

    TJobResourcesWithQuota ResourceUsage;
    bool Preemptible = false;

    // Marks assignment with allocation waiting for revival.
    bool Reviving = false;

    TAllocationId AllocationId;
    std::optional<TInstant> PreemptibleProgressStartTime;

    TAssignment(
        std::string allocationGroupName,
        TJobResourcesWithQuota resourceUsage,
        TOperation* operation,
        TNode* node);

    void AddAllocation(const TAllocationStatePtr& allocation);
    TJobResources UpdateResourceUsage(const TJobResources& newUsage);
};

using TAllocationIdToAssignment = THashMap<TAllocationId, TAssignmentPtr>;

void Serialize(const TAssignment& assignment, NYson::IYsonConsumer* consumer);

DEFINE_REFCOUNTED_TYPE(TAssignment)

////////////////////////////////////////////////////////////////////////////////

struct TPreemptionInfo
{
    const EAllocationPreemptionReason Reason;
    const std::string Description;
    const TOperationId PreemptedForOperationId;
};

void Serialize(const TPreemptionInfo& preemptionInfo, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TAllocationState final
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TAllocationId, Id);
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerClient::TNodeId, NodeId);
    DEFINE_BYREF_RO_PROPERTY(TWeakPtr<TAssignment>, Assignment);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(std::optional<TPreemptionInfo>, PreemptionInfo);

    DEFINE_BYVAL_RO_PROPERTY(TInstant, CreationTime);

public:
    TAllocationState(
        TAllocationId id,
        NNodeTrackerClient::TNodeId nodeId,
        TWeakPtr<TAssignment> assignment,
        const TJobResources& resourceUsage);

    // Updates ResourceUsage, returns delta.
    TJobResources UpdateResourceUsage(const TJobResources& newUsage);

    void SetAssignment(TWeakPtr<TAssignment> assignment);

    TAllocationSnapshotState BuildSnapshotInfo(TOperationId operationId) const;
};

void Serialize(const TAllocationState& allocation, NYson::IYsonConsumer* consumer);

DEFINE_REFCOUNTED_TYPE(TAllocationState)

using TAllocationIdToAllocationState = THashMap<TAllocationId, TAllocationStatePtr>;

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Hide operation and node behind an interface so that
// assignment plan update algorithm would not be able to change internal state.
class TOperation final
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TOperationId, Id);
    DEFINE_BYVAL_RO_PROPERTY(EOperationType, Type);

    DEFINE_BYREF_RO_PROPERTY(std::optional<TAllocationGroupResourcesMap>, InitialGroupedNeededResources);

    DEFINE_BYREF_RO_PROPERTY(THashSet<TAssignmentPtr>, Assignments);

    DEFINE_BYREF_RO_PROPERTY(TJobResources, AssignedResourceUsage);

    using TAssignmentCountPerGroup = TCompactFlatMap<std::string, int, 8>;
    DEFINE_BYREF_RO_PROPERTY(TAssignmentCountPerGroup, EmptyAssignmentCountPerGroup);

    DEFINE_BYREF_RW_PROPERTY(TAllocationGroupResourcesMap, ReadyToAssignGroupedNeededResources);

    DEFINE_BYREF_RW_PROPERTY(TAllocationGroupResourcesMap, ExtraGroupedNeededResources);

    // Works only for full-host module-bound operations and smaller gangs.
    DEFINE_BYREF_RO_PROPERTY(std::optional<THashSet<std::string>>, SpecifiedSchedulingModules);

    DEFINE_BYREF_RO_PROPERTY(TSchedulingTagFilter, SchedulingTagFilter);

    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(Gang);
    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(Starving);

    // Priority module binding may evict some current operations from a module if necessary.
    // TODO(eshcherbin): (!) Set this property.
    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(PriorityModuleBindingEnabled);

    //! These properties can be changed during assignment plan update.
    DEFINE_BYREF_RW_PROPERTY(std::optional<std::string>, SchedulingModule);

    //! Computed from the operation's node-share on its bound module via
    //! TGpuSchedulingPolicyConfig::ModuleShareToNetworkPriority.
    //! Set only for full-host module-bound operations; reset together with SchedulingModule_.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TNetworkPriority>, NetworkPriority);

    // Used only for full-host module-bound operations.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TInstant>, WaitingForModuleBindingSince);
    DEFINE_BYREF_RW_PROPERTY(std::optional<TInstant>, WaitingForAssignmentsSince);

    // Full-host module-bound operation is either fully preemptible or none of its assignments are preemptible.
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(Preemptible);

    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(Enabled);

    DEFINE_BYREF_RO_PROPERTY(TAllocationIdToAssignment, AllocationIdToAssignment);
    DEFINE_BYREF_RO_PROPERTY(TAllocationIdToAllocationState, AllocationIdToAllocationState);

public:
    TOperation(
        TOperationId id,
        EOperationType type,
        bool gang,
        std::optional<THashSet<std::string>> specifiedSchedulingModules,
        TSchedulingTagFilter schedulingTagFilter);

    void Initialize(const TAllocationGroupResourcesMap& initialGroupedNeededResources);
    bool IsInitialized() const;

    bool IsFullHost() const;
    bool IsFullHostModuleBound() const;

    int GetInitialNeededAllocationCount() const;
    int GetReadyToAssignNeededAllocationCount() const;
    int GetExtraNeededAllocationCount() const;

    void AddPlannedAssignment(const TAssignmentPtr& assignment, bool withExtraResources = false);
    void RemoveAssignment(const TAssignmentPtr& assignment);

    //! Inserts assignment without modifying groupedNeededResources.
    void AddAssignment(const TAssignmentPtr& assignment);

    void AddAllocation(const TAllocationStatePtr& allocation, const TAssignmentPtr& assignment);

    //! Inserts a TAllocationState that has no assignment yet (orphan, awaiting node registration).
    //! Does not touch Assignments_, AllocationIdToAssignment_, EmptyAssignmentCountPerGroup_,
    //! or AssignedResourceUsage_.
    void AddOrphanAllocation(const TAllocationStatePtr& allocation);

    //! Reattaches a TAllocationState to a non-preliminary assignment that was preserved across
    //! DisableOperation(markAsNonAlive=false). The assignment's AllocationId entry must already
    //! be present in AllocationIdToAssignment_.
    void AddRevivedAllocation(
        const TAllocationStatePtr& allocation,
        const TAssignmentPtr& assignment);

    void RemoveAllocation(TAllocationId allocationId);

    //! Deletes all allocation objects. Assignments kept.
    void RemoveAllAllocations();

    void SetPreemptible(bool preemptible);

    //! For a full-host module-bound operation returns the module, where its assignments are currently located.
    //! Operation may be not bound to any module but have running assignments (e.g. if operation briefly became preemptible).
    //! When choosing a module for such operation, we will either choose this module or preempt all assignments in it.
    std::optional<std::string> GetUsedSchedulingModule() const;

    void ResetSchedulingModule();

    bool IsZeroAssignedUsage() const;

    TOperationSnapshotState BuildSnapshotInfo() const;

private:
    friend struct TAssignment;

    int DoGetNeededAllocationCount(const TAllocationGroupResourcesMap& groupedNeededResources) const;
};

using TOperationMap = THashMap<TOperationId, TOperationPtr>;

void Serialize(const TOperation& operation, NYson::IYsonConsumer* consumer);

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

struct TGpuScheduleAllocationsStatistics
    : public TScheduleAllocationsStatistics
{
    int ScheduledAllocationCount = 0;
    int PreemptedAllocationCount = 0;
};
using TGpuScheduleAllocationsStatisticsPtr = TIntrusivePtr<TGpuScheduleAllocationsStatistics>;

void Serialize(const TGpuScheduleAllocationsStatisticsPtr& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TNode final
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NNodeTrackerClient::TNodeId, Id);
    // NB: Descriptor may be missing if the node has only just registered and we haven't processed any heartbeats from it.
    DEFINE_BYREF_RO_PROPERTY(std::string, Address);
    DEFINE_BYREF_RO_PROPERTY(TExecNodeDescriptorPtr, Descriptor);
    // TODO(eshcherbin): Add type alias for scheduling module.
    DEFINE_BYREF_RW_PROPERTY(std::optional<std::string>, SchedulingModule);

    using TAssignmentSet = TCompactSet<TAssignmentPtr, MaxNodeGpuCount>;
    DEFINE_BYREF_RO_PROPERTY(TAssignmentSet, Assignments);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, AssignedResourceUsage);

    DEFINE_BYREF_RO_PROPERTY(TAllocationIdToAssignment, AllocationIdToAssignment);
    DEFINE_BYREF_RW_PROPERTY(TGpuScheduleAllocationsStatisticsPtr, LastSchedulingHeartbeatStatistics);

    using TAllocationIdSet = TCompactSet<TAllocationId, MaxNodeGpuCount>;
    DEFINE_BYREF_RO_PROPERTY(TAllocationIdSet, AllocationsToPreempt);
    DEFINE_BYREF_RO_PROPERTY(TAllocationIdSet, PreemptedAllocations);

public:
    explicit TNode(NNodeTrackerClient::TNodeId id, std::string address);

    bool IsSchedulable() const;

    int GetUnassignedGpuCount() const;

    std::vector<TDiskQuota> GetPreliminaryAssignedDiskRequests() const;

    void AddAssignment(const TAssignmentPtr& assignment);
    void RemoveAssignment(const TAssignmentPtr& assignment);
    void PreemptAssignment(
        const TAssignmentPtr& assignment,
        EAllocationPreemptionReason reason,
        std::string description,
        TOperationId preemptedForOperationId = {});

    void SetDescriptor(TExecNodeDescriptorPtr descriptor);

    void PreemptAllocation(TAllocationId allocationId);
    void RemovePreemptedAllocation(TAllocationId allocationId);

    TNodeSnapshotState BuildSnapshotInfo() const;

private:
    friend struct TAssignment;

    void AddAllocation(const TAllocationStatePtr& allocation, const TAssignmentPtr& assignment);
};

using TNodeMap = THashMap<NNodeTrackerClient::TNodeId, TNodePtr>;

void Serialize(const TNode& node, NYson::IYsonConsumer* consumer);

DEFINE_REFCOUNTED_TYPE(TNode)

////////////////////////////////////////////////////////////////////////////////

struct TGpuModuleStatistics final
{
    int TotalNodes = 0;
    int UnreservedNodes = 0;
    int FullHostModuleBoundOperations = 0;
};

void Serialize(const TGpuModuleStatistics& node, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TGpuPlanUpdateStatistics final
{
    NProfiling::TWallTimer Timer;

    TDuration UpdatingOperationResourcesDuration;
    TDuration FullHostPlanningDuration;
    TDuration RegularPlanningDuration;
    TDuration ExtraPlanningDuration;

    int PlannedAssignments = 0;
    int PreemptedAssignments = 0;

    THashMap<std::string, TGpuModuleStatistics> ModuleStatistics;
};

DEFINE_REFCOUNTED_TYPE(TGpuPlanUpdateStatistics)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
