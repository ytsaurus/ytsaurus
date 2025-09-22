#pragma once

#include "private.h"

#include <yt/yt/server/scheduler/strategy/policy/public.h>

#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>
#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <library/cpp/yt/compact_containers/compact_set.h>
#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

inline constexpr int MaxNodeGpuCount = 8;

////////////////////////////////////////////////////////////////////////////////

struct TAssignment final
{
    const std::string AllocationGroupName;
    TOperation* const Operation = nullptr;
    TNode* const Node = nullptr;

    TJobResourcesWithQuota ResourceUsage;

    bool Preemptible = false;
    bool Preempted = false;
    std::optional<EAllocationPreemptionReason> PreemptionReason;
    std::optional<std::string> PreemptionDescription;

    TAssignment(
        std::string allocationGroupName,
        TJobResourcesWithQuota resourceUsage,
        TOperation* operation,
        TNode* node);
};

DEFINE_REFCOUNTED_TYPE(TAssignment)

////////////////////////////////////////////////////////////////////////////////

class TOperation final
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TOperationId, Id);
    DEFINE_BYVAL_RO_PROPERTY(EOperationType, Type);
    // TODO(eshcherbin): What if some allocations complete successfully?
    DEFINE_BYREF_RO_PROPERTY(TAllocationGroupResourcesMap, InitialGroupedNeededResources);

    DEFINE_BYREF_RO_PROPERTY(THashSet<TAssignmentPtr>, Assignments);

    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceUsage);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, AssignedResourceUsage);

    DEFINE_BYREF_RW_PROPERTY(TAllocationGroupResourcesMap, ReadyToAssignGroupedNeededResources);

    // Works only for full-host module-bound operations and smaller gangs.
    DEFINE_BYREF_RO_PROPERTY(std::optional<THashSet<std::string>>, SpecifiedSchedulingModules);

    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(Gang);
    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(Starving);

    // Priority module binding may evict some current operations from a module if necessary.
    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(PriorityModuleBindingEnabled);

    //! These properties can be changed during assignment plan update.
    DEFINE_BYREF_RW_PROPERTY(std::optional<std::string>, SchedulingModule);

    // Used only for full-host module-bound operations.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TInstant>, WaitingForModuleBindingSince);
    DEFINE_BYREF_RW_PROPERTY(std::optional<TInstant>, WaitingForAssignmentsSince);

    // Full-host module-bound operation is either fully preemptible or none of its assignments are preemptible.
    // TODO(eshcherbin): Should we consider that |ReadyToAssignGroupedNeededResources| is always equal to
    // |InitialGroupedNeededResources| for full-host module-bound operations, which are not preemptible and have no assignments?
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(Preemptible);

public:
    TOperation(
        TOperationId id,
        EOperationType type,
        const TAllocationGroupResourcesMap& initialGroupedNeededResources,
        bool gang,
        std::optional<THashSet<std::string>> specifiedSchedulingModules);

    bool IsFullHost() const;
    bool IsFullHostModuleBound() const;

    int GetInitialNeededAllocationCount() const;
    int GetReadyToAssignNeededAllocationCount() const;

    void AddAssignment(const TAssignmentPtr& assignment);
    void RemoveAssignment(const TAssignmentPtr& assignment);

    void SetPreemptible(bool preemptible);

    //! For a full-host module-bound operation returns the module, where its assignments are currently located.
    //! Operation may be not bound to any module but have running assignments (e.g. if operation briefly became preemptible).
    //! When choosing a module for such operation, we will either choose this module or preempt all assignments in it.
    std::optional<std::string> GetUsedSchedulingModule() const;

private:
    int DoGetNeededAllocationCount(const TAllocationGroupResourcesMap& groupedNeededResources) const;
};

using TOperationMap = THashMap<TOperationId, TOperationPtr>;

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Support disk
class TNode final
{
public:
    // NB: Descriptor may be missing if the node has only just registered and we haven't processed any heartbeats from it.
    DEFINE_BYREF_RO_PROPERTY(TExecNodeDescriptorPtr, Descriptor);
    DEFINE_BYREF_RO_PROPERTY(std::optional<std::string>, SchedulingModule);

    using TAssignmentSet = TCompactSet<TAssignmentPtr, MaxNodeGpuCount>;
    DEFINE_BYREF_RO_PROPERTY(TAssignmentSet, Assignments);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, AssignedResourceUsage);

    DEFINE_BYREF_RO_PROPERTY(TAssignmentSet, PreemptedAssignments);

public:
    int GetUnassignedGpuCount() const;

    void SetSchedulingModule(std::string schedulingModule);
    void UpdateDescriptor(TExecNodeDescriptorPtr descriptor);

    std::vector<TDiskQuota> GetPreliminaryAssignedDiskRequests() const;

    void AddAssignment(const TAssignmentPtr& assignment);
    void RemoveAssignment(const TAssignmentPtr& assignment);
    void PreemptAssignment(const TAssignmentPtr& assignment);
};

using TNodeMap = THashMap<NNodeTrackerClient::TNodeId, TNodePtr>;

DEFINE_REFCOUNTED_TYPE(TNode)


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
