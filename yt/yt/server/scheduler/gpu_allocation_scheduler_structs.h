#pragma once

#include "private.h"

#include <yt/yt/server/lib/scheduler/scheduling_segment_map.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <library/cpp/yt/compact_containers/compact_set.h>
#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

inline constexpr int MaxNodeGpuCount = 8;

////////////////////////////////////////////////////////////////////////////////

struct TGpuSchedulerAssignment final
{
    const std::string AllocationGroupName;
    TGpuSchedulerOperation* const Operation = nullptr;
    TGpuSchedulerNode* const Node = nullptr;

    TJobResourcesWithQuota ResourceUsage;

    bool Preemptible = false;
    bool Preempted = false;
    std::optional<EAllocationPreemptionReason> PreemptionReason;
    std::optional<std::string> PreemptionDescription;

    TGpuSchedulerAssignment(
        std::string allocationGroupName,
        TJobResourcesWithQuota resourceUsage,
        TGpuSchedulerOperation* operation,
        TGpuSchedulerNode* node);
};

DEFINE_REFCOUNTED_TYPE(TGpuSchedulerAssignment)

////////////////////////////////////////////////////////////////////////////////

class TGpuSchedulerOperation final
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TOperationId, Id);
    DEFINE_BYVAL_RO_PROPERTY(EOperationType, Type);
    DEFINE_BYREF_RO_PROPERTY(TAllocationGroupResourcesMap, InitialGroupedNeededResources);

    DEFINE_BYREF_RO_PROPERTY(THashSet<TGpuSchedulerAssignmentPtr>, Assignments);

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
    TGpuSchedulerOperation(
        TOperationId id,
        EOperationType type,
        const TAllocationGroupResourcesMap& initialGroupedNeededResources,
        bool gang,
        std::optional<THashSet<std::string>> specifiedSchedulingModules);

    bool IsFullHost() const;
    bool IsFullHostModuleBound() const;

    int GetInitialNeededAllocationCount() const;
    int GetReadyToAssignNeededAllocationCount() const;

    void AddAssignment(const TGpuSchedulerAssignmentPtr& assignment);
    void RemoveAssignment(const TGpuSchedulerAssignmentPtr& assignment);

    void SetPreemptible(bool preemptible);

    //! For a full-host module-bound operation returns the module, where its assignments are currently located.
    //! Operation may be not bound to any module but have running assignments (e.g. if operation briefly became preemptible).
    //! When choosing a module for such operation, we will either choose this module or preempt all assignments in it.
    std::optional<std::string> GetUsedSchedulingModule() const;

private:
    int DoGetNeededAllocationCount(const TAllocationGroupResourcesMap& groupedNeededResources) const;
};

using TGpuSchedulerOperationMap = THashMap<TOperationId, TGpuSchedulerOperationPtr>;

DEFINE_REFCOUNTED_TYPE(TGpuSchedulerOperation)

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Support disk
class TGpuSchedulerNode final
{
public:
    // NB: Descriptor may be missing if the node has only just registered and we haven't processed any heartbeats from it.
    DEFINE_BYREF_RO_PROPERTY(TExecNodeDescriptorPtr, Descriptor);
    DEFINE_BYREF_RO_PROPERTY(std::optional<std::string>, SchedulingModule);

    using TAssignmentSet = TCompactSet<TGpuSchedulerAssignmentPtr, MaxNodeGpuCount>;
    DEFINE_BYREF_RO_PROPERTY(TAssignmentSet, Assignments);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, AssignedResourceUsage);

    DEFINE_BYREF_RO_PROPERTY(TAssignmentSet, PreemptedAssignments);

public:
    int GetUnassignedGpuCount() const;

    void SetSchedulingModule(std::string schedulingModule);
    void UpdateDescriptor(TExecNodeDescriptorPtr descriptor);

    void AddAssignment(const TGpuSchedulerAssignmentPtr& assignment);
    void RemoveAssignment(const TGpuSchedulerAssignmentPtr& assignment);
    void PreemptAssignment(const TGpuSchedulerAssignmentPtr& assignment);
};

using TGpuSchedulerNodeMap = THashMap<NNodeTrackerClient::TNodeId, TGpuSchedulerNodePtr>;

DEFINE_REFCOUNTED_TYPE(TGpuSchedulerNode)


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
