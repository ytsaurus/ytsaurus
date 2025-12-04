#pragma once

#include "private.h"

#include <yt/yt/server/scheduler/strategy/pool_tree_snapshot.h>

#include <yt/yt/server/scheduler/common/public.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): (!) Remove this interface completely and provide tree snapshot the other way round.
struct ISchedulingPolicyHost
    : public virtual TRefCounted
{
    //! Thread affinity: Control.
    virtual TPoolTreeSnapshotPtr GetTreeSnapshot() const noexcept = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulingPolicyHost)

////////////////////////////////////////////////////////////////////////////////

struct TPostUpdateContext
    : public TRefCounted
{
    const EPolicyKind PolicyKind;

protected:
    explicit TPostUpdateContext(EPolicyKind policyKind);
};

DEFINE_REFCOUNTED_TYPE(TPostUpdateContext)

struct TPoolTreeSnapshotState
    : public TRefCounted
{
    const EPolicyKind PolicyKind;

protected:
    explicit TPoolTreeSnapshotState(EPolicyKind policyKind);
};

DEFINE_REFCOUNTED_TYPE(TPoolTreeSnapshotState)

////////////////////////////////////////////////////////////////////////////////

struct ISchedulingPolicy
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    //! Node management.
    virtual void RegisterNode(NNodeTrackerClient::TNodeId nodeId, const std::string& nodeAddress) = 0;
    virtual void UnregisterNode(NNodeTrackerClient::TNodeId nodeId) = 0;

    //! Scheduling.
    virtual void ProcessSchedulingHeartbeat(
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        bool skipScheduleAllocations) = 0;

    //! Operation management.
    virtual void RegisterOperation(const TPoolTreeOperationElement* element) = 0;
    virtual void UnregisterOperation(const TPoolTreeOperationElement* element) = 0;

    virtual TError OnOperationMaterialized(const TPoolTreeOperationElement* element) = 0;
    virtual TError CheckOperationSchedulingInSeveralTreesAllowed(const TPoolTreeOperationElement* element) const = 0;

    virtual void EnableOperation(const TPoolTreeOperationElement* element) = 0;
    virtual void DisableOperation(TPoolTreeOperationElement* element, bool markAsNonAlive) = 0;

    virtual void RegisterAllocationsFromRevivedOperation(
        TPoolTreeOperationElement* element,
        std::vector<TAllocationPtr> allocations) const = 0;
    virtual bool ProcessAllocationUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        TPoolTreeOperationElement* element,
        TAllocationId allocationId,
        const TJobResources& allocationResources,
        bool resetPreemptibleProgress,
        const std::optional<std::string>& allocationDataCenter,
        const std::optional<std::string>& allocationInfinibandCluster,
        std::optional<EAbortReason>* maybeAbortReason) const = 0;
    virtual bool ProcessFinishedAllocation(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        TPoolTreeOperationElement* element,
        TAllocationId allocationId) const = 0;

    //! Diagnostics.
    virtual void BuildSchedulingAttributesStringForNode(
        NNodeTrackerClient::TNodeId nodeId,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const = 0;
    virtual void BuildSchedulingAttributesForNode(NNodeTrackerClient::TNodeId nodeId, NYTree::TFluentMap fluent) const = 0;
    virtual void BuildSchedulingAttributesStringForOngoingAllocations(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const std::vector<TAllocationPtr>& allocations,
        TInstant now,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const = 0;

    virtual void BuildElementLoggingStringAttributes(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeElement* element,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const = 0;

    virtual void PopulateOrchidService(const NYTree::TCompositeMapServicePtr& orchidService) const = 0;

    virtual void ProfileOperation(
        const TPoolTreeOperationElement* element,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        NProfiling::ISensorWriter* writer) const = 0;

    //! Post update.
    virtual TPostUpdateContextPtr CreatePostUpdateContext(TPoolTreeRootElement* rootElement) = 0;
    virtual void PostUpdate(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContextPtr* postUpdateContext) = 0;
    virtual TPoolTreeSnapshotStatePtr CreateSnapshotState(TPostUpdateContextPtr* postUpdateContext) = 0;

    virtual void OnResourceUsageSnapshotUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot) const = 0;

    //! Miscellaneous.
    virtual void UpdateConfig(TStrategyTreeConfigPtr config) = 0;

    virtual void InitPersistentState(NYTree::INodePtr persistentState) = 0;
    virtual NYTree::INodePtr BuildPersistentState() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchedulingPolicy)

////////////////////////////////////////////////////////////////////////////////

ISchedulingPolicyPtr CreateSchedulingPolicy(
    std::string treeId,
    NLogging::TLogger logger,
    TWeakPtr<ISchedulingPolicyHost> host,
    IPoolTreeHost* treeHost,
    IStrategyHost* strategyHost,
    TStrategyTreeConfigPtr config,
    NProfiling::TProfiler profiler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
