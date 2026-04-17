#pragma once

#include "private.h"

#include <yt/yt/server/scheduler/strategy/field_filter.h>

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

////////////////////////////////////////////////////////////////////////////////

struct TPoolTreeSnapshotState
    : public TRefCounted
{
    const EPolicyKind PolicyKind;

protected:
    explicit TPoolTreeSnapshotState(EPolicyKind policyKind);
};

DEFINE_REFCOUNTED_TYPE(TPoolTreeSnapshotState)

////////////////////////////////////////////////////////////////////////////////

struct TProcessAllocationUpdateResult
{
    EAllocationUpdateStatus Status = EAllocationUpdateStatus::Updated;
    bool NeedToPostpone = false;
    bool NeedToAbort = false;
    std::optional<EAbortReason> AbortReason;
};

////////////////////////////////////////////////////////////////////////////////

struct ISchedulingPolicy
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    //! Node management.
    //! Thread affinity: Control.
    virtual void RegisterNode(NNodeTrackerClient::TNodeId nodeId, const std::string& nodeAddress) = 0;
    virtual void UnregisterNode(NNodeTrackerClient::TNodeId nodeId) = 0;

    //! Scheduling.
    //! Thread affinity: Any.
    virtual void ProcessSchedulingHeartbeat(
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        bool skipScheduleAllocations) = 0;

    //! Operation management.
    //! Thread affinity: Control.
    virtual void RegisterOperation(const TPoolTreeOperationElement* element) = 0;
    virtual void UnregisterOperation(const TPoolTreeOperationElement* element) = 0;

    //! Thread affinity: Control.
    virtual TError OnOperationMaterialized(const TPoolTreeOperationElement* element) = 0;
    virtual TError CheckOperationSchedulingInSeveralTreesAllowed(const TPoolTreeOperationElement* element) const = 0;

    //! Thread affinity: Control.
    virtual void EnableOperation(const TPoolTreeOperationElement* element) = 0;
    virtual void DisableOperation(TPoolTreeOperationElement* element, bool markAsNonAlive) = 0;

    //! Thread affinity: Control.
    virtual void RegisterAllocationsFromRevivedOperation(
        TPoolTreeOperationElement* element,
        std::vector<TAllocationPtr> allocations) const = 0;

    //! Thread affinity: Any.
    virtual TProcessAllocationUpdateResult ProcessAllocationUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        TPoolTreeOperationElement* element,
        const TAllocationUpdate& allocationUpdate) = 0;

    //! Diagnostics.
    //! Thread affinity: Any.
    virtual void BuildSchedulingAttributesStringForNode(
        NNodeTrackerClient::TNodeId nodeId,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const = 0;
    virtual void BuildSchedulingAttributesForNode(NNodeTrackerClient::TNodeId nodeId, NYTree::TFluentMap fluent) const = 0;
    virtual void BuildSchedulingAttributesStringForOngoingAllocations(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const std::vector<TAllocationPtr>& allocations,
        TInstant now,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const = 0;

    //! Thread affinity: Any.
    virtual void BuildElementLoggingStringAttributes(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeElement* element,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const = 0;

    //! Thread affinity: Control.
    virtual void PopulateOrchidService(const NYTree::TCompositeMapServicePtr& orchidService) const = 0;

    //! Thread affinity: Profiler thread.
    virtual void ProfileOperation(
        const TPoolTreeOperationElement* element,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        NProfiling::ISensorWriter* writer) const = 0;

    //! Post update.
    //! Thread affinity: Control.
    virtual TPostUpdateContextPtr CreatePostUpdateContext(TPoolTreeRootElement* rootElement) = 0;

    //! Thread affinity: Any.
    virtual void PostUpdate(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContextPtr* postUpdateContext) = 0;

    //! Thread affinity: Control.
    virtual TPoolTreeSnapshotStatePtr CreateSnapshotState(TPostUpdateContextPtr* postUpdateContext) = 0;

    //! Thread affinity: Any.
    virtual void OnResourceUsageSnapshotUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot) const = 0;

    //! Miscellaneous.
    //! Thread affinity: Control.
    virtual void UpdateConfig(TStrategyTreeConfigPtr config) = 0;

    //! Thread affinity: Control.
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

// TODO(yaishenka): YT-27597 Implement this methods in GPU policy and refactor it.
struct TSchedulingPolicyStaticCaller
{
    static TError CheckOperationIsStuck(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeOperationElement* element,
        TInstant now,
        TInstant activationTime,
        const TOperationStuckCheckOptionsPtr& options);

    static void BuildOperationProgress(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeOperationElement* element,
        IStrategyHost* const strategyHost,
        NYTree::TFluentMap fluent);

    static void BuildElementYson(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeElement* element,
        const TFieldFilter& filter,
        NYTree::TFluentMap fluent);
};

} // namespace NYT::NScheduler::NStrategy::NPolicy
