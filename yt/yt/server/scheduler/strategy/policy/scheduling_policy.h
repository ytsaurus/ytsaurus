#pragma once

#include "private.h"
#include "structs.h"
#include "operation_shared_state.h"
#include "pool_tree_snapshot_state.h"
#include "persistent_state.h"
#include "scheduling_heartbeat_context.h"
#include "scheduling_segment_manager.h"

#include <yt/yt/server/scheduler/strategy/field_filter.h>
#include <yt/yt/server/scheduler/strategy/pool_tree_element.h>
#include <yt/yt/server/scheduler/strategy/pool_tree_snapshot.h>

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

constexpr int SchedulingIndexProfilingRangeCount = 12;
constexpr int MaxProfiledSchedulingStageAttemptIndex = 8;

constexpr int InvalidSchedulableChildSetIndex = -1;

////////////////////////////////////////////////////////////////////////////////

using TJobResourcesMap = THashMap<int, TJobResources>;
using TNonOwningAllocationSet = THashSet<TAllocation*>;

////////////////////////////////////////////////////////////////////////////////

class TDynamicAttributesList;

// NB(eschcherbin): It would be more correct to design this class as an interface
// with two implementations (simple and with heap), but this would introduce
// an extra level of indirection to a performance critical part of code.
class TSchedulableChildSet
{
public:
    TSchedulableChildSet(
        const TPoolTreeCompositeElement* owningElement,
        TNonOwningElementList children,
        TDynamicAttributesList* dynamicAttributesList,
        bool useHeap);

    const TNonOwningElementList& GetChildren() const;
    TPoolTreeElement* GetBestActiveChild() const;

    void OnChildAttributesUpdated(const TPoolTreeElement* child);

    // For testing purposes.
    bool UsesHeapInTest() const;

private:
    const TPoolTreeCompositeElement* OwningElement_;
    TDynamicAttributesList* const DynamicAttributesList_;
    const bool UseHeap_;

    TNonOwningElementList Children_;

    bool Comparator(const TPoolTreeElement* lhs, const TPoolTreeElement* rhs) const;

    void MoveBestChildToFront();

    void InitializeChildrenOrder();

    void OnChildAttributesUpdatedHeap(int childIndex);
    void OnChildAttributesUpdatedSimple(int childIndex);
};

////////////////////////////////////////////////////////////////////////////////

struct TDynamicAttributes
{
    //! Precomputed in dynamic attributes snapshot and updated after an allocation is scheduled or the usage is stale.
    // NB(eshcherbin): Never change this field directly, use special dynamic attributes manager's methods instead.
    TJobResources ResourceUsage;
    NProfiling::TCpuInstant ResourceUsageUpdateTime;
    bool Alive = true;
    // Local satisfaction is based on pool's usage.
    // Unlike regular satisfaction for a pool, we can precompute it in the dynamic attributes snapshot.
    double LocalSatisfactionRatio = 0.0;

    //! Computed in preschedule allocation and updated when anything about the element changes.
    double SatisfactionRatio = 0.0;
    bool Active = false;
    TPoolTreeOperationElement* BestLeafDescendant = nullptr;
    // Used only for pools.
    std::optional<TSchedulableChildSet> SchedulableChildSet;
    // Index of this element in its parent's schedulable child set.
    int SchedulableChildSetIndex = InvalidSchedulableChildSetIndex;
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicAttributesList final
    : public std::vector<TDynamicAttributes>
{
public:
    explicit TDynamicAttributesList(int size = 0);

    TDynamicAttributes& AttributesOf(const TPoolTreeElement* element);
    const TDynamicAttributes& AttributesOf(const TPoolTreeElement* element) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TDynamicAttributesListSnapshot final
{
    explicit TDynamicAttributesListSnapshot(TDynamicAttributesList value);

    const TDynamicAttributesList Value;
};

DEFINE_REFCOUNTED_TYPE(TDynamicAttributesListSnapshot)

////////////////////////////////////////////////////////////////////////////////

class TDynamicAttributesManager
{
public:
    static TDynamicAttributesList BuildDynamicAttributesListFromSnapshot(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot,
        NProfiling::TCpuInstant now);

    //! If |snapshotState| is null, all liveness checks will be disabled.
    //! This is used for dynamic attributes computation at post update.
    explicit TDynamicAttributesManager(TPoolTreeSnapshotStatePtr snapshotState = {}, int size = 0);

    void SetAttributesList(TDynamicAttributesList attributesList);

    const TDynamicAttributes& AttributesOf(const TPoolTreeElement* element) const;

    void InitializeAttributesAtCompositeElement(
        TPoolTreeCompositeElement* element,
        std::optional<TNonOwningElementList> consideredSchedulableChildren,
        bool useChildHeap = true);
    void InitializeAttributesAtOperation(TPoolTreeOperationElement* element, bool isActive = true);

    // NB(eshcherbin): This is an ad-hoc way to initialize resource usage at a single place, where snapshot isn't ready yet.
    void InitializeResourceUsageAtPostUpdate(const TPoolTreeElement* element, const TJobResources& resourceUsage);

    void ActivateOperation(TPoolTreeOperationElement* element);
    void DeactivateOperation(TPoolTreeOperationElement* element);

    void UpdateOperationResourceUsage(TPoolTreeOperationElement* element, NProfiling::TCpuInstant now);

    void Clear();

    //! Diagnostics.
    int GetCompositeElementDeactivationCount() const;

private:
    const TPoolTreeSnapshotStatePtr SnapshotState_;
    TDynamicAttributesList AttributesList_;

    int CompositeElementDeactivationCount_ = 0;

    TDynamicAttributes& AttributesOf(const TPoolTreeElement* element);

    bool ShouldCheckLiveness() const;

    void UpdateAttributesHierarchically(
        TPoolTreeOperationElement* element,
        const TJobResources& resourceUsageDelta = {},
        bool checkAncestorsActiveness = true);

    // NB(eshcherbin): Should only use |UpdateAttributes| in order to update child heaps correctly.
    // The only exception is using |UpdateAttributesAtXxx| during initialization.
    void UpdateAttributes(TPoolTreeElement* element);
    void UpdateAttributesAtCompositeElement(TPoolTreeCompositeElement* element);
    void UpdateAttributesAtOperation(TPoolTreeOperationElement* element);

    TPoolTreeElement* GetBestActiveChild(TPoolTreeCompositeElement* element) const;

    static void SetResourceUsage(
        const TPoolTreeElement* element,
        TDynamicAttributes* attributes,
        const TJobResources& resourceUsage,
        std::optional<NProfiling::TCpuInstant> updateTime = {});
    static void IncreaseResourceUsage(
        const TPoolTreeElement* element,
        TDynamicAttributes* attributes,
        const TJobResources& resourceUsageDelta,
        std::optional<NProfiling::TCpuInstant> updateTime = {});

    static void DoUpdateOperationResourceUsage(
        const TPoolTreeOperationElement* element,
        TDynamicAttributes* operationAttributes,
        const TOperationSharedStatePtr& operationSharedState,
        TCpuInstant now);

    struct TFillResourceUsageContext
    {
        const TPoolTreeSnapshotPtr& TreeSnapshot;
        const TResourceUsageSnapshotPtr& ResourceUsageSnapshot;
        const TCpuInstant Now;
        TDynamicAttributesList* AttributesList;
    };
    static TJobResources FillResourceUsage(const TPoolTreeElement* element, TFillResourceUsageContext* context);
    static TJobResources FillResourceUsageAtCompositeElement(
        const TPoolTreeCompositeElement* element,
        TFillResourceUsageContext* context);
    static TJobResources FillResourceUsageAtOperation(
        const TPoolTreeOperationElement* element,
        TFillResourceUsageContext* context);
};

////////////////////////////////////////////////////////////////////////////////

struct TSchedulingStageProfilingCounters
{
    TSchedulingStageProfilingCounters() = default;
    explicit TSchedulingStageProfilingCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter PrescheduleAllocationCount;
    NProfiling::TCounter UselessPrescheduleAllocationCount;
    NProfiling::TEventTimer PrescheduleAllocationTime;
    NProfiling::TEventTimer TotalControllerScheduleAllocationTime;
    NProfiling::TTimeGauge ControllerScheduleAllocationTime;
    NProfiling::TEventTimer ExecControllerScheduleAllocationTime;
    NProfiling::TEventTimer StrategyScheduleAllocationTime;
    NProfiling::TEventTimer PackingRecordHeartbeatTime;
    NProfiling::TEventTimer PackingCheckTime;
    NProfiling::TEventTimer AnalyzeAllocationsTime;
    NProfiling::TTimeCounter CumulativePrescheduleAllocationTime;
    NProfiling::TTimeCounter CumulativeTotalControllerScheduleAllocationTime;
    NProfiling::TTimeCounter CumulativeExecControllerScheduleAllocationTime;
    NProfiling::TTimeCounter CumulativeStrategyScheduleAllocationTime;
    NProfiling::TTimeCounter CumulativeAnalyzeAllocationsTime;
    NProfiling::TCounter ScheduleAllocationAttemptCount;
    NProfiling::TCounter ScheduleAllocationFailureCount;
    NProfiling::TCounter ControllerScheduleAllocationCount;
    NProfiling::TCounter ControllerScheduleAllocationTimedOutCount;

    TEnumIndexedArray<NControllerAgent::EScheduleFailReason, NProfiling::TCounter> ControllerScheduleAllocationFail;
    TEnumIndexedArray<EDeactivationReason, NProfiling::TCounter> DeactivationCount;
    std::array<NProfiling::TCounter, SchedulingIndexProfilingRangeCount + 1> SchedulingIndexCounters;
    std::array<NProfiling::TCounter, SchedulingIndexProfilingRangeCount + 1> MaxSchedulingIndexCounters;

    std::array<NProfiling::TCounter, MaxProfiledSchedulingStageAttemptIndex + 1> StageAttemptCount;

    NProfiling::TSummary ActiveTreeSize;
    NProfiling::TSummary ActiveOperationCount;
};

////////////////////////////////////////////////////////////////////////////////

struct TAllocationWithPreemptionInfo
{
    TAllocationPtr Allocation;
    EAllocationPreemptionStatus PreemptionStatus = EAllocationPreemptionStatus::NonPreemptible;
    TPoolTreeOperationElement* OperationElement;

    bool operator ==(const TAllocationWithPreemptionInfo& other) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TAllocationWithPreemptionInfo& allocationInfo, TStringBuf /*spec*/);

using TAllocationWithPreemptionInfoSet = THashSet<TAllocationWithPreemptionInfo>;
using TAllocationWithPreemptionInfoSetMap = THashMap<int, TAllocationWithPreemptionInfoSet>;

} // namespace NYT::NScheduler::NStrategy::NPolicy

template <>
struct THash<NYT::NScheduler::NStrategy::NPolicy::TAllocationWithPreemptionInfo>
{
    inline size_t operator ()(const NYT::NScheduler::NStrategy::NPolicy::TAllocationWithPreemptionInfo& allocationInfo) const
    {
        return THash<NYT::NScheduler::TAllocationPtr>()(allocationInfo.Allocation);
    }
};

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): This class is now huge and a bit overloaded with methods and state. Think about further refactoring.
class TScheduleAllocationsContext
    : public TRefCounted
{
public:
    DEFINE_BYREF_RO_PROPERTY(ISchedulingHeartbeatContextPtr, SchedulingHeartbeatContext);
    DEFINE_BYREF_RO_PROPERTY(TPoolTreeSnapshotPtr, TreeSnapshot);
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(SsdPriorityPreemptionEnabled);

    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(SchedulingInfoLoggingEnabled);
    DEFINE_BYREF_RW_PROPERTY(TScheduleAllocationsStatistics, SchedulingStatistics);

    // NB(eshcherbin): The following properties are public for testing purposes.
    DEFINE_BYREF_RW_PROPERTY(TAllocationWithPreemptionInfoSetMap, ConditionallyPreemptibleAllocationSetMap);

public:
    TScheduleAllocationsContext(
        ISchedulingHeartbeatContextPtr schedulingHeartbeatContext,
        TPoolTreeSnapshotPtr treeSnapshot,
        const TNodeStatePtr& nodeState,
        bool schedulingInfoLoggingEnabled,
        IStrategyHost* strategyHost,
        const NProfiling::TCounter& scheduleAllocationsDeadlineReachedCounter,
        const NLogging::TLogger& logger);

    void PrepareForScheduling();
    //! Filters schedulable elements that will be considered for schedulaing, and initializes their dynamic attributes.
    void PrescheduleAllocation(
        const std::optional<TNonOwningOperationElementList>& consideredSchedulableOperations = {},
        EOperationPreemptionPriority targetOperationPreemptionPriority = EOperationPreemptionPriority::None);

    bool ShouldContinueScheduling(const std::optional<TJobResources>& customMinSpareAllocationResources = {}) const;

    struct TFairShareScheduleAllocationResult
    {
        bool Finished = true;
        bool Scheduled = false;
    };
    TFairShareScheduleAllocationResult ScheduleAllocation(bool ignorePacking);

    // NB(eshcherbin): For testing purposes only.
    bool ScheduleAllocationInTest(TPoolTreeOperationElement* element, bool ignorePacking);

    int GetOperationWithPreemptionPriorityCount(EOperationPreemptionPriority priority) const;

    void AnalyzePreemptibleAllocations(
        EOperationPreemptionPriority targetOperationPreemptionPriority,
        EAllocationPreemptionLevel minAllocationPreemptionLevel,
        std::vector<TAllocationWithPreemptionInfo>* unconditionallyPreemptibleAllocations,
        TNonOwningAllocationSet* forcefullyPreemptibleAllocations);
    void PreemptAllocationsAfterScheduling(
        EOperationPreemptionPriority targetOperationPreemptionPriority,
        std::vector<TAllocationWithPreemptionInfo> preemptibleAllocations,
        const TNonOwningAllocationSet& forcefullyPreemptibleAllocations,
        const TAllocationPtr& allocationStartedUsingPreemption);
    void AbortAllocationsSinceResourcesOvercommit() const;
    void PreemptAllocation(
        const TAllocationPtr& allocation,
        TPoolTreeOperationElement* element,
        EAllocationPreemptionReason preemptionReason) const;

    TNonOwningOperationElementList ExtractBadPackingOperations();

    void StartStage(
        EAllocationSchedulingStage stage,
        TSchedulingStageProfilingCounters* profilingCounters,
        bool preemptive = false,
        int stageAttemptIndex = 0);
    void FinishStage();
    int GetStageMaxSchedulingIndex() const;
    bool GetStagePrescheduleExecuted() const;

    // NB(eshcherbin): The following methods are public for testing purposes.
    const TPoolTreeElement* FindPreemptionBlockingAncestor(
        const TPoolTreeOperationElement* element,
        EAllocationPreemptionLevel allocationPreemptionLevel,
        EOperationPreemptionPriority operationPreemptionPriority) const;

    struct TPrepareConditionalUsageDiscountsContext
    {
        const EOperationPreemptionPriority TargetOperationPreemptionPriority;
        TJobResourcesWithQuota CurrentConditionalDiscount;
    };
    void PrepareConditionalUsageDiscounts(
        const TPoolTreeElement* element,
        TPrepareConditionalUsageDiscountsContext* context);
    const TAllocationWithPreemptionInfoSet& GetConditionallyPreemptibleAllocationsInPool(
        const TPoolTreeCompositeElement* element) const;

    const TDynamicAttributes& DynamicAttributesOf(const TPoolTreeElement* element) const;

    bool CheckScheduleAllocationTimeoutExpired() const;

    //! Testing.
    void DeactivateOperationInTest(TPoolTreeOperationElement* element);

private:
    const TCpuInstant SchedulingDeadline_;
    const ESchedulingSegment NodeSchedulingSegment_;
    const TOperationCountByPreemptionPriority OperationCountByPreemptionPriority_;
    const THashSet<int> SsdPriorityPreemptionMedia_;
    const TDynamicAttributesListSnapshotPtr DynamicAttributesListSnapshot_;

    IStrategyHost* const StrategyHost_;
    const NProfiling::TCounter ScheduleAllocationsDeadlineReachedCounter_;
    const NLogging::TLogger Logger;

    bool Initialized_ = false;

    struct TStageState
    {
        const EAllocationSchedulingStage Stage;
        const bool Preemptive;
        TSchedulingStageProfilingCounters* const ProfilingCounters;
        const int StageAttemptIndex;

        NProfiling::TWallTimer Timer;

        bool PrescheduleExecuted = false;

        std::vector<TDuration> ScheduleAllocationDurations;

        TDuration TotalDuration;
        TDuration PrescheduleDuration;
        TDuration TotalScheduleAllocationDuration;
        TDuration ExecScheduleAllocationDuration;
        TDuration PackingRecordHeartbeatDuration;
        TDuration PackingCheckDuration;
        TDuration AnalyzeAllocationsDuration;
        TEnumIndexedArray<NControllerAgent::EScheduleFailReason, int> FailedScheduleAllocation;

        int ActiveOperationCount = 0;
        int ActiveTreeSize = 0;
        int TotalHeapElementCount = 0;
        int ScheduleAllocationAttemptCount = 0;
        int ScheduleAllocationFailureCount = 0;
        TEnumIndexedArray<EDeactivationReason, int> DeactivationReasons;
        THashMap<int, int> SchedulingIndexToScheduleAllocationAttemptCount;
        int MaxSchedulingIndex = UndefinedSchedulingIndex;
    };
    std::optional<TStageState> StageState_;

    TDynamicAttributesManager DynamicAttributesManager_;

    std::vector<bool> CanSchedule_;

    //! Populated only for pools.
    TJobResourcesMap LocalUnconditionalUsageDiscountMap_;

    // Indexed with tree index like static/dynamic attributes list.
    std::optional<std::vector<TNonOwningElementList>> ConsideredSchedulableChildrenPerPool_;

    TNonOwningOperationElementList BadPackingOperations_;

    //! Common element methods.
    const TStaticAttributes& StaticAttributesOf(const TPoolTreeElement* element) const;
    bool IsActive(const TPoolTreeElement* element) const;
    // Returns resource usage observed in current heartbeat.
    TJobResources GetCurrentResourceUsage(const TPoolTreeElement* element) const;

    TJobResources GetHierarchicalAvailableResources(const TPoolTreeElement* element, bool allowLimitsOvercommit) const;
    TJobResources GetLocalAvailableResourceLimits(const TPoolTreeElement* element, bool allowLimitsOvercommit) const;
    TJobResources GetLocalUnconditionalUsageDiscount(const TPoolTreeElement* element) const;

    void CollectConsideredSchedulableChildrenPerPool(
        const std::optional<TNonOwningOperationElementList>& consideredSchedulableOperations);

    void PrescheduleAllocation(TPoolTreeElement* element, EOperationPreemptionPriority targetOperationPreemptionPriority);
    void PrescheduleAllocationAtCompositeElement(
        TPoolTreeCompositeElement* element,
        EOperationPreemptionPriority targetOperationPreemptionPriority);
    void PrescheduleAllocationAtOperation(
        TPoolTreeOperationElement* element,
        EOperationPreemptionPriority targetOperationPreemptionPriority);

    TPoolTreeOperationElement* FindBestOperationForScheduling();
    //! Returns whether scheduling attempt was successful.
    bool ScheduleAllocation(TPoolTreeOperationElement* element, bool ignorePacking);

    void PrepareConditionalUsageDiscountsAtCompositeElement(
        const TPoolTreeCompositeElement* element,
        TPrepareConditionalUsageDiscountsContext* context);
    void PrepareConditionalUsageDiscountsAtOperation(
        const TPoolTreeOperationElement* element,
        TPrepareConditionalUsageDiscountsContext* context);

    //! Pool methods.
    // Empty for now, save space for later.

    //! Operation methods.
    std::optional<EDeactivationReason> TryStartScheduleAllocation(
        TPoolTreeOperationElement* element,
        TJobResources* precommittedResourcesOutput,
        TJobResources* availableResourcesOutput,
        TDiskResources* availableDiskResourcesOutput);
    TControllerScheduleAllocationResultPtr DoScheduleAllocation(
        TPoolTreeOperationElement* element,
        const TJobResources& availableResources,
        const TDiskResources& availableDiskResources,
        TJobResources* precommittedResources);
    void FinishScheduleAllocation(TPoolTreeOperationElement* element);

    EOperationPreemptionPriority GetOperationPreemptionPriority(
        const TPoolTreeOperationElement* operationElement,
        EOperationPreemptionPriorityScope scope = EOperationPreemptionPriorityScope::OperationAndAncestors) const;

    bool CheckForDeactivation(TPoolTreeOperationElement* element, EOperationPreemptionPriority operationPreemptionPriority);
    void ActivateOperation(TPoolTreeOperationElement* element);
    void DeactivateOperation(TPoolTreeOperationElement* element, EDeactivationReason reason);
    void OnOperationDeactivated(
        TPoolTreeOperationElement* element,
        EDeactivationReason reason,
        bool considerInOperationCounter = true);

    std::optional<EDeactivationReason> CheckBlocked(const TPoolTreeOperationElement* element) const;

    bool IsSchedulingSegmentCompatibleWithNode(const TPoolTreeOperationElement* element) const;

    bool IsOperationResourceUsageOutdated(const TPoolTreeOperationElement* element) const;
    void UpdateOperationResourceUsage(TPoolTreeOperationElement* element);

    bool HasAllocationsSatisfyingResourceLimits(
        const TPoolTreeOperationElement* element,
        TEnumIndexedArray<EJobResourceWithDiskQuotaType, bool>* unsatisfiedResources) const;

    TStrategyPackingConfigPtr GetPackingConfig() const;
    bool CheckPacking(const TPoolTreeOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot) const;
    void ReactivateBadPackingOperations();

    // Shared state methods.
    void RecordPackingHeartbeat(const TPoolTreeOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot);
    bool IsAllocationKnown(const TPoolTreeOperationElement* element, TAllocationId allocationId) const;
    bool IsOperationEnabled(const TPoolTreeOperationElement* element) const;
    void OnMinNeededResourcesUnsatisfied(
        const TPoolTreeOperationElement* element,
        const TEnumIndexedArray<EJobResourceWithDiskQuotaType, bool>& unsatisfiedResources) const;
    void UpdateOperationPreemptionStatusStatistics(
        const TPoolTreeOperationElement* element,
        EOperationPreemptionStatus status) const;
    void IncrementOperationScheduleAllocationAttemptCount(const TPoolTreeOperationElement* element) const;
    int GetOperationRunningAllocationCount(const TPoolTreeOperationElement* element) const;

    //! Other methods.
    bool CanSchedule(int schedulingTagFilterIndex) const;

    EAllocationSchedulingStage GetStageType() const;
    void ProfileAndLogStatisticsOfStage();
    void ProfileStageStatistics();
    void LogStageStatistics();

    EAllocationPreemptionLevel GetAllocationPreemptionLevel(
        const TAllocationWithPreemptionInfo& allocationWithPreemptionInfo) const;
    bool IsEligibleForSsdPriorityPreemption(const THashSet<int>& diskRequestMedia) const;
};

DEFINE_REFCOUNTED_TYPE(TScheduleAllocationsContext)

////////////////////////////////////////////////////////////////////////////////

struct TRegularSchedulingParameters
{
    const std::optional<TNonOwningOperationElementList>& ConsideredOperations = {};
    const std::optional<TJobResources>& CustomMinSpareAllocationResources = {};
    bool IgnorePacking = false;
    bool OneAllocationOnly = false;
};

struct TPreemptiveSchedulingParameters
{
    EOperationPreemptionPriority TargetOperationPreemptionPriority = EOperationPreemptionPriority::None;
    EAllocationPreemptionLevel MinAllocationPreemptionLevel = EAllocationPreemptionLevel::Preemptible;
    bool ForcePreemptionAttempt = false;
};

using TPreemptiveStageWithParameters = std::pair<EAllocationSchedulingStage, TPreemptiveSchedulingParameters>;

static constexpr int MaxPreemptiveStageCount = TEnumTraits<EAllocationSchedulingStage>::GetDomainSize();
using TPreemptiveStageWithParametersList = TCompactVector<TPreemptiveStageWithParameters, MaxPreemptiveStageCount>;

////////////////////////////////////////////////////////////////////////////////

struct TPostUpdateContext
{
    TPoolTreeRootElement* RootElement;

    THashSet<int> SsdPriorityPreemptionMedia;
    TOperationElementsBySchedulingPriority SchedulableOperationsPerPriority;
    TStaticAttributesList StaticAttributesList;
    TOperationStateMap OperationIdToState;
    TSharedOperationStateMap OperationIdToSharedState;
    std::vector<TSchedulingTagFilter> KnownSchedulingTagFilters;
    TOperationCountsByPreemptionPriorityParameters OperationCountsByPreemptionPriorityParameters;
};

////////////////////////////////////////////////////////////////////////////////

struct ISchedulingPolicyHost
    : public virtual TRefCounted
{
    //! Thread affinity: Control.
    virtual TPoolTreeSnapshotPtr GetTreeSnapshot() const noexcept = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulingPolicy
    : public TRefCounted
{
public:
    TSchedulingPolicy(
        TString treeId,
        NLogging::TLogger logger,
        TWeakPtr<ISchedulingPolicyHost> host,
        IPoolTreeHost* treeHost,
        IStrategyHost* strategyHost,
        TStrategyTreeConfigPtr config,
        NProfiling::TProfiler profiler);

    //! Node management.
    void RegisterNode(NNodeTrackerClient::TNodeId nodeId);
    void UnregisterNode(NNodeTrackerClient::TNodeId nodeId);

    //! Process scheduling heartbeat.
    void ProcessSchedulingHeartbeat(
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        bool skipScheduleAllocations);

    //! Operation management.
    void RegisterOperation(const TPoolTreeOperationElement* element);
    void UnregisterOperation(const TPoolTreeOperationElement* element);

    TError OnOperationMaterialized(const TPoolTreeOperationElement* element);
    TError CheckOperationSchedulingInSeveralTreesAllowed(const TPoolTreeOperationElement* element) const;

    void EnableOperation(const TPoolTreeOperationElement* element) const;
    void DisableOperation(TPoolTreeOperationElement* element, bool markAsNonAlive) const;

    void RegisterAllocationsFromRevivedOperation(
        TPoolTreeOperationElement* element,
        std::vector<TAllocationPtr> allocations) const;
    bool ProcessAllocationUpdate(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        TPoolTreeOperationElement* element,
        TAllocationId allocationId,
        const TJobResources& allocationResources,
        bool resetPreemptibleProgress,
        const std::optional<std::string>& allocationDataCenter,
        const std::optional<std::string>& allocationInfinibandCluster,
        std::optional<EAbortReason>* maybeAbortReason) const;
    bool ProcessFinishedAllocation(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        TPoolTreeOperationElement* element,
        TAllocationId allocationId) const;

    //! Diagnostics.
    void BuildSchedulingAttributesStringForNode(
        NNodeTrackerClient::TNodeId nodeId,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const;
    void BuildSchedulingAttributesForNode(NNodeTrackerClient::TNodeId nodeId, NYTree::TFluentMap fluent) const;
    void BuildSchedulingAttributesStringForOngoingAllocations(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const std::vector<TAllocationPtr>& allocations,
        TInstant now,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const;

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

    //! Post update.
    TPostUpdateContext CreatePostUpdateContext(TPoolTreeRootElement* rootElement);
    void PostUpdate(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContext* postUpdateContext);
    TPoolTreeSnapshotStatePtr CreateSnapshotState(TPostUpdateContext* postUpdateContext);

    void OnResourceUsageSnapshotUpdate(const TPoolTreeSnapshotPtr& treeSnapshot, const TResourceUsageSnapshotPtr& resourceUsageSnapshot) const;

    //! Tree profiling.
    void ProfileOperation(
        const TPoolTreeOperationElement* element,
        const TPoolTreeSnapshotPtr& treeSnapshot,
        NProfiling::ISensorWriter* writer) const;

    //! Miscellaneous.
    void UpdateConfig(TStrategyTreeConfigPtr config);

    void BuildElementLoggingStringAttributes(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TPoolTreeElement* element,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const;

    void InitPersistentState(NYTree::INodePtr persistentState);
    NYTree::INodePtr BuildPersistentState() const;

    static bool IsGpuTree(const TStrategyTreeConfigPtr& config);
    bool IsGpuTree() const;

    void PopulateOrchidService(const NYTree::TCompositeMapServicePtr& orchidService) const;

    //! Testing.
    void OnAllocationStartedInTest(
        TPoolTreeOperationElement* element,
        TAllocationId allocationId,
        const TJobResourcesWithQuota& resourceUsage);
    void ProcessAllocationUpdateInTest(
        TPoolTreeOperationElement* element,
        TAllocationId allocationId,
        const TJobResources& allocationResources);
    EAllocationPreemptionStatus GetAllocationPreemptionStatusInTest(
        const TPoolTreeOperationElement* element,
        TAllocationId allocationId) const;

    TFuture<void> Stop();

private:
    const TString TreeId_;
    const NLogging::TLogger Logger;
    // NB(eshcherbin): While tree host and strategy host are singletons (strategy and scheduler respectively), allocation scheduler host (tree)
    // can be outlived by some asynchronous actions. Therefore, we store it as a weak pointer rather than a raw pointer.
    const TWeakPtr<ISchedulingPolicyHost> Host_;
    IPoolTreeHost* const TreeHost_;
    IStrategyHost* const StrategyHost_;

    TStrategyTreeConfigPtr Config_;

    NProfiling::TProfiler Profiler_;

    NConcurrency::TPeriodicExecutorPtr SchedulingSegmentsManagementExecutor_;
    NConcurrency::TPeriodicExecutorPtr MinNodeResourceLimitsCheckExecutor_;

    TEnumIndexedArray<EAllocationSchedulingStage, std::unique_ptr<TSchedulingStageProfilingCounters>> SchedulingStageProfilingCounters_;

    TOperationStateMap OperationIdToState_;
    TSharedOperationStateMap OperationIdToSharedState_;

    NProfiling::TTimeCounter CumulativeScheduleAllocationsTime_;
    NProfiling::TEventTimer ScheduleAllocationsTime_;

    NProfiling::TEventTimer GracefulPreemptionTime_;

    NProfiling::TCounter ScheduleAllocationsDeadlineReachedCounter_;

    NProfiling::TBufferedProducerPtr OperationCountByPreemptionPriorityBufferedProducer_;

    std::atomic<TCpuInstant> LastSchedulingInformationLoggedTime_ = 0;

    TCachedAllocationPreemptionStatuses CachedAllocationPreemptionStatuses_;

    std::optional<THashSet<int>> SsdPriorityPreemptionMedia_;

    TSchedulingSegmentManager SchedulingSegmentManager_;
    NYson::TYsonString SerializedSchedulingSegmentsInfo_;

    // TODO(eshcherbin): Add generic data structure for state sharding.
    struct alignas(CacheLineSize) TNodeStateShard
    {
        TNodeStateMap NodeIdToState;
        THashMap<NNodeTrackerClient::TNodeId, TCpuInstant> NodeIdToLastPreemptiveSchedulingTime;
    };
    std::array<TNodeStateShard, MaxNodeShardCount> NodeStateShards_;

    // NB(eshcherbin): Used only as a value to store until the initialization deadline passes
    // and we start building up-to-date persistent state.
    TInstant SchedulingSegmentsInitializationDeadline_;
    TPersistentStatePtr InitialPersistentState_ = New<TPersistentState>();
    TPersistentStatePtr PersistentState_;

    TPersistentNodeSchedulingSegmentStateMap InitialPersistentSchedulingSegmentNodeStates_;
    TPersistentOperationSchedulingSegmentStateMap InitialPersistentSchedulingSegmentOperationStates_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    //! Initialization.
    void InitSchedulingProfilingCounters();

    //! Process node heartbeat, including allocation scheduling.
    TRunningAllocationStatistics ComputeRunningAllocationStatistics(
        const TNodeStatePtr& nodeState,
        const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext,
        const TPoolTreeSnapshotPtr& treeSnapshot);

    void PreemptAllocationsGracefully(const ISchedulingHeartbeatContextPtr& schedulingHeartbeatContext, const TPoolTreeSnapshotPtr& treeSnapshot) const;
    void ScheduleAllocations(TScheduleAllocationsContext* context);

    void DoRegularAllocationScheduling(TScheduleAllocationsContext* context);
    void DoPreemptiveAllocationScheduling(TScheduleAllocationsContext* context);

    TPreemptiveStageWithParametersList BuildPreemptiveSchedulingStageList(TScheduleAllocationsContext* context);

    void RunRegularSchedulingStage(const TRegularSchedulingParameters& parameters, TScheduleAllocationsContext* context);
    void RunPreemptiveSchedulingStage(const TPreemptiveSchedulingParameters& parameters, TScheduleAllocationsContext* context);

    const TOperationStatePtr& GetOperationState(TOperationId operationId) const;
    const TOperationSharedStatePtr& GetOperationSharedState(TOperationId operationId) const;

    //! Node management.
    std::optional<TPersistentNodeSchedulingSegmentState> FindInitialNodePersistentState(NNodeTrackerClient::TNodeId nodeId);

    //! Operation management.
    std::optional<TPersistentOperationSchedulingSegmentState> FindInitialOperationPersistentState(TOperationId operationId);

    //! Post update.
    void UpdateSsdPriorityPreemptionMedia();

    void InitializeStaticAttributes(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContext* postUpdateContext) const;
    void CollectSchedulableOperationsPerPriority(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContext* postUpdateContext) const;

    void PublishFairShare(
        TPoolTreeElement* element,
        TPostUpdateContext* postUpdateContext) const;
    void PublishFairShareAtCompositeElement(
        TPoolTreeCompositeElement* element,
        TPostUpdateContext* postUpdateContext) const;
    void PublishFairShareAtOperation(
        TPoolTreeOperationElement* element,
        TPostUpdateContext* postUpdateContext) const;

    void UpdateEffectiveRecursiveAttributes(
        const TPoolTreeElement* element,
        TPostUpdateContext* postUpdateContext);
    void UpdateEffectiveRecursiveAttributesAtCompositeElement(
        const TPoolTreeCompositeElement* element,
        TPostUpdateContext* postUpdateContext);
    void UpdateEffectiveRecursiveAttributesAtOperation(
        const TPoolTreeOperationElement* element,
        TPostUpdateContext* postUpdateContext);

    void ProcessUpdatedStarvationStatuses(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContext* postUpdateContext);
    void UpdateCachedAllocationPreemptionStatuses(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContext* postUpdateContext);
    void ComputeOperationSchedulingIndexes(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContext* context);
    void CollectKnownSchedulingTagFilters(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContext* postUpdateContext) const;
    void UpdateSsdNodeSchedulingAttributes(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContext* postUpdateContext) const;
    void CountOperationsByPreemptionPriority(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TPostUpdateContext* postUpdateContext) const;

    void InitializeDynamicAttributesAtUpdateRecursively(
        TPoolTreeElement* element,
        std::vector<TNonOwningElementList>* consideredSchedulableChildrenPerPool,
        TDynamicAttributesManager* dynamicAttributesManager) const;

    static void UpdateDynamicAttributesListSnapshot(
        const TPoolTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot);

    //! Miscellaneous
    TNodeStatePtr FindNodeState(NNodeTrackerClient::TNodeId nodeId) const;

    TOperationStateMap GetOperationStateMapSnapshot() const;
    TNodeStateMap GetNodeStateMapSnapshot() const;

    void ApplyOperationSchedulingSegmentsChanges(const TOperationStateMap& changedOperationStates);
    void ApplyNodeSchedulingSegmentsChanges(const TSetNodeSchedulingSegmentOptionsList& movedNodes);

    void ManageSchedulingSegments();

    void CheckMinNodeResourceLimits();
};

DEFINE_REFCOUNTED_TYPE(TSchedulingPolicy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
