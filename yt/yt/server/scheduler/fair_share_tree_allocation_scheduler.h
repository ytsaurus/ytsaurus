#pragma once

#include "private.h"
#include "fair_share_tree_allocation_scheduler_structs.h"
#include "fair_share_tree_allocation_scheduler_operation_shared_state.h"
#include "fair_share_tree_element.h"
#include "fair_share_tree_scheduling_snapshot.h"
#include "fair_share_tree_snapshot.h"
#include "fields_filter.h"
#include "persistent_fair_share_tree_allocation_scheduler_state.h"
#include "scheduling_segment_manager.h"

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NScheduler {

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
        const TSchedulerCompositeElement* owningElement,
        TNonOwningElementList children,
        TDynamicAttributesList* dynamicAttributesList,
        bool useHeap);

    const TNonOwningElementList& GetChildren() const;
    TSchedulerElement* GetBestActiveChild() const;

    void OnChildAttributesUpdated(const TSchedulerElement* child);

    // For testing purposes.
    bool UsesHeapInTest() const;

private:
    const TSchedulerCompositeElement* OwningElement_;
    TDynamicAttributesList* const DynamicAttributesList_;
    const bool UseFifoSchedulingOrder_;
    const bool UseHeap_;

    TNonOwningElementList Children_;

    bool Comparator(const TSchedulerElement* lhs, const TSchedulerElement* rhs) const;

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
    TSchedulerOperationElement* BestLeafDescendant = nullptr;
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

    TDynamicAttributes& AttributesOf(const TSchedulerElement* element);
    const TDynamicAttributes& AttributesOf(const TSchedulerElement* element) const;
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
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot,
        NProfiling::TCpuInstant now);

    //! If |schedulingSnapshot| is null, all liveness checks will be disabled.
    //! This is used for dynamic attributes computation at post update.
    explicit TDynamicAttributesManager(TFairShareTreeSchedulingSnapshotPtr schedulingSnapshot = {}, int size = 0);

    void SetAttributesList(TDynamicAttributesList attributesList);

    const TDynamicAttributes& AttributesOf(const TSchedulerElement* element) const;

    void InitializeAttributesAtCompositeElement(
        TSchedulerCompositeElement* element,
        std::optional<TNonOwningElementList> consideredSchedulableChildren,
        bool useChildHeap = true);
    void InitializeAttributesAtOperation(TSchedulerOperationElement* element, bool isActive = true);

    // NB(eshcherbin): This is an ad-hoc way to initialize resource usage at a single place, where snapshot isn't ready yet.
    void InitializeResourceUsageAtPostUpdate(const TSchedulerElement* element, const TJobResources& resourceUsage);

    void ActivateOperation(TSchedulerOperationElement* element);
    void DeactivateOperation(TSchedulerOperationElement* element);

    void UpdateOperationResourceUsage(TSchedulerOperationElement* element, NProfiling::TCpuInstant now);

    void Clear();

    //! Diagnostics.
    int GetCompositeElementDeactivationCount() const;

private:
    const TFairShareTreeSchedulingSnapshotPtr SchedulingSnapshot_;
    TDynamicAttributesList AttributesList_;

    int CompositeElementDeactivationCount_ = 0;

    TDynamicAttributes& AttributesOf(const TSchedulerElement* element);

    bool ShouldCheckLiveness() const;

    void UpdateAttributesHierarchically(
        TSchedulerOperationElement* element,
        const TJobResources& resourceUsageDelta = {},
        bool checkAncestorsActiveness = true);

    // NB(eshcherbin): Should only use |UpdateAttributes| in order to update child heaps correctly.
    // The only exception is using |UpdateAttributesAtXxx| during initialization.
    void UpdateAttributes(TSchedulerElement* element);
    void UpdateAttributesAtCompositeElement(TSchedulerCompositeElement* element);
    void UpdateAttributesAtOperation(TSchedulerOperationElement* element);

    TSchedulerElement* GetBestActiveChild(TSchedulerCompositeElement* element) const;
    TSchedulerElement* GetBestActiveChildFifo(TSchedulerCompositeElement* element) const;
    TSchedulerElement* GetBestActiveChildFairShare(TSchedulerCompositeElement* element) const;

    static void SetResourceUsage(
        const TSchedulerElement* element,
        TDynamicAttributes* attributes,
        const TJobResources& resourceUsage,
        std::optional<NProfiling::TCpuInstant> updateTime = {});
    static void IncreaseResourceUsage(
        const TSchedulerElement* element,
        TDynamicAttributes* attributes,
        const TJobResources& resourceUsageDelta,
        std::optional<NProfiling::TCpuInstant> updateTime = {});

    static void DoUpdateOperationResourceUsage(
        const TSchedulerOperationElement* element,
        TDynamicAttributes* operationAttributes,
        const TFairShareTreeAllocationSchedulerOperationSharedStatePtr& operationSharedState,
        TCpuInstant now);

    struct TFillResourceUsageContext
    {
        const TFairShareTreeSnapshotPtr& TreeSnapshot;
        const TResourceUsageSnapshotPtr& ResourceUsageSnapshot;
        const TCpuInstant Now;
        TDynamicAttributesList* AttributesList;
    };
    static TJobResources FillResourceUsage(const TSchedulerElement* element, TFillResourceUsageContext* context);
    static TJobResources FillResourceUsageAtCompositeElement(
        const TSchedulerCompositeElement* element,
        TFillResourceUsageContext* context);
    static TJobResources FillResourceUsageAtOperation(
        const TSchedulerOperationElement* element,
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

    TEnumIndexedArray<NControllerAgent::EScheduleAllocationFailReason, NProfiling::TCounter> ControllerScheduleAllocationFail;
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
    TSchedulerOperationElement* OperationElement;

    bool operator ==(const TAllocationWithPreemptionInfo& other) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TAllocationWithPreemptionInfo& allocationInfo, TStringBuf /*format*/);
TString ToString(const TAllocationWithPreemptionInfo& allocationInfo);

using TAllocationWithPreemptionInfoSet = THashSet<TAllocationWithPreemptionInfo>;
using TAllocationWithPreemptionInfoSetMap = THashMap<int, TAllocationWithPreemptionInfoSet>;

} // namespace NYT::NScheduler

template <>
struct THash<NYT::NScheduler::TAllocationWithPreemptionInfo>
{
    inline size_t operator ()(const NYT::NScheduler::TAllocationWithPreemptionInfo& allocationInfo) const
    {
        return THash<NYT::NScheduler::TAllocationPtr>()(allocationInfo.Allocation);
    }
};

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): This class is now huge and a bit overloaded with methods and state. Think about further refactoring.
class TScheduleAllocationsContext
    : public TRefCounted
{
public:
    DEFINE_BYREF_RO_PROPERTY(ISchedulingContextPtr, SchedulingContext);
    DEFINE_BYREF_RO_PROPERTY(TFairShareTreeSnapshotPtr, TreeSnapshot);
    DEFINE_BYVAL_RO_PROPERTY(bool, SsdPriorityPreemptionEnabled);

    DEFINE_BYREF_RW_PROPERTY(TScheduleAllocationsStatistics, SchedulingStatistics);

    // NB(eshcherbin): The following properties are public for testing purposes.
    DEFINE_BYREF_RW_PROPERTY(TAllocationWithPreemptionInfoSetMap, ConditionallyPreemptibleAllocationSetMap);

public:
    TScheduleAllocationsContext(
        ISchedulingContextPtr schedulingContext,
        TFairShareTreeSnapshotPtr treeSnapshot,
        const TFairShareTreeAllocationSchedulerNodeState* nodeState,
        bool schedulingInfoLoggingEnabled,
        ISchedulerStrategyHost* strategyHost,
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
    bool ScheduleAllocationInTest(TSchedulerOperationElement* element, bool ignorePacking);

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
        TSchedulerOperationElement* element,
        EAllocationPreemptionReason preemptionReason) const;

    TNonOwningOperationElementList ExtractBadPackingOperations();

    void StartStage(
        EAllocationSchedulingStage stage,
        TSchedulingStageProfilingCounters* profilingCounters,
        int stageAttemptIndex = 0);
    void FinishStage();
    int GetStageMaxSchedulingIndex() const;
    bool GetStagePrescheduleExecuted() const;

    // NB(eshcherbin): The following methods are public for testing purposes.
    const TSchedulerElement* FindPreemptionBlockingAncestor(
        const TSchedulerOperationElement* element,
        EAllocationPreemptionLevel allocationPreemptionLevel,
        EOperationPreemptionPriority operationPreemptionPriority) const;

    struct TPrepareConditionalUsageDiscountsContext
    {
        const EOperationPreemptionPriority TargetOperationPreemptionPriority;
        TJobResourcesWithQuota CurrentConditionalDiscount;
    };
    void PrepareConditionalUsageDiscounts(
        const TSchedulerElement* element,
        TPrepareConditionalUsageDiscountsContext* context);
    const TAllocationWithPreemptionInfoSet& GetConditionallyPreemptibleAllocationsInPool(
        const TSchedulerCompositeElement* element) const;

    const TDynamicAttributes& DynamicAttributesOf(const TSchedulerElement* element) const;

    //! Testing.
    void DeactivateOperationInTest(TSchedulerOperationElement* element);

private:
    const TCpuInstant SchedulingDeadline_;
    const ESchedulingSegment NodeSchedulingSegment_;
    const TOperationCountByPreemptionPriority OperationCountByPreemptionPriority_;
    const THashSet<int> SsdPriorityPreemptionMedia_;
    const bool SchedulingInfoLoggingEnabled_;
    const TDynamicAttributesListSnapshotPtr DynamicAttributesListSnapshot_;

    ISchedulerStrategyHost* const StrategyHost_;
    const NProfiling::TCounter ScheduleAllocationsDeadlineReachedCounter_;
    const NLogging::TLogger Logger;

    bool Initialized_ = false;

    struct TStageState
    {
        const EAllocationSchedulingStage Stage;
        TSchedulingStageProfilingCounters* const ProfilingCounters;
        const int StageAttemptIndex;

        NProfiling::TWallTimer Timer;

        bool PrescheduleExecuted = false;

        TDuration TotalDuration;
        TDuration PrescheduleDuration;
        TDuration TotalScheduleAllocationDuration;
        TDuration ExecScheduleAllocationDuration;
        TDuration PackingRecordHeartbeatDuration;
        TDuration PackingCheckDuration;
        TDuration AnalyzeAllocationsDuration;
        TEnumIndexedArray<NControllerAgent::EScheduleAllocationFailReason, int> FailedScheduleAllocation;

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
    const TStaticAttributes& StaticAttributesOf(const TSchedulerElement* element) const;
    bool IsActive(const TSchedulerElement* element) const;
    // Returns resource usage observed in current heartbeat.
    TJobResources GetCurrentResourceUsage(const TSchedulerElement* element) const;

    TJobResources GetHierarchicalAvailableResources(const TSchedulerElement* element) const;
    TJobResources GetLocalAvailableResourceLimits(const TSchedulerElement* element) const;
    TJobResources GetLocalUnconditionalUsageDiscount(const TSchedulerElement* element) const;

    void CollectConsideredSchedulableChildrenPerPool(
        const std::optional<TNonOwningOperationElementList>& consideredSchedulableOperations);

    void PrescheduleAllocation(TSchedulerElement* element, EOperationPreemptionPriority targetOperationPreemptionPriority);
    void PrescheduleAllocationAtCompositeElement(
        TSchedulerCompositeElement* element,
        EOperationPreemptionPriority targetOperationPreemptionPriority);
    void PrescheduleAllocationAtOperation(
        TSchedulerOperationElement* element,
        EOperationPreemptionPriority targetOperationPreemptionPriority);

    TSchedulerOperationElement* FindBestOperationForScheduling();
    //! Returns whether scheduling attempt was successful.
    bool ScheduleAllocation(TSchedulerOperationElement* element, bool ignorePacking);

    void PrepareConditionalUsageDiscountsAtCompositeElement(
        const TSchedulerCompositeElement* element,
        TPrepareConditionalUsageDiscountsContext* context);
    void PrepareConditionalUsageDiscountsAtOperation(
        const TSchedulerOperationElement* element,
        TPrepareConditionalUsageDiscountsContext* context);

    //! Pool methods.
    // Empty for now, save space for later.

    //! Operation methods.
    std::optional<EDeactivationReason> TryStartScheduleAllocation(
        TSchedulerOperationElement* element,
        TJobResources* precommittedResourcesOutput,
        TJobResources* availableResourcesOutput,
        TDiskResources* availableDiskResourcesOutput);
    TControllerScheduleAllocationResultPtr DoScheduleAllocation(
        TSchedulerOperationElement* element,
        const TJobResources& availableResources,
        const TDiskResources& availableDiskResources,
        TJobResources* precommittedResources);
    void FinishScheduleAllocation(TSchedulerOperationElement* element);

    EOperationPreemptionPriority GetOperationPreemptionPriority(
        const TSchedulerOperationElement* operationElement,
        EOperationPreemptionPriorityScope scope = EOperationPreemptionPriorityScope::OperationAndAncestors) const;

    bool CheckForDeactivation(TSchedulerOperationElement* element, EOperationPreemptionPriority operationPreemptionPriority);
    void ActivateOperation(TSchedulerOperationElement* element);
    void DeactivateOperation(TSchedulerOperationElement* element, EDeactivationReason reason);
    void OnOperationDeactivated(
        TSchedulerOperationElement* element,
        EDeactivationReason reason,
        bool considerInOperationCounter = true);

    std::optional<EDeactivationReason> CheckBlocked(const TSchedulerOperationElement* element) const;

    bool IsSchedulingSegmentCompatibleWithNode(const TSchedulerOperationElement* element) const;

    bool IsOperationResourceUsageOutdated(const TSchedulerOperationElement* element) const;
    void UpdateOperationResourceUsage(TSchedulerOperationElement* element);

    bool HasAllocationsSatisfyingResourceLimits(const TSchedulerOperationElement* element) const;

    TFairShareStrategyPackingConfigPtr GetPackingConfig() const;
    bool CheckPacking(const TSchedulerOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot) const;
    void ReactivateBadPackingOperations();

    // Shared state methods.
    void RecordPackingHeartbeat(const TSchedulerOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot);
    bool IsAllocationKnown(const TSchedulerOperationElement* element, TAllocationId allocationId) const;
    bool IsOperationEnabled(const TSchedulerOperationElement* element) const;
    void OnMinNeededResourcesUnsatisfied(
        const TSchedulerOperationElement* element,
        const TJobResources& availableResources,
        const TJobResources& minNeededResources) const;
    void UpdateOperationPreemptionStatusStatistics(
        const TSchedulerOperationElement* element,
        EOperationPreemptionStatus status) const;
    void IncrementOperationScheduleAllocationAttemptCount(const TSchedulerOperationElement* element) const;
    int GetOperationRunningAllocationCount(const TSchedulerOperationElement* element) const;

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

struct TAllocationSchedulerPostUpdateContext
{
    TSchedulerRootElement* RootElement;

    THashSet<int> SsdPriorityPreemptionMedia;
    TOperationElementsBySchedulingPriority SchedulableOperationsPerPriority;
    TStaticAttributesList StaticAttributesList;
    TFairShareTreeAllocationSchedulerOperationStateMap OperationIdToState;
    TFairShareTreeAllocationSchedulerSharedOperationStateMap OperationIdToSharedState;
    std::vector<TSchedulingTagFilter> KnownSchedulingTagFilters;
    TOperationCountsByPreemptionPriorityParameters OperationCountsByPreemptionPriorityParameters;
};

////////////////////////////////////////////////////////////////////////////////

struct IFairShareTreeAllocationSchedulerHost
    : public virtual TRefCounted
{
    //! Thread affinity: Control.
    virtual TFairShareTreeSnapshotPtr GetTreeSnapshot() const noexcept = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeAllocationScheduler
    : public TRefCounted
{
public:
    TFairShareTreeAllocationScheduler(
        TString treeId,
        NLogging::TLogger logger,
        TWeakPtr<IFairShareTreeAllocationSchedulerHost> host,
        IFairShareTreeHost* treeHost,
        ISchedulerStrategyHost* strategyHost,
        TFairShareStrategyTreeConfigPtr config,
        NProfiling::TProfiler profiler);

    //! Node management.
    void RegisterNode(NNodeTrackerClient::TNodeId nodeId);
    void UnregisterNode(NNodeTrackerClient::TNodeId nodeId);

    //! Process scheduling heartbeat.
    void ProcessSchedulingHeartbeat(
        const ISchedulingContextPtr& schedulingContext,
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        bool skipScheduleAllocations);

    //! Operation management.
    void RegisterOperation(const TSchedulerOperationElement* element);
    void UnregisterOperation(const TSchedulerOperationElement* element);

    void OnOperationMaterialized(const TSchedulerOperationElement* element);
    TError CheckOperationSchedulingInSeveralTreesAllowed(const TSchedulerOperationElement* element) const;

    void EnableOperation(const TSchedulerOperationElement* element) const;
    void DisableOperation(TSchedulerOperationElement* element, bool markAsNonAlive) const;

    void RegisterAllocationsFromRevivedOperation(
        TSchedulerOperationElement* element,
        std::vector<TAllocationPtr> allocations) const;
    void ProcessUpdatedAllocation(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TSchedulerOperationElement* element,
        TAllocationId allocationId,
        const TJobResources& allocationResources,
        const std::optional<TString>& allocationDataCenter,
        const std::optional<TString>& allocationInfinibandCluster,
        std::optional<EAbortReason>* maybeAbortReason) const;
    void ProcessFinishedAllocation(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TSchedulerOperationElement* element,
        TAllocationId allocationId) const;

    //! Diagnostics.
    void BuildSchedulingAttributesStringForNode(
        NNodeTrackerClient::TNodeId nodeId,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const;
    void BuildSchedulingAttributesForNode(NNodeTrackerClient::TNodeId nodeId, NYTree::TFluentMap fluent) const;
    void BuildSchedulingAttributesStringForOngoingAllocations(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const std::vector<TAllocationPtr>& allocations,
        TInstant now,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const;

    static TError CheckOperationIsHung(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TSchedulerOperationElement* element,
        TInstant now,
        TInstant activationTime,
        TDuration safeTimeout,
        int minScheduleAllocationCallAttempts,
        const THashSet<EDeactivationReason>& deactivationReasons);

    static void BuildOperationProgress(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TSchedulerOperationElement* element,
        ISchedulerStrategyHost* const strategyHost,
        NYTree::TFluentMap fluent);
    static void BuildElementYson(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TSchedulerElement* element,
        const TFieldsFilter& filter,
        NYTree::TFluentMap fluent);

    //! Post update.
    TAllocationSchedulerPostUpdateContext CreatePostUpdateContext(TSchedulerRootElement* rootElement);
    void PostUpdate(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TAllocationSchedulerPostUpdateContext* postUpdateContext);
    TFairShareTreeSchedulingSnapshotPtr CreateSchedulingSnapshot(TAllocationSchedulerPostUpdateContext* postUpdateContext);

    void OnResourceUsageSnapshotUpdate(const TFairShareTreeSnapshotPtr& treeSnapshot, const TResourceUsageSnapshotPtr& resourceUsageSnapshot) const;

    //! Tree profiling.
    void ProfileOperation(
        const TSchedulerOperationElement* element,
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        NProfiling::ISensorWriter* writer) const;

    //! Miscellaneous.
    void UpdateConfig(TFairShareStrategyTreeConfigPtr config);

    void BuildElementLoggingStringAttributes(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TSchedulerElement* element,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const;

    void InitPersistentState(NYTree::INodePtr persistentState);
    NYTree::INodePtr BuildPersistentState() const;

    //! Testing.
    void OnAllocationStartedInTest(
        TSchedulerOperationElement* element,
        TAllocationId allocationId,
        const TJobResourcesWithQuota& resourceUsage);
    void ProcessUpdatedAllocationInTest(
        TSchedulerOperationElement* element,
        TAllocationId allocationId,
        const TJobResources& allocationResources);
    EAllocationPreemptionStatus GetAllocationPreemptionStatusInTest(
        const TSchedulerOperationElement* element,
        TAllocationId allocationId) const;

private:
    const TString TreeId_;
    const NLogging::TLogger Logger;
    // NB(eshcherbin): While tree host and strategy host are singletons (strategy and scheduler respectively), allocation scheduler host (tree)
    // can be outlived by some asynchronous actions. Therefore, we store it as a weak pointer rather than a raw pointer.
    const TWeakPtr<IFairShareTreeAllocationSchedulerHost> Host_;
    IFairShareTreeHost* const TreeHost_;
    ISchedulerStrategyHost* const StrategyHost_;

    TFairShareStrategyTreeConfigPtr Config_;

    NProfiling::TProfiler Profiler_;

    NConcurrency::TPeriodicExecutorPtr SchedulingSegmentsManagementExecutor_;

    TEnumIndexedArray<EAllocationSchedulingStage, std::unique_ptr<TSchedulingStageProfilingCounters>> SchedulingStageProfilingCounters_;

    TFairShareTreeAllocationSchedulerOperationStateMap OperationIdToState_;
    TFairShareTreeAllocationSchedulerSharedOperationStateMap OperationIdToSharedState_;

    NProfiling::TTimeCounter CumulativeScheduleAllocationsTime_;
    NProfiling::TEventTimer ScheduleAllocationsTime_;

    NProfiling::TEventTimer GracefulPreemptionTime_;

    NProfiling::TCounter ScheduleAllocationsDeadlineReachedCounter_;

    NProfiling::TBufferedProducerPtr OperationCountByPreemptionPriorityBufferedProducer_;

    std::atomic<TCpuInstant> LastSchedulingInformationLoggedTime_ = 0;

    TCachedAllocationPreemptionStatuses CachedAllocationPreemptionStatuses_;

    std::optional<THashSet<int>> SsdPriorityPreemptionMedia_;

    TSchedulingSegmentManager SchedulingSegmentManager_;

    // TODO(eshcherbin): Add generic data structure for state sharding.
    struct alignas(CacheLineSize) TNodeStateShard
    {
        TFairShareTreeAllocationSchedulerNodeStateMap NodeIdToState;
        THashMap<NNodeTrackerClient::TNodeId, TCpuInstant> NodeIdToLastPreemptiveSchedulingTime;
    };
    std::array<TNodeStateShard, MaxNodeShardCount> NodeStateShards_;

    // NB(eshcherbin): Used only as a value to store until the initialization deadline passes
    // and we start building up-to-date persistent state.
    TInstant SchedulingSegmentsInitializationDeadline_;
    TPersistentFairShareTreeAllocationSchedulerStatePtr InitialPersistentState_ = New<TPersistentFairShareTreeAllocationSchedulerState>();
    TPersistentFairShareTreeAllocationSchedulerStatePtr PersistentState_;

    TPersistentNodeSchedulingSegmentStateMap InitialPersistentSchedulingSegmentNodeStates_;
    TPersistentOperationSchedulingSegmentStateMap InitialPersistentSchedulingSegmentOperationStates_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    //! Initialization.
    void InitSchedulingProfilingCounters();

    //! Process node heartbeat, including allocation scheduling.
    TRunningAllocationStatistics ComputeRunningAllocationStatistics(
        const TFairShareTreeAllocationSchedulerNodeState* nodeState,
        const ISchedulingContextPtr& schedulingContext,
        const TFairShareTreeSnapshotPtr& treeSnapshot);

    void PreemptAllocationsGracefully(const ISchedulingContextPtr& schedulingContext, const TFairShareTreeSnapshotPtr& treeSnapshot) const;
    void ScheduleAllocations(TScheduleAllocationsContext* context);

    void DoRegularAllocationScheduling(TScheduleAllocationsContext* context);
    void DoPreemptiveAllocationScheduling(TScheduleAllocationsContext* context);

    TPreemptiveStageWithParametersList BuildPreemptiveSchedulingStageList(TScheduleAllocationsContext* context);

    void RunRegularSchedulingStage(const TRegularSchedulingParameters& parameters, TScheduleAllocationsContext* context);
    void RunPreemptiveSchedulingStage(const TPreemptiveSchedulingParameters& parameters, TScheduleAllocationsContext* context);

    const TFairShareTreeAllocationSchedulerOperationStatePtr& GetOperationState(TOperationId operationId) const;
    const TFairShareTreeAllocationSchedulerOperationSharedStatePtr& GetOperationSharedState(TOperationId operationId) const;

    //! Node management.
    std::optional<TPersistentNodeSchedulingSegmentState> FindInitialNodePersistentState(NNodeTrackerClient::TNodeId nodeId);

    //! Operation management.
    std::optional<TPersistentOperationSchedulingSegmentState> FindInitialOperationPersistentState(TOperationId operationId);

    //! Post update.
    void UpdateSsdPriorityPreemptionMedia();

    void InitializeStaticAttributes(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TAllocationSchedulerPostUpdateContext* postUpdateContext) const;
    void CollectSchedulableOperationsPerPriority(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TAllocationSchedulerPostUpdateContext* postUpdateContext) const;

    void PublishFairShareAndUpdatePreemptionAttributes(
        TSchedulerElement* element,
        TAllocationSchedulerPostUpdateContext* postUpdateContext) const;
    void PublishFairShareAndUpdatePreemptionAttributesAtCompositeElement(
        TSchedulerCompositeElement* element,
        TAllocationSchedulerPostUpdateContext* postUpdateContext) const;
    void PublishFairShareAndUpdatePreemptionAttributesAtOperation(
        TSchedulerOperationElement* element,
        TAllocationSchedulerPostUpdateContext* postUpdateContext) const;

    void UpdateEffectiveRecursiveAttributes(
        const TSchedulerElement* element,
        TAllocationSchedulerPostUpdateContext* postUpdateContext);
    void UpdateEffectiveRecursiveAttributesAtCompositeElement(
        const TSchedulerCompositeElement* element,
        TAllocationSchedulerPostUpdateContext* postUpdateContext);
    void UpdateEffectiveRecursiveAttributesAtOperation(
        const TSchedulerOperationElement* element,
        TAllocationSchedulerPostUpdateContext* postUpdateContext);

    void ProcessUpdatedStarvationStatuses(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TAllocationSchedulerPostUpdateContext* postUpdateContext);
    void UpdateCachedAllocationPreemptionStatuses(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TAllocationSchedulerPostUpdateContext* postUpdateContext);
    void ComputeOperationSchedulingIndexes(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TAllocationSchedulerPostUpdateContext* context);
    void CollectKnownSchedulingTagFilters(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TAllocationSchedulerPostUpdateContext* postUpdateContext) const;
    void UpdateSsdNodeSchedulingAttributes(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TAllocationSchedulerPostUpdateContext* postUpdateContext) const;
    void CountOperationsByPreemptionPriority(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TAllocationSchedulerPostUpdateContext* postUpdateContext) const;

    void InitializeDynamicAttributesAtUpdateRecursively(
        TSchedulerElement* element,
        std::vector<TNonOwningElementList>* consideredSchedulableChildrenPerPool,
        TDynamicAttributesManager* dynamicAttributesManager) const;

    static void UpdateDynamicAttributesListSnapshot(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot);

    //! Miscellaneous
    const TFairShareTreeAllocationSchedulerNodeState* FindNodeState(NNodeTrackerClient::TNodeId nodeId) const;
    TFairShareTreeAllocationSchedulerNodeState* FindNodeState(NNodeTrackerClient::TNodeId nodeId);

    TFairShareTreeAllocationSchedulerOperationStateMap GetOperationStateMapSnapshot() const;
    TFairShareTreeAllocationSchedulerNodeStateMap GetNodeStateMapSnapshot() const;

    void ApplyOperationSchedulingSegmentsChanges(const TFairShareTreeAllocationSchedulerOperationStateMap& changedOperationStates);
    void ApplyNodeSchedulingSegmentsChanges(const TSetNodeSchedulingSegmentOptionsList& movedNodes);

    void ManageSchedulingSegments();
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeAllocationScheduler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
