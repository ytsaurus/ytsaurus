#pragma once

#include "private.h"
#include "fair_share_tree_job_scheduler_structs.h"
#include "fair_share_tree_job_scheduler_operation_shared_state.h"
#include "fair_share_tree_element.h"
#include "fair_share_tree_snapshot.h"
#include "fields_filter.h"
#include "persistent_fair_share_tree_job_scheduler_state.h"
#include "scheduling_segment_manager.h"

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

constexpr int SchedulingIndexProfilingRangeCount = 12;
constexpr int InvalidSchedulableChildSetIndex = -1;
constexpr int EmptySchedulingTagFilterIndex = -1;

////////////////////////////////////////////////////////////////////////////////

using TJobResourcesMap = THashMap<int, TJobResources>;
using TNonOwningJobSet = THashSet<TJob*>;

////////////////////////////////////////////////////////////////////////////////

using TOperationElementsBySchedulingPriority = TEnumIndexedVector<EOperationSchedulingPriority, TNonOwningOperationElementList>;

using TOperationCountByPreemptionPriority = TEnumIndexedVector<EOperationPreemptionPriority, int>;
using TOperationPreemptionPriorityParameters = std::pair<EOperationPreemptionPriorityScope, /*ssdPriorityPreemptionEnabled*/ bool>;
using TOperationCountsByPreemptionPriorityParameters = THashMap<TOperationPreemptionPriorityParameters, TOperationCountByPreemptionPriority>;

////////////////////////////////////////////////////////////////////////////////

struct TStaticAttributes
{
    int SchedulingIndex = UndefinedSchedulingIndex;
    int SchedulingTagFilterIndex = EmptySchedulingTagFilterIndex;
    bool EffectiveAggressivePreemptionAllowed = true;
    // Used for checking if operation is hung.
    bool IsAliveAtUpdate = false;

    // Only for operations.
    TFairShareTreeJobSchedulerOperationStatePtr OperationState;
    TFairShareTreeJobSchedulerOperationSharedStatePtr OperationSharedState;
    bool AreRegularJobsOnSsdNodesAllowed = true;
};

////////////////////////////////////////////////////////////////////////////////

class TStaticAttributesList final
    : public std::vector<TStaticAttributes>
{
public:
    TStaticAttributes& AttributesOf(const TSchedulerElement* element);
    const TStaticAttributes& AttributesOf(const TSchedulerElement* element) const;
};

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
    //! Precomputed in dynamic attributes snapshot and updated after a job is scheduled or the usage is stale.
    // NB(eshcherbin): Never change this field directly, use special dynamic attributes manager's methods instead.
    TJobResources ResourceUsage;
    NProfiling::TCpuInstant ResourceUsageUpdateTime;
    bool Alive = true;
    // Local satisfaction is based on pool's usage.
    // Unlike regular satisfaction for a pool, we can precompute it in the dynamic attributes snapshot.
    double LocalSatisfactionRatio = 0.0;

    //! Computed in preschedule job and updated when anything about the element changes.
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

using TDynamicAttributesListSnapshotPtr = TIntrusivePtr<TDynamicAttributesListSnapshot>;

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
        const TFairShareTreeJobSchedulerOperationSharedStatePtr& operationSharedState,
        TCpuInstant now);

    struct TFillResourceUsageContext
    {
        const TFairShareTreeSnapshotPtr& TreeSnapshot;
        const TResourceUsageSnapshotPtr& ResourceUsageSnapshot;
        const TCpuInstant Now;
        TDynamicAttributesList* AttributesList;
    };
    static TJobResources FillResourceUsage(const TSchedulerElement* element, TFillResourceUsageContext* context);
    static TJobResources FillResourceUsageAtCompositeElement(const TSchedulerCompositeElement* element, TFillResourceUsageContext* context);
    static TJobResources FillResourceUsageAtOperation(const TSchedulerOperationElement* element, TFillResourceUsageContext* context);
};

////////////////////////////////////////////////////////////////////////////////

struct TScheduleJobsProfilingCounters
{
    TScheduleJobsProfilingCounters() = default;
    explicit TScheduleJobsProfilingCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter PrescheduleJobCount;
    NProfiling::TCounter UselessPrescheduleJobCount;
    NProfiling::TEventTimer PrescheduleJobTime;
    NProfiling::TEventTimer TotalControllerScheduleJobTime;
    NProfiling::TEventTimer ExecControllerScheduleJobTime;
    NProfiling::TEventTimer StrategyScheduleJobTime;
    NProfiling::TEventTimer PackingRecordHeartbeatTime;
    NProfiling::TEventTimer PackingCheckTime;
    NProfiling::TEventTimer AnalyzeJobsTime;
    NProfiling::TTimeCounter CumulativePrescheduleJobTime;
    NProfiling::TTimeCounter CumulativeTotalControllerScheduleJobTime;
    NProfiling::TTimeCounter CumulativeExecControllerScheduleJobTime;
    NProfiling::TTimeCounter CumulativeStrategyScheduleJobTime;
    NProfiling::TTimeCounter CumulativeAnalyzeJobsTime;
    NProfiling::TCounter ScheduleJobAttemptCount;
    NProfiling::TCounter ScheduleJobFailureCount;
    NProfiling::TCounter ControllerScheduleJobCount;
    NProfiling::TCounter ControllerScheduleJobTimedOutCount;

    TEnumIndexedVector<NControllerAgent::EScheduleJobFailReason, NProfiling::TCounter> ControllerScheduleJobFail;
    TEnumIndexedVector<EDeactivationReason, NProfiling::TCounter> DeactivationCount;
    std::array<NProfiling::TCounter, SchedulingIndexProfilingRangeCount + 1> SchedulingIndexCounters;
    std::array<NProfiling::TCounter, SchedulingIndexProfilingRangeCount + 1> MaxSchedulingIndexCounters;

    NProfiling::TSummary ActiveTreeSize;
    NProfiling::TSummary ActiveOperationCount;
};

struct TScheduleJobsStage
{
    EJobSchedulingStage Type;
    TScheduleJobsProfilingCounters ProfilingCounters;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobWithPreemptionInfo
{
    TJobPtr Job;
    EJobPreemptionStatus PreemptionStatus = EJobPreemptionStatus::NonPreemptible;
    TSchedulerOperationElement* OperationElement;

    bool operator ==(const TJobWithPreemptionInfo& other) const = default;
};

void FormatValue(TStringBuilderBase* builder, const TJobWithPreemptionInfo& jobInfo, TStringBuf /*format*/);
TString ToString(const TJobWithPreemptionInfo& jobInfo);

using TJobWithPreemptionInfoSet = THashSet<TJobWithPreemptionInfo>;
using TJobWithPreemptionInfoSetMap = THashMap<int, TJobWithPreemptionInfoSet>;

} // namespace NYT::NScheduler

template <>
struct THash<NYT::NScheduler::TJobWithPreemptionInfo>
{
    inline size_t operator ()(const NYT::NScheduler::TJobWithPreemptionInfo& jobInfo) const
    {
        return THash<NYT::NScheduler::TJobPtr>()(jobInfo.Job);
    }
};

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): This class is now huge and a bit overloaded with methods and state. Think about further refactoring.
class TScheduleJobsContext
{
public:
    DEFINE_BYREF_RO_PROPERTY(ISchedulingContextPtr, SchedulingContext);

    DEFINE_BYREF_RW_PROPERTY(TScheduleJobsStatistics, SchedulingStatistics);

    DEFINE_BYVAL_RW_PROPERTY(bool, SsdPriorityPreemptionEnabled);
    DEFINE_BYREF_RW_PROPERTY(THashSet<int>, SsdPriorityPreemptionMedia)

    // NB(eshcherbin): The following properties are public for testing purposes.
    DEFINE_BYREF_RW_PROPERTY(TJobWithPreemptionInfoSetMap, ConditionallyPreemptibleJobSetMap);

public:
    TScheduleJobsContext(
        ISchedulingContextPtr schedulingContext,
        TFairShareTreeSnapshotPtr treeSnapshot,
        std::vector<TSchedulingTagFilter> knownSchedulingTagFilters,
        ESchedulingSegment nodeSchedulingSegment,
        const TOperationCountByPreemptionPriority& operationCountByPreemptionPriority,
        bool enableSchedulingInfoLogging,
        ISchedulerStrategyHost* strategyHost,
        const NLogging::TLogger& logger);

    void PrepareForScheduling();
    //! Filters schedulable elements that will be considered for schedulaing, and initializes their dynamic attributes.
    void PrescheduleJob(
        const std::optional<TNonOwningOperationElementList>& consideredSchedulableOperations = {},
        EOperationPreemptionPriority targetOperationPreemptionPriority = EOperationPreemptionPriority::None);

    struct TFairShareScheduleJobResult
    {
        bool Finished = true;
        bool Scheduled = false;
    };
    TFairShareScheduleJobResult ScheduleJob(bool ignorePacking);

    // NB(eshcherbin): For testing purposes only.
    bool ScheduleJobInTest(TSchedulerOperationElement* element, bool ignorePacking);

    int GetOperationWithPreemptionPriorityCount(EOperationPreemptionPriority priority) const;

    void AnalyzePreemptibleJobs(
        EOperationPreemptionPriority targetOperationPreemptionPriority,
        EJobPreemptionLevel minJobPreemptionLevel,
        std::vector<TJobWithPreemptionInfo>* unconditionallyPreemptibleJobs,
        TNonOwningJobSet* forcefullyPreemptibleJobs);
    void PreemptJobsAfterScheduling(
        EOperationPreemptionPriority targetOperationPreemptionPriority,
        std::vector<TJobWithPreemptionInfo> preemptibleJobs,
        const TNonOwningJobSet& forcefullyPreemptibleJobs,
        const TJobPtr& jobStartedUsingPreemption);
    void AbortJobsSinceResourcesOvercommit() const;
    void PreemptJob(
        const TJobPtr& job,
        TSchedulerOperationElement* element,
        EJobPreemptionReason preemptionReason) const;

    void ReactivateBadPackingOperations();
    bool HasBadPackingOperations() const;

    void StartStage(TScheduleJobsStage* schedulingStage);
    void FinishStage();
    int GetStageMaxSchedulingIndex() const;
    bool GetStagePrescheduleExecuted() const;

    void SetDynamicAttributesListSnapshot(TDynamicAttributesListSnapshotPtr snapshot);

    // NB(eshcherbin): The following methods are public for testing purposes.
    const TSchedulerElement* FindPreemptionBlockingAncestor(
        const TSchedulerOperationElement* element,
        EOperationPreemptionPriority operationPreemptionPriority) const;

    struct TPrepareConditionalUsageDiscountsContext
    {
        const EOperationPreemptionPriority TargetOperationPreemptionPriority;
        TJobResources CurrentConditionalDiscount;
    };
    void PrepareConditionalUsageDiscounts(const TSchedulerElement* element, TPrepareConditionalUsageDiscountsContext* context);
    const TJobWithPreemptionInfoSet& GetConditionallyPreemptibleJobsInPool(const TSchedulerCompositeElement* element) const;

    const TDynamicAttributes& DynamicAttributesOf(const TSchedulerElement* element) const;

    //! Testing.
    void DeactivateOperationInTest(TSchedulerOperationElement* element);

private:
    const TFairShareTreeSnapshotPtr TreeSnapshot_;
    const std::vector<TSchedulingTagFilter> KnownSchedulingTagFilters_;
    // TODO(eshcherbin): Think about storing the entire node state here.
    const ESchedulingSegment NodeSchedulingSegment_;
    const TOperationCountByPreemptionPriority OperationCountByPreemptionPriority_;
    const bool EnableSchedulingInfoLogging_;
    ISchedulerStrategyHost* const StrategyHost_;
    const NLogging::TLogger Logger;

    bool Initialized_ = false;

    struct TStageState
    {
        TScheduleJobsStage* const SchedulingStage;

        NProfiling::TWallTimer Timer;

        bool PrescheduleExecuted = false;

        TDuration TotalDuration;
        TDuration PrescheduleDuration;
        TDuration TotalScheduleJobDuration;
        TDuration ExecScheduleJobDuration;
        TDuration PackingRecordHeartbeatDuration;
        TDuration PackingCheckDuration;
        TDuration AnalyzeJobsDuration;
        TEnumIndexedVector<NControllerAgent::EScheduleJobFailReason, int> FailedScheduleJob;

        int ActiveOperationCount = 0;
        int ActiveTreeSize = 0;
        int TotalHeapElementCount = 0;
        int ScheduleJobAttemptCount = 0;
        int ScheduleJobFailureCount = 0;
        TEnumIndexedVector<EDeactivationReason, int> DeactivationReasons;
        THashMap<int, int> SchedulingIndexToScheduleJobAttemptCount;
        int MaxSchedulingIndex = UndefinedSchedulingIndex;
    };
    std::optional<TStageState> StageState_;

    TDynamicAttributesListSnapshotPtr DynamicAttributesListSnapshot_;
    TDynamicAttributesManager DynamicAttributesManager_;

    std::vector<bool> CanSchedule_;

    std::vector<TSchedulerOperationElementPtr> BadPackingOperations_;

    // Populated only for pools.
    TJobResourcesMap LocalUnconditionalUsageDiscountMap_;

    THashMap<const TSchedulerCompositeElement*, TNonOwningElementList> ConsideredSchedulableChildrenPerPool_;

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
    std::optional<TNonOwningElementList> GetConsideredSchedulableChildrenForPool(const TSchedulerCompositeElement* element);

    void PrescheduleJob(TSchedulerElement* element, EOperationPreemptionPriority targetOperationPreemptionPriority);
    void PrescheduleJobAtCompositeElement(TSchedulerCompositeElement* element, EOperationPreemptionPriority targetOperationPreemptionPriority);
    void PrescheduleJobAtOperation(TSchedulerOperationElement* element, EOperationPreemptionPriority targetOperationPreemptionPriority);

    TSchedulerOperationElement* FindBestOperationForScheduling();
    //! Returns whether scheduling attempt was successful.
    bool ScheduleJob(TSchedulerOperationElement* element, bool ignorePacking);

    void PrepareConditionalUsageDiscountsAtCompositeElement(const TSchedulerCompositeElement* element, TPrepareConditionalUsageDiscountsContext* context);
    void PrepareConditionalUsageDiscountsAtOperation(const TSchedulerOperationElement* element, TPrepareConditionalUsageDiscountsContext* context);

    //! Pool methods.
    // Empty for now, save space for later.

    //! Operation methods.
    std::optional<EDeactivationReason> TryStartScheduleJob(
        TSchedulerOperationElement* element,
        TJobResources* precommittedResourcesOutput,
        TJobResources* availableResourcesOutput);
    TControllerScheduleJobResultPtr DoScheduleJob(
        TSchedulerOperationElement* element,
        const TJobResources& availableResources,
        TJobResources* precommittedResources);
    void FinishScheduleJob(TSchedulerOperationElement* element);

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

    bool HasJobsSatisfyingResourceLimits(const TSchedulerOperationElement* element) const;

    TFairShareStrategyPackingConfigPtr GetPackingConfig() const;
    bool CheckPacking(const TSchedulerOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot) const;

    // Shared state methods.
    void RecordPackingHeartbeat(const TSchedulerOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot);
    bool IsJobKnown(const TSchedulerOperationElement* element, TJobId jobId) const;
    bool IsOperationEnabled(const TSchedulerOperationElement* element) const;
    void OnMinNeededResourcesUnsatisfied(
        const TSchedulerOperationElement* element,
        const TJobResources& availableResources,
        const TJobResources& minNeededResources) const;
    void UpdateOperationPreemptionStatusStatistics(const TSchedulerOperationElement* element, EOperationPreemptionStatus status) const;
    int GetOperationRunningJobCount(const TSchedulerOperationElement* element) const;

    //! Other methods.
    bool CanSchedule(int schedulingTagFilterIndex) const;

    EJobSchedulingStage GetStageType() const;
    void ProfileAndLogStatisticsOfStage();
    void ProfileStageStatistics();
    void LogStageStatistics();

    EJobPreemptionLevel GetJobPreemptionLevel(const TJobWithPreemptionInfo& jobWithPreemptionInfo) const;
    bool IsEligibleForSsdPriorityPreemption(const THashSet<int>& diskRequestMedia) const;
};

////////////////////////////////////////////////////////////////////////////////

struct TPreemptiveScheduleJobsStage
{
    TScheduleJobsStage* Stage;
    EOperationPreemptionPriority TargetOperationPreemptionPriority = EOperationPreemptionPriority::None;
    EJobPreemptionLevel MinJobPreemptionLevel = EJobPreemptionLevel::Preemptible;
    bool ForcePreemptionAttempt = false;
};

static const int MaxPreemptiveStageCount = 4;
using TPreemptiveScheduleJobsStageList = TCompactVector<TPreemptiveScheduleJobsStage, MaxPreemptiveStageCount>;

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeSchedulingSnapshot
    : public TRefCounted
{
public:
    DEFINE_BYREF_RO_PROPERTY(TStaticAttributesList, StaticAttributesList);
    DEFINE_BYREF_RO_PROPERTY(TOperationElementsBySchedulingPriority, SchedulableOperationsPerPriority);
    DEFINE_BYREF_RO_PROPERTY(THashSet<int>, SsdPriorityPreemptionMedia);
    DEFINE_BYREF_RO_PROPERTY(TCachedJobPreemptionStatuses, CachedJobPreemptionStatuses);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TSchedulingTagFilter>, KnownSchedulingTagFilters);
    DEFINE_BYREF_RO_PROPERTY(TOperationCountsByPreemptionPriorityParameters, OperationCountsByPreemptionPriorityParameters);

public:
    TFairShareTreeSchedulingSnapshot(
        TStaticAttributesList staticAttributesList,
        TOperationElementsBySchedulingPriority schedulableOperationsPerPriority,
        THashSet<int> ssdPriorityPreemptionMedia,
        TCachedJobPreemptionStatuses cachedJobPreemptionStatuses,
        std::vector<TSchedulingTagFilter> knownSchedulingTagFilters,
        TOperationCountsByPreemptionPriorityParameters operationCountsByPreemptionPriorityParameters,
        TFairShareTreeJobSchedulerOperationStateMap operationIdToState,
        TFairShareTreeJobSchedulerSharedOperationStateMap operationIdToSharedState);

    const TFairShareTreeJobSchedulerOperationStatePtr& GetOperationState(const TSchedulerOperationElement* element) const;
    const TFairShareTreeJobSchedulerOperationSharedStatePtr& GetOperationSharedState(const TSchedulerOperationElement* element) const;

    //! Faster versions of |GetOperationState| and |GetOperationSharedState| which do not do an extra hashmap lookup and rely on tree indices instead.
    const TFairShareTreeJobSchedulerOperationStatePtr& GetEnabledOperationState(const TSchedulerOperationElement* element) const;
    const TFairShareTreeJobSchedulerOperationSharedStatePtr& GetEnabledOperationSharedState(const TSchedulerOperationElement* element) const;

private:
    // NB(eshcherbin): Enabled operations' states are also stored in static attributes to eliminate a hashmap lookup during scheduling.
    TFairShareTreeJobSchedulerOperationStateMap OperationIdToState_;
    TFairShareTreeJobSchedulerSharedOperationStateMap OperationIdToSharedState_;
    TAtomicIntrusivePtr<TDynamicAttributesListSnapshot> DynamicAttributesListSnapshot_;

    TDynamicAttributesListSnapshotPtr GetDynamicAttributesListSnapshot() const;
    void UpdateDynamicAttributesListSnapshot(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot);

    friend class TFairShareTreeJobScheduler;
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeSchedulingSnapshot);

////////////////////////////////////////////////////////////////////////////////

struct TJobSchedulerPostUpdateContext
{
    TSchedulerRootElement* RootElement;

    THashSet<int> SsdPriorityPreemptionMedia;
    TOperationElementsBySchedulingPriority SchedulableOperationsPerPriority;
    TStaticAttributesList StaticAttributesList;
    TFairShareTreeJobSchedulerOperationStateMap OperationIdToState;
    TFairShareTreeJobSchedulerSharedOperationStateMap OperationIdToSharedState;
    std::vector<TSchedulingTagFilter> KnownSchedulingTagFilters;
    TOperationCountsByPreemptionPriorityParameters OperationCountsByPreemptionPriorityParameters;
};

////////////////////////////////////////////////////////////////////////////////

struct IFairShareTreeJobSchedulerHost
    : public virtual TRefCounted
{
    //! Thread affinity: Control.
    virtual TFairShareTreeSnapshotPtr GetTreeSnapshot() const noexcept = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeJobScheduler
    : public TRefCounted
{
public:
    TFairShareTreeJobScheduler(
        TString treeId,
        NLogging::TLogger logger,
        TWeakPtr<IFairShareTreeJobSchedulerHost> host,
        IFairShareTreeHost* treeHost,
        ISchedulerStrategyHost* strategyHost,
        TFairShareStrategyTreeConfigPtr config,
        NProfiling::TProfiler profiler);

    //! Node management.
    void RegisterNode(NNodeTrackerClient::TNodeId nodeId);
    void UnregisterNode(NNodeTrackerClient::TNodeId nodeId);

    //! Process scheduling heartbeat.
    void ProcessSchedulingHeartbeat(const ISchedulingContextPtr& schedulingContext, const TFairShareTreeSnapshotPtr& treeSnapshot, bool skipScheduleJobs);

    //! Operation management.
    void RegisterOperation(const TSchedulerOperationElement* element);
    void UnregisterOperation(const TSchedulerOperationElement* element);

    void OnOperationMaterialized(const TSchedulerOperationElement* element);
    TError CheckOperationSchedulingInSeveralTreesAllowed(const TSchedulerOperationElement* element) const;

    void EnableOperation(const TSchedulerOperationElement* element) const;
    void DisableOperation(TSchedulerOperationElement* element, bool markAsNonAlive) const;

    void RegisterJobsFromRevivedOperation(TSchedulerOperationElement* element, std::vector<TJobPtr> jobs) const;
    void ProcessUpdatedJob(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TSchedulerOperationElement* element,
        TJobId jobId,
        const TJobResources& jobResources,
        const std::optional<TString>& jobDataCenter,
        const std::optional<TString>& jobInfinibandCluster,
        bool* shouldAbortJob) const;
    void ProcessFinishedJob(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TSchedulerOperationElement* element,
        TJobId jobId) const;

    //! Diagnostics.
    void BuildSchedulingAttributesStringForNode(NNodeTrackerClient::TNodeId nodeId, TDelimitedStringBuilderWrapper& delimitedBuilder) const;
    void BuildSchedulingAttributesForNode(NNodeTrackerClient::TNodeId nodeId, NYTree::TFluentMap fluent) const;
    void BuildSchedulingAttributesStringForOngoingJobs(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const std::vector<TJobPtr>& jobs,
        TInstant now,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const;

    static TError CheckOperationIsHung(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TSchedulerOperationElement* element,
        TInstant now,
        TInstant activationTime,
        TDuration safeTimeout,
        int minScheduleJobCallAttempts,
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
    TJobSchedulerPostUpdateContext CreatePostUpdateContext(TSchedulerRootElement* rootElement);
    void PostUpdate(
        TFairSharePostUpdateContext* fairSharePostUpdateContext,
        TJobSchedulerPostUpdateContext* postUpdateContext);
    TFairShareTreeSchedulingSnapshotPtr CreateSchedulingSnapshot(TJobSchedulerPostUpdateContext* postUpdateContext);

    void OnResourceUsageSnapshotUpdate(const TFairShareTreeSnapshotPtr& treeSnapshot, const TResourceUsageSnapshotPtr& resourceUsageSnapshot) const;

    //! Miscellaneous.
    void UpdateConfig(TFairShareStrategyTreeConfigPtr config);

    void BuildElementLoggingStringAttributes(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TSchedulerElement* element,
        TDelimitedStringBuilderWrapper& delimitedBuilder) const;

    void InitPersistentState(NYTree::INodePtr persistentState);
    NYTree::INodePtr BuildPersistentState() const;

    //! Testing.
    void OnJobStartedInTest(
        TSchedulerOperationElement* element,
        TJobId jobId,
        const TJobResourcesWithQuota& resourceUsage);
    void ProcessUpdatedJobInTest(
        TSchedulerOperationElement* element,
        TJobId jobId,
        const TJobResources& jobResources);
    EJobPreemptionStatus GetJobPreemptionStatusInTest(const TSchedulerOperationElement* element, TJobId jobId) const;

private:
    const TString TreeId_;
    const NLogging::TLogger Logger;
    // NB(eshcherbin): While tree host and strategy host are singletons (strategy and scheduler respectively), job scheduler host (tree)
    // can be outlived by some asynchronous actions. Therefore, we store it as a weak pointer rather than a raw pointer.
    const TWeakPtr<IFairShareTreeJobSchedulerHost> Host_;
    IFairShareTreeHost* const TreeHost_;
    ISchedulerStrategyHost* const StrategyHost_;

    TFairShareStrategyTreeConfigPtr Config_;

    NProfiling::TProfiler Profiler_;

    NConcurrency::TPeriodicExecutorPtr SchedulingSegmentsManagementExecutor_;

    TEnumIndexedVector<EJobSchedulingStage, TScheduleJobsStage> SchedulingStages_;

    TFairShareTreeJobSchedulerOperationStateMap OperationIdToState_;
    TFairShareTreeJobSchedulerSharedOperationStateMap OperationIdToSharedState_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, NodeIdToLastPreemptiveSchedulingTimeLock_);
    THashMap<NNodeTrackerClient::TNodeId, TCpuInstant> NodeIdToLastPreemptiveSchedulingTime_;

    NProfiling::TTimeCounter CumulativeScheduleJobsTime_;
    NProfiling::TEventTimer ScheduleJobsTime_;

    NProfiling::TEventTimer GracefulPreemptionTime_;

    NProfiling::TCounter ScheduleJobsDeadlineReachedCounter_;

    NProfiling::TBufferedProducerPtr OperationCountByPreemptionPriorityBufferedProducer_;

    std::atomic<TCpuInstant> LastSchedulingInformationLoggedTime_ = 0;

    TCachedJobPreemptionStatuses CachedJobPreemptionStatuses_;

    std::optional<THashSet<int>> SsdPriorityPreemptionMedia_;

    TSchedulingSegmentManager SchedulingSegmentManager_;

    // TODO(eshcherbin): Add generic data structure for state sharding.
    struct alignas(CacheLineSize) TNodeStateShard
    {
        TFairShareTreeJobSchedulerNodeStateMap NodeIdToState;
    };
    std::array<TNodeStateShard, MaxNodeShardCount> NodeStateShards_;

    // NB(eshcherbin): Used only as a value to store until the initialization deadline passes
    // and we start building up-to-date persistent state.
    TInstant SchedulingSegmentsInitializationDeadline_;
    TPersistentFairShareTreeJobSchedulerStatePtr InitialPersistentState_ = New<TPersistentFairShareTreeJobSchedulerState>();
    TPersistentFairShareTreeJobSchedulerStatePtr PersistentState_;

    TPersistentNodeSchedulingSegmentStateMap InitialPersistentSchedulingSegmentNodeStates_;
    TPersistentOperationSchedulingSegmentStateMap InitialPersistentSchedulingSegmentOperationStates_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    //! Initialization.
    void InitSchedulingStages();

    //! Process node heartbeat, including job scheduling.
    TRunningJobStatistics ComputeRunningJobStatistics(const ISchedulingContextPtr& schedulingContext, const TFairShareTreeSnapshotPtr& treeSnapshot);

    void PreemptJobsGracefully(const ISchedulingContextPtr& schedulingContext, const TFairShareTreeSnapshotPtr& treeSnapshot) const;
    void ScheduleJobs(const ISchedulingContextPtr& schedulingContext, ESchedulingSegment nodeSchedulingSegment, const TFairShareTreeSnapshotPtr& treeSnapshot);

    TPreemptiveScheduleJobsStageList BuildPreemptiveSchedulingStageList(TScheduleJobsContext* context);

    void ScheduleJobsWithoutPreemption(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
        const std::optional<TNonOwningOperationElementList>& consideredOperations,
        const std::optional<TJobResources>& customMinSpareJobResources,
        TCpuInstant startTime);
    void ScheduleJobsPackingFallback(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
        TCpuInstant startTime);
    void DoScheduleJobsWithoutPreemption(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
        const std::optional<TNonOwningOperationElementList>& consideredOperations,
        const std::optional<TJobResources>& customMinSpareJobResources,
        TCpuInstant startTime,
        bool ignorePacking,
        bool oneJobOnly);

    void ScheduleJobsWithPreemption(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
        TCpuInstant startTime,
        EOperationPreemptionPriority targetOperationPreemptionPriority,
        EJobPreemptionLevel minJobPreemptionLevel,
        bool forcePreemptionAttempt);

    const TFairShareTreeJobSchedulerOperationStatePtr& GetOperationState(TOperationId operationId) const;
    const TFairShareTreeJobSchedulerOperationSharedStatePtr& GetOperationSharedState(TOperationId operationId) const;

    //! Node management.
    std::optional<TPersistentNodeSchedulingSegmentState> FindInitialNodePersistentState(NNodeTrackerClient::TNodeId nodeId);

    //! Operation management.
    std::optional<TPersistentOperationSchedulingSegmentState> FindInitialOperationPersistentState(TOperationId operationId);

    //! Post update.
    void UpdateSsdPriorityPreemptionMedia();

    void InitializeStaticAttributes(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;
    void CollectSchedulableOperationsPerPriority(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;

    void PublishFairShareAndUpdatePreemptionAttributes(TSchedulerElement* element, TJobSchedulerPostUpdateContext* postUpdateContext) const;
    void PublishFairShareAndUpdatePreemptionAttributesAtCompositeElement(TSchedulerCompositeElement* element, TJobSchedulerPostUpdateContext* postUpdateContext) const;
    void PublishFairShareAndUpdatePreemptionAttributesAtOperation(TSchedulerOperationElement* element, TJobSchedulerPostUpdateContext* postUpdateContext) const;

    void ProcessUpdatedStarvationStatuses(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext);
    void UpdateCachedJobPreemptionStatuses(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext);
    void ComputeDynamicAttributesAtUpdateRecursively(TSchedulerElement* element, TDynamicAttributesManager* dynamicAttributesManager) const;
    void BuildSchedulableIndices(TDynamicAttributesManager* dynamicAttributesManager, TJobSchedulerPostUpdateContext* context) const;
    void CollectKnownSchedulingTagFilters(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;
    void UpdateSsdNodeSchedulingAttributes(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;
    void CountOperationsByPreemptionPriority(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;

    //! Miscellaneous
    const TFairShareTreeJobSchedulerNodeState* FindNodeState(NNodeTrackerClient::TNodeId nodeId) const;
    TFairShareTreeJobSchedulerNodeState* FindNodeState(NNodeTrackerClient::TNodeId nodeId);

    TFairShareTreeJobSchedulerOperationStateMap GetOperationStateMapSnapshot() const;
    TFairShareTreeJobSchedulerNodeStateMap GetNodeStateMapSnapshot() const;

    void ApplyOperationSchedulingSegmentsChanges(const TFairShareTreeJobSchedulerOperationStateMap& changedOperationStates);
    void ApplyNodeSchedulingSegmentsChanges(const TSetNodeSchedulingSegmentOptionsList& movedNodes);

    void ManageSchedulingSegments();
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeJobScheduler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
