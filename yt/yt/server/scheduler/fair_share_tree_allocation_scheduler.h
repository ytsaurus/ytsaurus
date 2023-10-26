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
using TNonOwningJobSet = THashSet<TJob*>;

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

struct TSchedulingStageProfilingCounters
{
    TSchedulingStageProfilingCounters() = default;
    explicit TSchedulingStageProfilingCounters(const NProfiling::TProfiler& profiler);

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

    std::array<NProfiling::TCounter, MaxProfiledSchedulingStageAttemptIndex + 1> StageAttemptCount;

    NProfiling::TSummary ActiveTreeSize;
    NProfiling::TSummary ActiveOperationCount;
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
    : public TRefCounted
{
public:
    DEFINE_BYREF_RO_PROPERTY(ISchedulingContextPtr, SchedulingContext);
    DEFINE_BYREF_RO_PROPERTY(TFairShareTreeSnapshotPtr, TreeSnapshot);
    DEFINE_BYVAL_RO_PROPERTY(bool, SsdPriorityPreemptionEnabled);

    DEFINE_BYREF_RW_PROPERTY(TScheduleJobsStatistics, SchedulingStatistics);

    // NB(eshcherbin): The following properties are public for testing purposes.
    DEFINE_BYREF_RW_PROPERTY(TJobWithPreemptionInfoSetMap, ConditionallyPreemptibleJobSetMap);

public:
    TScheduleJobsContext(
        ISchedulingContextPtr schedulingContext,
        TFairShareTreeSnapshotPtr treeSnapshot,
        const TFairShareTreeJobSchedulerNodeState* nodeState,
        bool schedulingInfoLoggingEnabled,
        ISchedulerStrategyHost* strategyHost,
        const NProfiling::TCounter& scheduleJobsDeadlineReachedCounter,
        const NLogging::TLogger& logger);

    void PrepareForScheduling();
    //! Filters schedulable elements that will be considered for schedulaing, and initializes their dynamic attributes.
    void PrescheduleJob(
        const std::optional<TNonOwningOperationElementList>& consideredSchedulableOperations = {},
        EOperationPreemptionPriority targetOperationPreemptionPriority = EOperationPreemptionPriority::None);

    bool ShouldContinueScheduling(const std::optional<TJobResources>& customMinSpareJobResources = {}) const;

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

    TNonOwningOperationElementList ExtractBadPackingOperations();

    void StartStage(
        EJobSchedulingStage stage,
        TSchedulingStageProfilingCounters* profilingCounters,
        int stageAttemptIndex = 0);
    void FinishStage();
    int GetStageMaxSchedulingIndex() const;
    bool GetStagePrescheduleExecuted() const;

    // NB(eshcherbin): The following methods are public for testing purposes.
    const TSchedulerElement* FindPreemptionBlockingAncestor(
        const TSchedulerOperationElement* element,
        EJobPreemptionLevel jobPreemptionLevel,
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
    const TCpuInstant SchedulingDeadline_;
    const ESchedulingSegment NodeSchedulingSegment_;
    const TOperationCountByPreemptionPriority OperationCountByPreemptionPriority_;
    const THashSet<int> SsdPriorityPreemptionMedia_;
    const bool SchedulingInfoLoggingEnabled_;
    const TDynamicAttributesListSnapshotPtr DynamicAttributesListSnapshot_;

    ISchedulerStrategyHost* const StrategyHost_;
    const NProfiling::TCounter ScheduleJobsDeadlineReachedCounter_;
    const NLogging::TLogger Logger;

    bool Initialized_ = false;

    struct TStageState
    {
        const EJobSchedulingStage Stage;
        TSchedulingStageProfilingCounters* const ProfilingCounters;
        const int StageAttemptIndex;

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
    void ReactivateBadPackingOperations();

    // Shared state methods.
    void RecordPackingHeartbeat(const TSchedulerOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot);
    bool IsJobKnown(const TSchedulerOperationElement* element, TJobId jobId) const;
    bool IsOperationEnabled(const TSchedulerOperationElement* element) const;
    void OnMinNeededResourcesUnsatisfied(
        const TSchedulerOperationElement* element,
        const TJobResources& availableResources,
        const TJobResources& minNeededResources) const;
    void UpdateOperationPreemptionStatusStatistics(const TSchedulerOperationElement* element, EOperationPreemptionStatus status) const;
    void IncrementOperationScheduleJobAttemptCount(const TSchedulerOperationElement* element) const;
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

DEFINE_REFCOUNTED_TYPE(TScheduleJobsContext)

////////////////////////////////////////////////////////////////////////////////

struct TRegularSchedulingParameters
{
    const std::optional<TNonOwningOperationElementList>& ConsideredOperations = {};
    const std::optional<TJobResources>& CustomMinSpareJobResources = {};
    bool IgnorePacking = false;
    bool OneJobOnly = false;
};

struct TPreemptiveSchedulingParameters
{
    EOperationPreemptionPriority TargetOperationPreemptionPriority = EOperationPreemptionPriority::None;
    EJobPreemptionLevel MinJobPreemptionLevel = EJobPreemptionLevel::Preemptible;
    bool ForcePreemptionAttempt = false;
};

using TPreemptiveStageWithParameters = std::pair<EJobSchedulingStage, TPreemptiveSchedulingParameters>;

static constexpr int MaxPreemptiveStageCount = TEnumTraits<EJobSchedulingStage>::GetDomainSize();
using TPreemptiveStageWithParametersList = TCompactVector<TPreemptiveStageWithParameters, MaxPreemptiveStageCount>;

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

    TEnumIndexedVector<EJobSchedulingStage, std::unique_ptr<TSchedulingStageProfilingCounters>> SchedulingStageProfilingCounters_;

    TFairShareTreeJobSchedulerOperationStateMap OperationIdToState_;
    TFairShareTreeJobSchedulerSharedOperationStateMap OperationIdToSharedState_;

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
        THashMap<NNodeTrackerClient::TNodeId, TCpuInstant> NodeIdToLastPreemptiveSchedulingTime;
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
    void InitSchedulingProfilingCounters();

    //! Process node heartbeat, including job scheduling.
    TRunningJobStatistics ComputeRunningJobStatistics(const ISchedulingContextPtr& schedulingContext, const TFairShareTreeSnapshotPtr& treeSnapshot);

    void PreemptJobsGracefully(const ISchedulingContextPtr& schedulingContext, const TFairShareTreeSnapshotPtr& treeSnapshot) const;
    void ScheduleJobs(TScheduleJobsContext* context);

    void DoRegularJobScheduling(TScheduleJobsContext* context);
    void DoPreemptiveJobScheduling(TScheduleJobsContext* context);

    TPreemptiveStageWithParametersList BuildPreemptiveSchedulingStageList(TScheduleJobsContext* context);

    void RunRegularSchedulingStage(const TRegularSchedulingParameters& parameters, TScheduleJobsContext* context);
    void RunPreemptiveSchedulingStage(const TPreemptiveSchedulingParameters& parameters, TScheduleJobsContext* context);

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

    void UpdateEffectiveRecursiveAttributes(const TSchedulerElement* element, TJobSchedulerPostUpdateContext* postUpdateContext);
    void UpdateEffectiveRecursiveAttributesAtCompositeElement(const TSchedulerCompositeElement* element, TJobSchedulerPostUpdateContext* postUpdateContext);
    void UpdateEffectiveRecursiveAttributesAtOperation(const TSchedulerOperationElement* element, TJobSchedulerPostUpdateContext* postUpdateContext);

    void ProcessUpdatedStarvationStatuses(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext);
    void UpdateCachedJobPreemptionStatuses(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext);
    void ComputeOperationSchedulingIndexes(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* context);
    void CollectKnownSchedulingTagFilters(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;
    void UpdateSsdNodeSchedulingAttributes(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;
    void CountOperationsByPreemptionPriority(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;

    void InitializeDynamicAttributesAtUpdateRecursively(
        TSchedulerElement* element,
        std::vector<TNonOwningElementList>* consideredSchedulableChildrenPerPool,
        TDynamicAttributesManager* dynamicAttributesManager) const;

    static void UpdateDynamicAttributesListSnapshot(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot);

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
