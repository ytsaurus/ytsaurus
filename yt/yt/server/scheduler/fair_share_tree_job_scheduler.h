#pragma once

#include "private.h"
#include "fair_share_tree_element.h"
#include "fair_share_tree_snapshot.h"
#include "fields_filter.h"

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/core/misc/atomic_ptr.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

constexpr int SchedulingIndexProfilingRangeCount = 12;
constexpr int InvalidChildHeapIndex = -1;
constexpr int EmptySchedulingTagFilterIndex = -1;

////////////////////////////////////////////////////////////////////////////////

using TJobResourcesMap = THashMap<int, TJobResources>;
using TNonOwningJobSet = THashSet<TJob*>;

////////////////////////////////////////////////////////////////////////////////

struct TStaticAttributes
{
    int SchedulingIndex = UndefinedSchedulingIndex;
    int SchedulingTagFilterIndex = EmptySchedulingTagFilterIndex;
    bool EffectiveAggressivePreemptionAllowed = true;
    // Used for checking if operation is hung.
    bool IsAliveAtUpdate = false;

    // Only for operations.
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

struct TDynamicAttributes
{
    double SatisfactionRatio = 0.0;
    bool Active = false;
    bool Alive = true;
    TSchedulerOperationElement* BestLeafDescendant = nullptr;
    TJobResources ResourceUsage;
    NProfiling::TCpuInstant ResourceUsageUpdateTime;

    int HeapIndex = InvalidChildHeapIndex;
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
    static constexpr bool EnableHazard = true;

    explicit TDynamicAttributesListSnapshot(TDynamicAttributesList value);

    const TDynamicAttributesList Value;
};

using TDynamicAttributesListSnapshotPtr = TIntrusivePtr<TDynamicAttributesListSnapshot>;

////////////////////////////////////////////////////////////////////////////////

class TChildHeap
{
public:
    TChildHeap(
        const TSchedulerCompositeElement* owningElement,
        TDynamicAttributesList* dynamicAttributesList);
    TSchedulerElement* GetTop() const;
    void Update(const TSchedulerElement* child);

    // For testing purposes.
    const std::vector<TSchedulerElement*>& GetHeap() const;

private:
    const TSchedulerCompositeElement* OwningElement_;
    TDynamicAttributesList* const DynamicAttributesList_;
    const ESchedulingMode Mode_;

    std::vector<TSchedulerElement*> ChildHeap_;

    bool Comparator(const TSchedulerElement* lhs, const TSchedulerElement* rhs) const;
};

using TChildHeapMap = THashMap<int, TChildHeap>;

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

    void InitializeAttributesAtCompositeElement(TSchedulerCompositeElement* element, bool useChildHeap = true);
    void InitializeAttributesAtOperation(TSchedulerOperationElement* element, bool isActive = true);

    void ActivateOperation(TSchedulerOperationElement* element);
    void DeactivateOperation(TSchedulerOperationElement* element);

    void UpdateOperationResourceUsage(TSchedulerOperationElement* element, NProfiling::TCpuInstant now);

    void Clear();

    //! Diagnostics.
    int GetCompositeElementDeactivationCount() const;

    //! Testing.
    const TChildHeapMap& GetChildHeapMapInTest() const;

private:
    const TFairShareTreeSchedulingSnapshotPtr SchedulingSnapshot_;
    TDynamicAttributesList AttributesList_;
    TChildHeapMap ChildHeapMap_;

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

    void UpdateChildInHeap(const TSchedulerCompositeElement* parent, const TSchedulerElement* child);

    TSchedulerElement* GetBestActiveChild(TSchedulerCompositeElement* element) const;
    TSchedulerElement* GetBestActiveChildFifo(TSchedulerCompositeElement* element) const;
    TSchedulerElement* GetBestActiveChildFairShare(TSchedulerCompositeElement* element) const;

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

struct TFairShareScheduleJobResult
{
    bool Finished = true;
    bool Scheduled = false;
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
        bool enableSchedulingInfoLogging,
        ISchedulerStrategyHost* strategyHost,
        const NLogging::TLogger& logger);

    void PrepareForScheduling();
    void PrescheduleJob(EOperationPreemptionPriority targetOperationPreemptionPriority = EOperationPreemptionPriority::None);
    TFairShareScheduleJobResult ScheduleJob(bool ignorePacking);
    // NB(eshcherbin): Public for testing purposes.
    TFairShareScheduleJobResult ScheduleJob(TSchedulerElement* element, bool ignorePacking);

    void CountOperationsByPreemptionPriority();
    int GetOperationWithPreemptionPriorityCount(
        EOperationPreemptionPriority priority,
        EOperationPreemptionPriorityScope scope = EOperationPreemptionPriorityScope::OperationAndAncestors) const;

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
    const TChildHeapMap& GetChildHeapMapInTest() const;

private:
    const TFairShareTreeSnapshotPtr TreeSnapshot_;
    const std::vector<TSchedulingTagFilter> KnownSchedulingTagFilters_;
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

    using TOperationCountByPreemptionPriority = TEnumIndexedVector<EOperationPreemptionPriority, int>;
    TEnumIndexedVector<EOperationPreemptionPriorityScope, TOperationCountByPreemptionPriority> OperationCountByPreemptionPriority_;

    std::vector<bool> CanSchedule_;

    std::vector<TSchedulerOperationElementPtr> BadPackingOperations_;

    // Populated only for pools.
    TJobResourcesMap LocalUnconditionalUsageDiscountMap_;

    //! Common element methods.
    const TStaticAttributes& StaticAttributesOf(const TSchedulerElement* element) const;
    bool IsActive(const TSchedulerElement* element) const;
    // Returns resource usage observed in current heartbeat.
    TJobResources GetCurrentResourceUsage(const TSchedulerElement* element) const;

    TJobResources GetHierarchicalAvailableResources(const TSchedulerElement* element) const;
    TJobResources GetLocalAvailableResourceLimits(const TSchedulerElement* element) const;
    TJobResources GetLocalUnconditionalUsageDiscount(const TSchedulerElement* element) const;

    void PrescheduleJob(TSchedulerElement* element, EOperationPreemptionPriority targetOperationPreemptionPriority);
    void PrescheduleJobAtCompositeElement(TSchedulerCompositeElement* element, EOperationPreemptionPriority targetOperationPreemptionPriority);
    void PrescheduleJobAtOperation(TSchedulerOperationElement* element, EOperationPreemptionPriority targetOperationPreemptionPriority);

    TFairShareScheduleJobResult ScheduleJobAtCompositeElement(TSchedulerCompositeElement* element, bool ignorePacking);
    TFairShareScheduleJobResult ScheduleJobAtOperation(TSchedulerOperationElement* element, bool ignorePacking);

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

using TOperationIdToJobSchedulerSharedState = THashMap<TOperationId, TFairShareTreeJobSchedulerOperationSharedStatePtr>;

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeSchedulingSnapshot
    : public TRefCounted
{
public:
    DEFINE_BYREF_RO_PROPERTY(TStaticAttributesList, StaticAttributesList);
    DEFINE_BYREF_RO_PROPERTY(THashSet<int>, SsdPriorityPreemptionMedia);
    DEFINE_BYREF_RO_PROPERTY(TCachedJobPreemptionStatuses, CachedJobPreemptionStatuses);
    DEFINE_BYREF_RO_PROPERTY(TTreeSchedulingSegmentsState, SchedulingSegmentsState);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TSchedulingTagFilter>, KnownSchedulingTagFilters);

public:
    TFairShareTreeSchedulingSnapshot(
        TStaticAttributesList staticAttributesList,
        THashSet<int> ssdPriorityPreemptionMedia,
        TCachedJobPreemptionStatuses cachedJobPreemptionStatuses,
        TTreeSchedulingSegmentsState schedulingSegmentsState,
        std::vector<TSchedulingTagFilter> knownSchedulingTagFilters,
        TOperationIdToJobSchedulerSharedState operationIdToSharedState);

    const TFairShareTreeJobSchedulerOperationSharedStatePtr& GetOperationSharedState(const TSchedulerOperationElement* element) const;
    //! Faster version of |GetOperationSharedState| which does not do an extra hashmap lookup and relies on tree indices instead.
    const TFairShareTreeJobSchedulerOperationSharedStatePtr& GetEnabledOperationSharedState(const TSchedulerOperationElement* element) const;

private:
    // NB(eshcherbin): Enabled operations' shared states are also stored in static attributes to eliminate a hashmap lookup during scheduling.
    TOperationIdToJobSchedulerSharedState OperationIdToSharedState_;
    // TODO(eshcherbin): Change to new TAtomicIntrusivePtr.
    TAtomicPtr<TDynamicAttributesListSnapshot> DynamicAttributesListSnapshot_;

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

    TManageTreeSchedulingSegmentsContext ManageSchedulingSegmentsContext;
    TStaticAttributesList StaticAttributesList;
    TOperationIdToJobSchedulerSharedState OperationIdToSharedState;
    std::vector<TSchedulingTagFilter> KnownSchedulingTagFilters;
};

////////////////////////////////////////////////////////////////////////////////

class TFairShareTreeJobScheduler
    : public TRefCounted
{
public:
    TFairShareTreeJobScheduler(
        TString treeId,
        NLogging::TLogger logger,
        ISchedulerStrategyHost* strategyHost,
        TFairShareStrategyTreeConfigPtr config,
        NProfiling::TProfiler profiler);

    //! Schedule jobs.
    void ScheduleJobs(const ISchedulingContextPtr& schedulingContext, const TFairShareTreeSnapshotPtr& treeSnapshot);
    void PreemptJobsGracefully(const ISchedulingContextPtr& schedulingContext, const TFairShareTreeSnapshotPtr& treeSnapshot) const;

    //! Operation management.
    void RegisterOperation(const TSchedulerOperationElement* element);
    void UnregisterOperation(const TSchedulerOperationElement* element);

    void EnableOperation(const TSchedulerOperationElement* element) const;
    void DisableOperation(TSchedulerOperationElement* element, bool markAsNonAlive) const;

    void RegisterJobsFromRevivedOperation(TSchedulerOperationElement* element, const std::vector<TJobPtr>& jobs) const;
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

    std::pair<TSetNodeSchedulingSegmentOptionsList, TError> ManageNodeSchedulingSegments(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const THashSet<NNodeTrackerClient::TNodeId>& treeNodeIds);

private:
    const TString TreeId_;
    const NLogging::TLogger Logger;
    ISchedulerStrategyHost* const StrategyHost_;

    TFairShareStrategyTreeConfigPtr Config_;

    NProfiling::TProfiler Profiler_;

    TEnumIndexedVector<EJobSchedulingStage, TScheduleJobsStage> SchedulingStages_;

    TOperationIdToJobSchedulerSharedState OperationIdToSharedState_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, NodeIdToLastPreemptiveSchedulingTimeLock_);
    THashMap<NNodeTrackerClient::TNodeId, TCpuInstant> NodeIdToLastPreemptiveSchedulingTime_;

    NProfiling::TTimeCounter CumulativeScheduleJobsTime_;

    NProfiling::TCounter ScheduleJobsDeadlineReachedCounter_;

    using TSummaryByPreemptionPriority = TEnumIndexedVector<EOperationPreemptionPriority, NProfiling::TSummary>;
    TEnumIndexedVector<EOperationPreemptionPriorityScope, TSummaryByPreemptionPriority> OperationCountByPreemptionPrioritySummary_;

    std::atomic<TCpuInstant> LastSchedulingInformationLoggedTime_ = 0;

    TCachedJobPreemptionStatuses CachedJobPreemptionStatuses_;

    std::optional<THashSet<int>> SsdPriorityPreemptionMedia_;

    TNodeSchedulingSegmentManager NodeSchedulingSegmentManager_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void InitSchedulingStages();
    TPreemptiveScheduleJobsStageList BuildPreemptiveSchedulingStageList(TScheduleJobsContext* context);

    void ScheduleJobsWithoutPreemption(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
        TCpuInstant startTime);
    void ScheduleJobsPackingFallback(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
        TCpuInstant startTime);
    void DoScheduleJobsWithoutPreemption(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        TScheduleJobsContext* context,
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

    const TFairShareTreeJobSchedulerOperationSharedStatePtr& GetOperationSharedState(TOperationId operationId) const;

    void UpdateSsdPriorityPreemptionMedia();

    void InitializeStaticAttributes(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;

    void PublishFairShareAndUpdatePreemptionAttributes(TSchedulerElement* element, TJobSchedulerPostUpdateContext* postUpdateContext) const;
    void PublishFairShareAndUpdatePreemptionAttributesAtCompositeElement(TSchedulerCompositeElement* element, TJobSchedulerPostUpdateContext* postUpdateContext) const;
    void PublishFairShareAndUpdatePreemptionAttributesAtOperation(TSchedulerOperationElement* element, TJobSchedulerPostUpdateContext* postUpdateContext) const;

    void ProcessUpdatedStarvationStatuses(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext);
    void UpdateCachedJobPreemptionStatuses(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext);
    void ComputeDynamicAttributesAtUpdateRecursively(TSchedulerElement* element, TDynamicAttributesManager* dynamicAttributesManager) const;
    void BuildSchedulableIndices(TDynamicAttributesManager* dynamicAttributesManager, TJobSchedulerPostUpdateContext* context) const;
    void ManageSchedulingSegments(TFairSharePostUpdateContext* fairSharePostUpdateContext, TManageTreeSchedulingSegmentsContext* manageSegmentsContext) const;
    void CollectKnownSchedulingTagFilters(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;
    void UpdateSsdNodeSchedulingAttributes(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;

    static std::optional<bool> IsAggressivePreemptionAllowed(const TSchedulerElement* element);
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeJobScheduler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
