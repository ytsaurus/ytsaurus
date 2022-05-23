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

// TODO(eshcherbin): Think about naming of TStaticAttributes and TDynamicAttributes?
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
    // TODO(eshcherbin): Change to IsAlive.
    bool IsNotAlive = false;
    TSchedulerOperationElement* BestLeafDescendant = nullptr;
    TJobResources ResourceUsage;
    NProfiling::TCpuInstant ResourceUsageUpdateTime;

    int HeapIndex = InvalidChildHeapIndex;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(eshcherbin): Incorporate child heap map into TDynamicAttributesList.
class TChildHeapMap;

class TDynamicAttributesList
{
public:
    explicit TDynamicAttributesList(int size = 0);

    TDynamicAttributes& AttributesOf(const TSchedulerElement* element);
    const TDynamicAttributes& AttributesOf(const TSchedulerElement* element) const;

    void UpdateAttributes(
        TSchedulerElement* element,
        const TFairShareTreeSchedulingSnapshotPtr& schedulingSnapshot,
        bool checkLiveness,
        TChildHeapMap* childHeapMap);

    void DeactivateAll();

    void InitializeResourceUsage(
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot,
        NProfiling::TCpuInstant now);
    void ActualizeResourceUsageOfOperation(
        TSchedulerOperationElement* element,
        const TFairShareTreeJobSchedulerOperationSharedStatePtr& operationSharedState);

private:
    std::vector<TDynamicAttributes> Value_;

    void UpdateAttributesAtCompositeElement(
        TSchedulerCompositeElement* element,
        const TFairShareTreeSchedulingSnapshotPtr& schedulingSnapshot,
        bool checkLiveness,
        TChildHeapMap* childHeapMap);
    void UpdateAttributesAtOperation(
        TSchedulerOperationElement* element,
        const TFairShareTreeSchedulingSnapshotPtr& schedulingSnapshot,
        bool checkLiveness,
        TChildHeapMap* childHeapMap);

    TSchedulerElement* GetBestActiveChild(TSchedulerCompositeElement* element, TChildHeapMap* childHeapMap) const;
    TSchedulerElement* GetBestActiveChildFifo(TSchedulerCompositeElement* element) const;
    TSchedulerElement* GetBestActiveChildFairShare(TSchedulerCompositeElement* element) const;

    TJobResources FillResourceUsage(
        TSchedulerElement* element,
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot);
    TJobResources FillResourceUsageAtCompositeElement(
        TSchedulerCompositeElement* element,
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot);
    TJobResources FillResourceUsageAtOperation(
        TSchedulerOperationElement* element,
        const TFairShareTreeSnapshotPtr& treeSnapshot,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot);
};

////////////////////////////////////////////////////////////////////////////////

struct TDynamicAttributesListSnapshot final
{
    static constexpr bool EnableHazard = true;

    TDynamicAttributesList Value;
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
    void Update(TSchedulerElement* child);

    // For testing purposes.
    const std::vector<TSchedulerElement*>& GetHeap() const;

private:
    const TSchedulerCompositeElement* OwningElement_;
    TDynamicAttributesList* const DynamicAttributesList_;
    const ESchedulingMode Mode_;

    std::vector<TSchedulerElement*> ChildHeap_;

    bool Comparator(const TSchedulerElement* lhs, const TSchedulerElement* rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

class TChildHeapMap final
    : public THashMap<int, TChildHeap>
{
public:
    void UpdateChild(TSchedulerCompositeElement* parent, TSchedulerElement* child);
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
    EJobPreemptionStatus PreemptionStatus = EJobPreemptionStatus::NonPreemptable;
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
    DEFINE_BYREF_RW_PROPERTY(TJobWithPreemptionInfoSetMap, ConditionallyPreemptableJobSetMap);
    DEFINE_BYREF_RO_PROPERTY(TChildHeapMap, ChildHeapMap);

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
    int GetOperationWithPreemptionPriorityCount(EOperationPreemptionPriority priority) const;

    void AnalyzePreemptableJobs(
        EOperationPreemptionPriority targetOperationPreemptionPriority,
        EJobPreemptionLevel minJobPreemptionLevel,
        std::vector<TJobWithPreemptionInfo>* unconditionallyPreemptableJobs,
        TNonOwningJobSet* forcefullyPreemptableJobs);
    void PreemptJobsAfterScheduling(
        EOperationPreemptionPriority targetOperationPreemptionPriority,
        std::vector<TJobWithPreemptionInfo> preemptableJobs,
        const TNonOwningJobSet& forcefullyPreemptableJobs,
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
    const TJobWithPreemptionInfoSet& GetConditionallyPreemptableJobsInPool(const TSchedulerCompositeElement* element) const;

    TDynamicAttributes& DynamicAttributesOf(const TSchedulerElement* element);
    const TDynamicAttributes& DynamicAttributesOf(const TSchedulerElement* element) const;

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
    TDynamicAttributesList DynamicAttributesList_;

    TEnumIndexedVector<EOperationPreemptionPriority, int> OperationCountByPreemptionPriority_;

    std::vector<bool> CanSchedule_;

    std::vector<TSchedulerOperationElementPtr> BadPackingOperations_;

    // Populated only for pools.
    TJobResourcesMap LocalUnconditionalUsageDiscountMap_;

    //! Common element methods.
    const TStaticAttributes& StaticAttributesOf(const TSchedulerElement* element) const;
    bool IsActive(const TSchedulerElement* element) const;
    // Returns resource usage observed in current heartbeat.
    TJobResources GetCurrentResourceUsage(const TSchedulerElement* element) const;

    void UpdateDynamicAttributes(TSchedulerElement* element, bool checkLiveness);

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
    void InitializeChildHeap(const TSchedulerCompositeElement* element);

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

    EOperationPreemptionPriority GetOperationPreemptionPriority(const TSchedulerOperationElement* operationElement) const;

    void CheckForDeactivation(TSchedulerOperationElement* element, EOperationPreemptionPriority operationPreemptionPriority);
    void ActivateOperation(TSchedulerOperationElement* element);
    void DeactivateOperation(TSchedulerOperationElement* element, EDeactivationReason reason);
    void OnOperationDeactivated(
        TSchedulerOperationElement* element,
        EDeactivationReason reason,
        bool considerInOperationCounter = true);

    std::optional<EDeactivationReason> CheckBlocked(const TSchedulerOperationElement* element) const;

    bool IsSchedulingSegmentCompatibleWithNode(const TSchedulerOperationElement* element) const;

    bool IsUsageOutdated(TSchedulerOperationElement* element) const;
    void UpdateCurrentResourceUsage(TSchedulerOperationElement* element);

    bool HasJobsSatisfyingResourceLimits(const TSchedulerOperationElement* element) const;

    void UpdateAncestorsDynamicAttributes(
        TSchedulerOperationElement* element,
        const TJobResources& resourceUsageDelta,
        bool checkAncestorsActiveness = true);

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
    EJobPreemptionLevel MinJobPreemptionLevel = EJobPreemptionLevel::Preemptable;
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
    TAtomicPtr<TDynamicAttributesListSnapshot> DynamicAttributesListSnapshot_;

    TDynamicAttributesListSnapshotPtr GetDynamicAttributesListSnapshot() const;
    void UpdateDynamicAttributesSnapshot(
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

    void OnOperationStarvationStatusChanged(
        TOperationId operationId,
        EStarvationStatus oldStatus,
        EStarvationStatus newStatus) const;

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

    struct TSchedulingTagFilterEntry
    {
        int Index;
        int Count;
    };
    THashMap<TSchedulingTagFilter, TSchedulingTagFilterEntry> SchedulingTagFilterToIndexAndCount_;

    NProfiling::TTimeCounter CumulativeScheduleJobsTime_;

    NProfiling::TCounter ScheduleJobsDeadlineReachedCounter_;

    std::atomic<TCpuInstant> LastSchedulingInformationLoggedTime_ = 0;

    TCachedJobPreemptionStatuses CachedJobPreemptionStatuses_;

    std::optional<THashSet<int>> SsdPriorityPreemptionMedia_;

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

    void UpdateCachedJobPreemptionStatuses(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext);
    void ComputeDynamicAttributesAtUpdateRecursively(TSchedulerElement* element, TDynamicAttributesList* dynamicAttributesList, TChildHeapMap* childHeapMap) const;
    void BuildSchedulableIndices(TDynamicAttributesList* dynamicAttributesList, TChildHeapMap* childHeapMap, TJobSchedulerPostUpdateContext* context) const;
    void ManageSchedulingSegments(TFairSharePostUpdateContext* fairSharePostUpdateContext, TManageTreeSchedulingSegmentsContext* manageSegmentsContext) const;
    void CollectKnownSchedulingTagFilters(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;
    void UpdateSsdNodeSchedulingAttributes(TFairSharePostUpdateContext* fairSharePostUpdateContext, TJobSchedulerPostUpdateContext* postUpdateContext) const;

    static std::optional<bool> IsAggressivePreemptionAllowed(const TSchedulerElement* element);
};

DEFINE_REFCOUNTED_TYPE(TFairShareTreeJobScheduler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
