#pragma once

#include "fair_share_strategy_operation_controller.h"
#include "job.h"
#include "private.h"
#include "resource_tree.h"
#include "resource_tree_element.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "scheduling_segment_manager.h"
#include "fair_share_strategy_operation_controller.h"
#include "packing.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/job_metrics.h>
#include <yt/yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/yt/server/lib/scheduler/resource_metering.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/core/misc/historic_usage_aggregator.h>

#include <yt/yt/library/vector_hdrf/resource_vector.h>

#include <yt/yt/library/vector_hdrf/fair_share_update.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NScheduler {

using NVectorHdrf::TSchedulableAttributes;
using NVectorHdrf::TDetailedFairShare;
using NVectorHdrf::TIntegralResourcesState;

////////////////////////////////////////////////////////////////////////////////

static constexpr int UnassignedTreeIndex = -1;
static constexpr int UndefinedSlotIndex = -1;
static constexpr int EmptySchedulingTagFilterIndex = -1;
static constexpr int UndefinedSchedulingIndex = -1;
static constexpr int SchedulingIndexProfilingRangeCount = 12;

////////////////////////////////////////////////////////////////////////////////

static constexpr double InfiniteSatisfactionRatio = 1e+9;

////////////////////////////////////////////////////////////////////////////////

int SchedulingIndexToProfilingRangeIndex(int schedulingIndex);
TString FormatProfilingRangeIndex(int rangeIndex);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EStarvationStatus,
    (NonStarving)
    (Starving)
    (AggressivelyStarving)
);

////////////////////////////////////////////////////////////////////////////////

struct IFairShareTreeElementHost
    : public virtual TRefCounted
{
    virtual TResourceTree* GetResourceTree() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairShareTreeElementHost)

////////////////////////////////////////////////////////////////////////////////

//! Attributes that are kept between fair share updates.
struct TPersistentAttributes
{
    EStarvationStatus StarvationStatus;
    TInstant LastNonStarvingTime = TInstant::Now();
    std::optional<TInstant> BelowFairShareSince;
    THistoricUsageAggregator HistoricUsageAggregator;

    TResourceVector BestAllocationShare = TResourceVector::Ones();
    TInstant LastBestAllocationRatioUpdateTime;

    TIntegralResourcesState IntegralResourcesState;

    TJobResources AppliedResourceLimits = TJobResources::Infinite();

    TSchedulingSegmentModule SchedulingSegmentModule;
    std::optional<TInstant> FailingToScheduleAtModuleSince;

    void ResetOnElementEnabled();
};

////////////////////////////////////////////////////////////////////////////////

class TChildHeap;

static const int InvalidHeapIndex = -1;

struct TDynamicAttributes
{
    double SatisfactionRatio = 0.0;
    bool Active = false;
    TSchedulerOperationElement* BestLeafDescendant = nullptr;
    TJobResources ResourceUsage;
    NProfiling::TCpuInstant ResourceUsageUpdateTime;

    int HeapIndex = InvalidHeapIndex;
};

////////////////////////////////////////////////////////////////////////////////

struct TResourceUsageSnapshot final
{
    static constexpr bool EnableHazard = true;

    THashMap<TOperationId, TJobResources> OperationIdToResourceUsage;
    THashMap<TString, TJobResources> PoolToResourceUsage;
};

using TResourceUsageSnapshotPtr = TIntrusivePtr<TResourceUsageSnapshot>;

////////////////////////////////////////////////////////////////////////////////

class TDynamicAttributesList
{
public:
    explicit TDynamicAttributesList(int size = 0);

    TDynamicAttributes& AttributesOf(const TSchedulerElement* element);
    const TDynamicAttributes& AttributesOf(const TSchedulerElement* element) const;

    void DeactivateAll();

    void InitializeResourceUsage(
        TSchedulerRootElement* rootElement,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot,
        NProfiling::TCpuInstant now);

private:
    std::vector<TDynamicAttributes> Value_;
};

using TChildHeapMap = THashMap<int, TChildHeap>;
using TJobResourcesMap = THashMap<int, TJobResources>;
using TNonOwningJobSet = THashSet<TJob*>;

////////////////////////////////////////////////////////////////////////////////

struct TDynamicAttributesListSnapshot final
{
    static constexpr bool EnableHazard = true;

    TDynamicAttributesList Value;
};

using TDynamicAttributesListSnapshotPtr = TIntrusivePtr<TDynamicAttributesListSnapshot>;

////////////////////////////////////////////////////////////////////////////////

struct TFairSharePostUpdateContext
{
    const TInstant Now;

    TCachedJobPreemptionStatuses CachedJobPreemptionStatuses;

    TEnumIndexedVector<EUnschedulableReason, int> UnschedulableReasons;

    TNonOwningOperationElementMap EnabledOperationIdToElement;
    TNonOwningOperationElementMap DisabledOperationIdToElement;
    TNonOwningPoolElementMap PoolNameToElement;
};

////////////////////////////////////////////////////////////////////////////////

struct TResourceDistributionInfo
{
    TJobResources DistributedStrongGuaranteeResources;
    TJobResources DistributedResourceFlow;
    TJobResources DistributedBurstGuaranteeResources;
    TJobResources DistributedResources;
    TJobResources UndistributedResources;
    TJobResources UndistributedResourceFlow;
    TJobResources UndistributedBurstGuaranteeResources;
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
    TFairShareScheduleJobResult(bool finished, bool scheduled)
        : Finished(finished)
        , Scheduled(scheduled)
    { }

    bool Finished;
    bool Scheduled;
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
    TSchedulerOperationElementPtr OperationElement;

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

class TScheduleJobsContext
{
private:
    struct TStageState
    {
        explicit TStageState(TScheduleJobsStage* schedulingStage);

        TScheduleJobsStage* const SchedulingStage;

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

    DEFINE_BYREF_RW_PROPERTY(std::vector<bool>, CanSchedule);

    DEFINE_BYREF_RW_PROPERTY(TDynamicAttributesList, DynamicAttributesList);

    DEFINE_BYREF_RW_PROPERTY(TChildHeapMap, ChildHeapMap);
    //! Populated only for pools.
    DEFINE_BYREF_RW_PROPERTY(TJobResourcesMap, LocalUnconditionalUsageDiscountMap);
    DEFINE_BYREF_RW_PROPERTY(TJobWithPreemptionInfoSetMap, ConditionallyPreemptableJobSetMap);

    DEFINE_BYREF_RW_PROPERTY(TJobResources, CurrentConditionalDiscount);

    DEFINE_BYREF_RO_PROPERTY(ISchedulingContextPtr, SchedulingContext);

    using TOperationCountByPreemptionPriority = TEnumIndexedVector<EOperationPreemptionPriority, int>;
    DEFINE_BYREF_RW_PROPERTY(TOperationCountByPreemptionPriority, OperationCountByPreemptionPriority);

    DEFINE_BYREF_RW_PROPERTY(TScheduleJobsStatistics, SchedulingStatistics);

    DEFINE_BYREF_RW_PROPERTY(std::vector<TSchedulerOperationElementPtr>, BadPackingOperations);

    DEFINE_BYREF_RW_PROPERTY(std::optional<TStageState>, StageState);

    DEFINE_BYREF_RW_PROPERTY(TDynamicAttributesListSnapshotPtr, DynamicAttributesListSnapshot);

    DEFINE_BYVAL_RW_PROPERTY(bool, SsdPriorityPreemptionEnabled);
    DEFINE_BYREF_RW_PROPERTY(THashSet<int>, SsdPriorityPreemptionMedia)

public:
    TScheduleJobsContext(
        ISchedulingContextPtr schedulingContext,
        std::vector<TSchedulingTagFilter> registeredSchedulingTagFilters,
        bool enableSchedulingInfoLogging,
        const NLogging::TLogger& logger);

    EOperationPreemptionPriority GetOperationPreemptionPriority(const TSchedulerOperationElement* operationElement) const;
    void CountOperationsByPreemptionPriority(const TSchedulerRootElementPtr& rootElement);

    EJobPreemptionLevel GetJobPreemptionLevel(const TJobWithPreemptionInfo& jobWithPreemptionInfo) const;

    void PrepareForScheduling(const TSchedulerRootElementPtr& rootElement);
    void PrescheduleJob(
        const TSchedulerRootElementPtr& rootElement,
        EOperationPreemptionPriority targetOperationPreemptionPriority = EOperationPreemptionPriority::None);

    TDynamicAttributes& DynamicAttributesFor(const TSchedulerElement* element);
    const TDynamicAttributes& DynamicAttributesFor(const TSchedulerElement* element) const;

    void PrepareConditionalUsageDiscounts(const TSchedulerRootElementPtr& rootElement, EOperationPreemptionPriority operationPreemptionPriority);
    const TJobWithPreemptionInfoSet& GetConditionallyPreemptableJobsInPool(const TSchedulerCompositeElement* element) const;
    TJobResources GetLocalUnconditionalUsageDiscountFor(const TSchedulerElement* element) const;

    void StartStage(TScheduleJobsStage* schedulingStage);

    EJobSchedulingStage GetStageType();

    void ProfileAndLogStatisticsOfStage();

    void FinishStage();

private:
    const std::vector<TSchedulingTagFilter> RegisteredSchedulingTagFilters_;

    const bool EnableSchedulingInfoLogging_;

    const NLogging::TLogger Logger;

    bool Initialized_ = false;

    NProfiling::TWallTimer Timer_;

    void ProfileStageStatistics();

    void LogStageStatistics();

    bool IsEligibleForSsdPriorityPreemption(const THashSet<int>& diskRequestMedia) const;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerElementFixedState
{
public:
    // These fields are persistent between updates.
    DEFINE_BYREF_RW_PROPERTY(TPersistentAttributes, PersistentAttributes);

    // This field used as both in fair share update and in schedule jobs.
    DEFINE_BYVAL_RW_PROPERTY(int, SchedulingTagFilterIndex, EmptySchedulingTagFilterIndex);

    // These fields are set in post update and used in schedule jobs.
    DEFINE_BYVAL_RO_PROPERTY(double, EffectiveFairShareStarvationTolerance, 1.0);
    DEFINE_BYVAL_RO_PROPERTY(TDuration, EffectiveFairShareStarvationTimeout);

    DEFINE_BYVAL_RO_PROPERTY(bool, EffectiveAggressiveStarvationEnabled, false)
    DEFINE_BYVAL_RO_PROPERTY(bool, EffectiveAggressivePreemptionAllowed, true);

    DEFINE_BYVAL_RO_PROPERTY(TSchedulerElement*, LowestStarvingAncestor, nullptr);
    DEFINE_BYVAL_RO_PROPERTY(TSchedulerElement*, LowestAggressivelyStarvingAncestor, nullptr);

protected:
    TSchedulerElementFixedState(
        ISchedulerStrategyHost* strategyHost,
        IFairShareTreeElementHost* treeElementHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        TString treeId);

    ISchedulerStrategyHost* const StrategyHost_;
    IFairShareTreeElementHost* const TreeElementHost_;

    TFairShareStrategyTreeConfigPtr TreeConfig_;

    // These fields calculated in preupdate and used for update.
    TJobResources ResourceDemand_;
    TJobResources ResourceUsageAtUpdate_;
    TJobResources ResourceLimits_;
    TJobResources SchedulingTagFilterResourceLimits_;

    // These attributes are calculated during fair share update and further used in schedule jobs.
    NVectorHdrf::TSchedulableAttributes Attributes_;

    // Used everywhere.
    TSchedulerCompositeElement* Parent_ = nullptr;

    // Assigned in preupdate, used in fair share update.
    TJobResources TotalResourceLimits_;
    int PendingJobCount_ = 0;
    TInstant StartTime_;

    // Assigned in preupdate, used in schedule jobs.
    bool Tentative_ = false;
    bool HasSpecifiedResourceLimits_ = false;

    // Assigned in postupdate, used in schedule jobs.
    int TreeIndex_ = UnassignedTreeIndex;

    // Index of operation element in order of scheduling priorities.
    int SchedulingIndex_ = UndefinedSchedulingIndex;

    // Flag indicates that we can change fields of scheduler elements.
    bool Mutable_ = true;

    const TString TreeId_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerElement
    : public virtual NVectorHdrf::TElement
    , public TSchedulerElementFixedState
{
public:
    //! Common interface.
    virtual TSchedulerElementPtr Clone(TSchedulerCompositeElement* clonedParent) = 0;

    virtual TString GetTreeId() const;

    const NLogging::TLogger& GetLogger() const override;
    bool AreDetailedLogsEnabled() const override;

    virtual TString GetLoggingString() const;

    TSchedulerCompositeElement* GetMutableParent();
    const TSchedulerCompositeElement* GetParent() const;

    void InitAccumulatedResourceVolume(TResourceVolume resourceVolume);

    bool IsAlive() const;
    void SetNonAlive();

    EStarvationStatus GetStarvationStatus() const;

    TJobResources GetInstantResourceUsage() const;

    virtual std::optional<double> GetSpecifiedFairShareStarvationTolerance() const = 0;
    virtual std::optional<TDuration> GetSpecifiedFairShareStarvationTimeout() const = 0;

    virtual ESchedulableStatus GetStatus(bool atUpdate = true) const;

    virtual TJobResources GetSpecifiedStrongGuaranteeResources() const;
    virtual TResourceVector GetMaxShare() const = 0;

    double GetMaxShareRatio() const;
    TResourceVector GetFairShare() const;
    double GetResourceDominantUsageShareAtUpdate() const;
    double GetAccumulatedResourceRatioVolume() const;
    TResourceVolume GetAccumulatedResourceVolume() const;

    bool IsStrictlyDominatesNonBlocked(const TResourceVector& lhs, const TResourceVector& rhs) const;

    //! Trunk node interface.
    // Used for manipulations with SchedulingTagFilterIndex.
    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const;
    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config);

    //! Pre fair share update methods.
    // At this stage we prepare attributes that need to be computed in the control thread
    // in a thread-unsafe manner.
    virtual void PreUpdateBottomUp(NVectorHdrf::TFairShareUpdateContext* context);

    TJobResources GetSchedulingTagFilterResourceLimits() const;
    TJobResources GetTotalResourceLimits() const;
    TJobResources GetMaxShareResourceLimits() const;
    virtual TJobResources GetSpecifiedResourceLimits() const = 0;

    virtual void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const = 0;

    //! Fair share update methods that implements NVectorHdrf::TElement interface.
    const TJobResources& GetResourceDemand() const override;
    const TJobResources& GetResourceUsageAtUpdate() const override;
    const TJobResources& GetResourceLimits() const override;

    double GetWeight() const override;

    TSchedulableAttributes& Attributes() override;
    const TSchedulableAttributes& Attributes() const override;

    TElement* GetParentElement() const override;
    const NVectorHdrf::TJobResourcesConfig* GetStrongGuaranteeResourcesConfig() const override;

    TInstant GetStartTime() const;

    //! Post fair share update methods.
    virtual void UpdateStarvationAttributes(TInstant now, bool enablePoolStarvation);
    virtual void MarkImmutable();

    //! Schedule jobs interface.
    virtual void PrescheduleJob(TScheduleJobsContext* context, EOperationPreemptionPriority targetOperationPreemptionPriority) = 0;

    virtual void UpdateDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap,
        bool checkLiveness) = 0;

    virtual TFairShareScheduleJobResult ScheduleJob(TScheduleJobsContext* context, bool ignorePacking) = 0;

    virtual std::optional<bool> IsAggressiveStarvationEnabled() const = 0;
    virtual std::optional<bool> IsAggressivePreemptionAllowed() const = 0;
    virtual bool HasAggressivelyStarvingElements(TScheduleJobsContext* context) const = 0;

    virtual const TJobResources& FillResourceUsageInDynamicAttributes(
        TDynamicAttributesList* attributesList,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot) = 0;

    int GetTreeIndex() const;

    int GetSchedulingIndex() const;
    void SetSchedulingIndex(int index);

    bool IsActive(const TDynamicAttributesList& dynamicAttributesList) const;
    bool AreResourceLimitsViolated() const;

    virtual void PrepareConditionalUsageDiscounts(TScheduleJobsContext* context, EOperationPreemptionPriority operationPreemptionPriority) const = 0;

    //! Other methods based on tree snapshot.
    virtual void BuildResourceMetering(
        const std::optional<TMeteringKey>& parentKey,
        const THashMap<TString, TResourceVolume>& poolResourceUsages,
        TMeteringMap* meteringMap) const;

private:
    TResourceTreeElementPtr ResourceTreeElement_;

protected:
    NLogging::TLogger Logger;

    TSchedulerElement(
        ISchedulerStrategyHost* strategyHost,
        IFairShareTreeElementHost* treeElementHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        TString treeId,
        TString id,
        EResourceTreeElementKind elementKind,
        const NLogging::TLogger& logger);
    TSchedulerElement(
        const TSchedulerElement& other,
        TSchedulerCompositeElement* clonedParent);

    ISchedulerStrategyHost* GetHost() const;

    void SetOperationAlert(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout);

    TString GetLoggingAttributesString() const;

    //! Pre update methods.
    TJobResources ComputeSchedulingTagFilterResourceLimits() const;
    TJobResources ComputeResourceLimits() const;

    //! Post update methods.
    virtual void SetStarvationStatus(EStarvationStatus starvationStatus);
    virtual void CheckForStarvation(TInstant now) = 0;

    ESchedulableStatus GetStatusImpl(double defaultTolerance, bool atUpdate) const;
    void CheckForStarvationImpl(
        TDuration fairShareStarvationTimeout,
        TDuration fairShareAggressiveStarvationTimeout,
        TInstant now);

    TResourceVector GetResourceUsageShare() const;

    // Publishes fair share and updates preemptable job lists of operations.
    virtual void PublishFairShareAndUpdatePreemptionSettings() = 0;
    virtual void UpdatePreemptionAttributes();

    // This method reuses common code with schedule jobs logic to calculate dynamic attributes.
    virtual void UpdateSchedulableAttributesFromDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        TChildHeapMap* childHeapMap);

    virtual bool IsSchedulable() const = 0;
    virtual void BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context) = 0;

    // Enumerates elements of the tree using inorder traversal. Returns first unused index.
    virtual int EnumerateElements(int startIndex, bool isSchedulableValueFilter);
    void SetTreeIndex(int treeIndex);

    virtual void BuildElementMapping(TFairSharePostUpdateContext* context) = 0;

    // Manage operation segments.
    virtual void CollectOperationSchedulingSegmentContexts(
        THashMap<TOperationId, TOperationSchedulingSegmentContext>* operationContexts) const = 0;
    virtual void ApplyOperationSchedulingSegmentChanges(
        const THashMap<TOperationId, TOperationSchedulingSegmentContext>& operationContexts) = 0;

    //! Schedule jobs methods.
    bool CheckDemand(const TJobResources& delta);
    TJobResources GetLocalAvailableResourceLimits(const TScheduleJobsContext& context) const;

    double ComputeLocalSatisfactionRatio(const TJobResources& resourceUsage) const;
    bool IsResourceBlocked(EJobResourceType resource) const;
    bool AreAllResourcesBlocked() const;

    // Returns resource usage observed in current heartbeat.
    TJobResources GetCurrentResourceUsage(const TDynamicAttributesList& dynamicAttributesList) const;

    void IncreaseHierarchicalResourceUsage(const TJobResources& delta);

    virtual void DisableNonAliveElements() = 0;

    virtual void CountOperationsByPreemptionPriority(TScheduleJobsContext* context) const = 0;

private:
    // Update methods.
    virtual std::optional<double> GetSpecifiedWeight() const = 0;
    int GetPendingJobCount() const;

    friend class TSchedulerCompositeElement;
    friend class TSchedulerOperationElement;
    friend class TSchedulerPoolElement;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerElement)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerCompositeElementFixedState
{
public:
    // Used only in trunk version and profiling.
    DEFINE_BYREF_RW_PROPERTY(int, RunningOperationCount);
    DEFINE_BYREF_RW_PROPERTY(int, OperationCount);
    DEFINE_BYREF_RW_PROPERTY(std::list<TOperationId>, PendingOperationIds);

protected:
    // Used in fair share update.
    ESchedulingMode Mode_ = ESchedulingMode::Fifo;
    std::vector<EFifoSortParameter> FifoSortParameters_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerCompositeElement
    : public virtual NVectorHdrf::TCompositeElement
    , public TSchedulerElement
    , public TSchedulerCompositeElementFixedState
{
public:
    TSchedulerCompositeElement(
        ISchedulerStrategyHost* strategyHost,
        IFairShareTreeElementHost* treeElementHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        const TString& treeId,
        const TString& id,
        EResourceTreeElementKind elementKind,
        const NLogging::TLogger& logger);
    TSchedulerCompositeElement(
        const TSchedulerCompositeElement& other,
        TSchedulerCompositeElement* clonedParent);

    //! Common interface.
    void AddChild(TSchedulerElement* child, bool enabled = true);
    void EnableChild(const TSchedulerElementPtr& child);
    void DisableChild(const TSchedulerElementPtr& child);
    void RemoveChild(TSchedulerElement* child);
    bool IsEnabledChild(TSchedulerElement* child);

    void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;

    // For testing purposes.
    std::vector<TSchedulerElementPtr> GetEnabledChildren();
    std::vector<TSchedulerElementPtr> GetDisabledChildren();

    //! Trunk node interface.
    virtual int GetMaxOperationCount() const = 0;
    virtual int GetMaxRunningOperationCount() const = 0;
    int GetAvailableRunningOperationCount() const;

    virtual TPoolIntegralGuaranteesConfigPtr GetIntegralGuaranteesConfig() const = 0;

    void IncreaseOperationCount(int delta);
    void IncreaseRunningOperationCount(int delta);

    virtual bool IsExplicit() const;
    virtual bool IsDefaultConfigured() const = 0;
    virtual bool AreImmediateOperationsForbidden() const = 0;

    bool IsEmpty() const;

    // For diagnostics purposes.
    TResourceVolume GetIntegralPoolCapacity() const;

    // For diagnostics purposes.
    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const = 0;

    //! Pre fair share update methods.
    void PreUpdateBottomUp(NVectorHdrf::TFairShareUpdateContext* context) override;

    //! Fair share update methods that implements NVectorHdrf::TCompositeElement interface.
    TElement* GetChild(int index) final;
    const TElement* GetChild(int index) const final;
    int GetChildrenCount() const final;

    std::vector<TSchedulerOperationElement*> GetChildOperations() const;
    int GetChildOperationCount() const noexcept;

    ESchedulingMode GetMode() const final;
    bool HasHigherPriorityInFifoMode(const NVectorHdrf::TElement* lhs, const NVectorHdrf::TElement* rhs) const final;

    //! Post fair share update methods.
    void UpdateStarvationAttributes(TInstant now, bool enablePoolStarvation) override;
    void MarkImmutable() override;

    //! Schedule jobs related methods.
    void PrescheduleJob(TScheduleJobsContext* context, EOperationPreemptionPriority targetOperationPreemptionPriority) final;

    void UpdateDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap,
        bool checkLiveness) final;

    TFairShareScheduleJobResult ScheduleJob(TScheduleJobsContext* context, bool ignorePacking) final;

    const TJobResources& FillResourceUsageInDynamicAttributes(
        TDynamicAttributesList* attributesList,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot) final;

    bool HasAggressivelyStarvingElements(TScheduleJobsContext* context) const override;

    void PrepareConditionalUsageDiscounts(TScheduleJobsContext* context, EOperationPreemptionPriority operationPreemptionPriority) const override;

    void CountOperationsByPreemptionPriority(TScheduleJobsContext* context) const override;

    //! Other methods.
    virtual THashSet<TString> GetAllowedProfilingTags() const = 0;

protected:
    using TChildMap = THashMap<TSchedulerElementPtr, int>;
    using TChildList = std::vector<TSchedulerElementPtr>;

    // Supported in trunk version, used in fair share update.
    TChildMap EnabledChildToIndex_;
    TChildList EnabledChildren_;
    TChildList SortedEnabledChildren_;

    TChildMap DisabledChildToIndex_;
    TChildList DisabledChildren_;

    // Computed in fair share update and used in schedule jobs.
    TChildList SchedulableChildren_;

    static void AddChild(TChildMap* map, TChildList* list, const TSchedulerElementPtr& child);
    static void RemoveChild(TChildMap* map, TChildList* list, const TSchedulerElementPtr& child);
    static bool ContainsChild(const TChildMap& map, const TSchedulerElementPtr& child);

    //! Pre fair share update methods.
    void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const override;

    //! Fair share update methods.
    void DisableNonAliveElements() override;

    //! Post fair share update methods.
    bool IsSchedulable() const override;
    void BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context) override;

    int EnumerateElements(int startIndex, bool isSchedulableValueFilter) override;

    void PublishFairShareAndUpdatePreemptionSettings() override;

    void UpdateSchedulableAttributesFromDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        TChildHeapMap* childHeapMap) override;

    void CollectOperationSchedulingSegmentContexts(
        THashMap<TOperationId, TOperationSchedulingSegmentContext>* operationContexts) const override;
    void ApplyOperationSchedulingSegmentChanges(
        const THashMap<TOperationId, TOperationSchedulingSegmentContext>& operationContexts) override;

    void BuildElementMapping(TFairSharePostUpdateContext* context) override;

    // Used to implement GetWeight.
    virtual bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const = 0;
    virtual THistoricUsageAggregationParameters GetHistoricUsageAggregationParameters() const = 0;

    void UpdateChild(TChildHeapMap& childHeapMap, TSchedulerElement* child);

private:
    friend class TSchedulerElement;
    friend class TSchedulerOperationElement;
    friend class TSchedulerRootElement;
    friend class TChildHeap;

    bool HasHigherPriorityInFifoMode(const TSchedulerElement* lhs, const TSchedulerElement* rhs) const;

    void InitializeChildHeap(TScheduleJobsContext* context);

    TSchedulerElement* GetBestActiveChild(const TDynamicAttributesList& dynamicAttributesList, const TChildHeapMap& childHeapMap) const;
    TSchedulerElement* GetBestActiveChildFifo(const TDynamicAttributesList& dynamicAttributesList) const;
    TSchedulerElement* GetBestActiveChildFairShare(const TDynamicAttributesList& dynamicAttributesList) const;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerCompositeElement)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolElementFixedState
{
protected:
    explicit TSchedulerPoolElementFixedState(TString id);

    const TString Id_;

    // Used only in trunk node.
    bool DefaultConfigured_ = true;
    bool EphemeralInDefaultParentPool_ = false;
    std::optional<TString> UserName_;

    // Used in preupdate.
    TSchedulingTagFilter SchedulingTagFilter_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerPoolElement
    : public NVectorHdrf::TPool
    , public TSchedulerCompositeElement
    , public TSchedulerPoolElementFixedState
{
public:
    TSchedulerPoolElement(
        ISchedulerStrategyHost* strategyHost,
        IFairShareTreeElementHost* treeElementHost,
        const TString& id,
        TPoolConfigPtr config,
        bool defaultConfigured,
        TFairShareStrategyTreeConfigPtr treeConfig,
        const TString& treeId,
        const NLogging::TLogger& logger);
    TSchedulerPoolElement(
        const TSchedulerPoolElement& other,
        TSchedulerCompositeElement* clonedParent);

    //! Common interface.
    TSchedulerElementPtr Clone(TSchedulerCompositeElement* clonedParent) override;

    TString GetId() const override;

    void AttachParent(TSchedulerCompositeElement* newParent);
    void ChangeParent(TSchedulerCompositeElement* newParent);
    void DetachParent();

    ESchedulableStatus GetStatus(bool atUpdate = true) const override;

    // Used for diagnostics only.
    TResourceVector GetMaxShare() const override;

    //! Trunk node interface.
    TPoolConfigPtr GetConfig() const;
    void SetConfig(TPoolConfigPtr config);
    void SetDefaultConfig();

    void SetUserName(const std::optional<TString>& userName);
    const std::optional<TString>& GetUserName() const;

    int GetMaxOperationCount() const override;
    int GetMaxRunningOperationCount() const override;

    TPoolIntegralGuaranteesConfigPtr GetIntegralGuaranteesConfig() const override;

    void SetEphemeralInDefaultParentPool();
    bool IsEphemeralInDefaultParentPool() const;

    bool IsExplicit() const override;
    bool IsDefaultConfigured() const override;
    bool AreImmediateOperationsForbidden() const override;

    std::vector<EFifoSortParameter> GetFifoSortParameters() const override;

    // Used to manipulate with SchedulingTagFilterIndex.
    const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    //! Fair share update methods that implements NVectorHdrf::TPool interface.
    bool AreDetailedLogsEnabled() const final;
    const NVectorHdrf::TJobResourcesConfig* GetStrongGuaranteeResourcesConfig() const override;

    double GetSpecifiedBurstRatio() const override;
    double GetSpecifiedResourceFlowRatio() const override;

    EIntegralGuaranteeType GetIntegralGuaranteeType() const override;
    TResourceVector GetIntegralShareLimitForRelaxedPool() const override;
    bool CanAcceptFreeVolume() const override;
    bool ShouldDistributeFreeVolumeAmongChildren() const override;

    const TIntegralResourcesState& IntegralResourcesState() const override;
    TIntegralResourcesState& IntegralResourcesState() override;

    //! Post fair share update methods.
    std::optional<double> GetSpecifiedFairShareStarvationTolerance() const override;
    std::optional<TDuration> GetSpecifiedFairShareStarvationTimeout() const override;

    //! Schedule job methods.
    std::optional<bool> IsAggressiveStarvationEnabled() const override;
    std::optional<bool> IsAggressivePreemptionAllowed() const override;

    //! Other methods.
    void BuildResourceMetering(
        const std::optional<TMeteringKey>& parentKey,
        const THashMap<TString, TResourceVolume>& poolResourceUsages,
        TMeteringMap* meteringMap) const override;

    THashSet<TString> GetAllowedProfilingTags() const override;

    bool IsFairShareTruncationInFifoPoolEnabled() const override;

protected:
    //! Pre fair share update methods.
    TJobResources GetSpecifiedResourceLimits() const override;

    //! Post fair share update methods.
    void SetStarvationStatus(EStarvationStatus starvationStatus) override;
    void CheckForStarvation(TInstant now) override;

    void BuildElementMapping(TFairSharePostUpdateContext* context) override;

private:
    TPoolConfigPtr Config_;

    bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const override;
    THistoricUsageAggregationParameters GetHistoricUsageAggregationParameters() const override;

    std::optional<double> GetSpecifiedWeight() const override;

    const TSchedulerCompositeElement* GetNearestAncestorWithResourceLimits(const TSchedulerCompositeElement* element) const;

    void DoSetConfig(TPoolConfigPtr newConfig);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerPoolElement)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerOperationElementFixedState
{
public:
    // Used by trunk node.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TString>, PendingByPool);

    DEFINE_BYREF_RW_PROPERTY(std::optional<ESchedulingSegment>, SchedulingSegment);
    DEFINE_BYREF_RW_PROPERTY(std::optional<THashSet<TString>>, SpecifiedSchedulingSegmentModules);

    DEFINE_BYREF_RO_PROPERTY(TJobResourcesWithQuotaList, DetailedMinNeededJobResources);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, AggregatedMinNeededJobResources);

    DEFINE_BYREF_RO_PROPERTY(THashSet<int>, DiskRequestMedia);

protected:
    TSchedulerOperationElementFixedState(
        IOperationStrategyHost* operation,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        TSchedulingTagFilter schedulingTagFilter);

    const TOperationId OperationId_;

    // Fixed in preupdate, used in postupdate.
    std::optional<EUnschedulableReason> UnschedulableReason_;

    IOperationStrategyHost* const Operation_;
    TFairShareStrategyOperationControllerConfigPtr ControllerConfig_;

    // Used only in trunk version.
    TString UserName_;

    // Used only for profiling.
    int SlotIndex_ = UndefinedSlotIndex;

    // Used to compute operation demand.
    TJobResources TotalNeededResources_;

    // Used in trunk node.
    bool RunningInThisPoolTree_ = false;

    // Fixed in preupdate and used to calculate resource limits.
    TSchedulingTagFilter SchedulingTagFilter_;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOperationPreemptionStatus,
    (AllowedUnconditionally)
    (AllowedConditionally)
    (ForbiddenSinceStarving)
    (ForbiddenSinceUnsatisfied)
    (ForbiddenSinceLowJobCount)
);

using TPreemptionStatusStatisticsVector = TEnumIndexedVector<EOperationPreemptionStatus, int>;

////////////////////////////////////////////////////////////////////////////////

class TSchedulerOperationElementSharedState
    : public TRefCounted
{
public:
    TSchedulerOperationElementSharedState(
        ISchedulerStrategyHost* strategyHost,
        int updatePreemptableJobsListLoggingPeriod,
        const NLogging::TLogger& logger);

    // Returns resources change.
    TJobResources SetJobResourceUsage(
        TJobId jobId,
        const TJobResources& resources);

    void UpdatePreemptableJobsList(
        const TResourceVector& fairShare,
        const TJobResources& totalResourceLimits,
        double preemptionSatisfactionThreshold,
        double aggressivePreemptionSatisfactionThreshold,
        int* moveCount,
        TSchedulerOperationElement* operationElement);

    bool GetPreemptable() const;
    void SetPreemptable(bool value);

    bool IsJobKnown(TJobId jobId) const;

    int GetRunningJobCount() const;
    int GetPreemptableJobCount() const;
    int GetAggressivelyPreemptableJobCount() const;

    bool AddJob(TJobId jobId, const TJobResources& resourceUsage, bool force);
    std::optional<TJobResources> RemoveJob(TJobId jobId);

    EJobPreemptionStatus GetJobPreemptionStatus(TJobId jobId) const;
    TJobPreemptionStatusMap GetJobPreemptionStatusMap() const;

    void UpdatePreemptionStatusStatistics(EOperationPreemptionStatus status);
    TPreemptionStatusStatisticsVector GetPreemptionStatusStatistics() const;

    void OnMinNeededResourcesUnsatisfied(
        const TScheduleJobsContext& context,
        const TJobResources& availableResources,
        const TJobResources& minNeededResources);
    TEnumIndexedVector<EJobResourceType, int> GetMinNeededResourcesUnsatisfiedCount();

    void OnOperationDeactivated(const TScheduleJobsContext& context, EDeactivationReason reason);
    TEnumIndexedVector<EDeactivationReason, int> GetDeactivationReasons();
    void ResetDeactivationReasonsFromLastNonStarvingTime();
    TEnumIndexedVector<EDeactivationReason, int> GetDeactivationReasonsFromLastNonStarvingTime();

    TInstant GetLastScheduleJobSuccessTime() const;

    TJobResources Disable();
    void Enable();
    bool Enabled();

    void RecordHeartbeat(
        const TPackingHeartbeatSnapshot& heartbeatSnapshot,
        const TFairShareStrategyPackingConfigPtr& config);
    bool CheckPacking(
        const TSchedulerOperationElement* operationElement,
        const TPackingHeartbeatSnapshot& heartbeatSnapshot,
        const TJobResourcesWithQuota& jobResources,
        const TJobResources& totalResourceLimits,
        const TFairShareStrategyPackingConfigPtr& config);

private:
    const ISchedulerStrategyHost* StrategyHost_;

    // TODO(eshcherbin): Use TEnumIndexedVector<EJobPreemptionStatus, TJobIdList> here and below.
    using TJobIdList = std::list<TJobId>;
    TJobIdList NonpreemptableJobs_;
    TJobIdList AggressivelyPreemptableJobs_;
    TJobIdList PreemptableJobs_;

    std::atomic<bool> Preemptable_ = {true};

    std::atomic<int> RunningJobCount_ = {0};
    TJobResources TotalResourceUsage_;
    TJobResources NonpreemptableResourceUsage_;
    TJobResources AggressivelyPreemptableResourceUsage_;

    std::atomic<int> UpdatePreemptableJobsListCount_ = {0};
    const int UpdatePreemptableJobsListLoggingPeriod_;

    // TODO(ignat): make it configurable.
    TDuration UpdateStateShardsBackoff_ = TDuration::Seconds(5);

    struct TJobProperties
    {
        //! Determines whether job belongs to the preemptable, aggressively preemptable or non-preemptable jobs list.
        EJobPreemptionStatus PreemptionStatus;

        //! Iterator in the per-operation list pointing to this particular job.
        TJobIdList::iterator JobIdListIterator;

        TJobResources ResourceUsage;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, JobPropertiesMapLock_);
    THashMap<TJobId, TJobProperties> JobPropertiesMap_;
    TInstant LastScheduleJobSuccessTime_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PreemptionStatusStatisticsLock_);
    TPreemptionStatusStatisticsVector PreemptionStatusStatistics_;

    const NLogging::TLogger Logger;

    struct TStateShard
    {
        TEnumIndexedVector<EDeactivationReason, std::atomic<int>> DeactivationReasons;
        TEnumIndexedVector<EDeactivationReason, std::atomic<int>> DeactivationReasonsFromLastNonStarvingTime;
        TEnumIndexedVector<EJobResourceType, std::atomic<int>> MinNeededResourcesUnsatisfiedCount;
        char Padding1[64];
        TEnumIndexedVector<EDeactivationReason, int> DeactivationReasonsLocal;
        TEnumIndexedVector<EDeactivationReason, int> DeactivationReasonsFromLastNonStarvingTimeLocal;
        TEnumIndexedVector<EJobResourceType, int> MinNeededResourcesUnsatisfiedCountLocal;
        char Padding2[64];
    };
    std::array<TStateShard, MaxNodeShardCount> StateShards_;
    TInstant LastStateShardsUpdateTime_ = TInstant();

    bool Enabled_ = false;

    TPackingStatistics HeartbeatStatistics_;

    TJobResources SetJobResourceUsage(TJobProperties* properties, const TJobResources& resources);

    TJobProperties* GetJobProperties(TJobId jobId);
    const TJobProperties* GetJobProperties(TJobId jobId) const;

    // Update atomic values from local values in shard state.
    void UpdateShardState();
};

DEFINE_REFCOUNTED_TYPE(TSchedulerOperationElementSharedState)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerOperationElement
    : public NVectorHdrf::TOperationElement
    , public TSchedulerElement
    , public TSchedulerOperationElementFixedState
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TOperationFairShareTreeRuntimeParametersPtr, RuntimeParameters);

    DEFINE_BYVAL_RO_PROPERTY(TStrategyOperationSpecPtr, Spec);

public:
    TSchedulerOperationElement(
        TFairShareStrategyTreeConfigPtr treeConfig,
        TStrategyOperationSpecPtr spec,
        TOperationFairShareTreeRuntimeParametersPtr runtimeParameters,
        TFairShareStrategyOperationControllerPtr controller,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        ISchedulerStrategyHost* strategyHost,
        IFairShareTreeElementHost* treeElementHost,
        IOperationStrategyHost* operation,
        const TString& treeId,
        const NLogging::TLogger& logger);
    TSchedulerOperationElement(
        const TSchedulerOperationElement& other,
        TSchedulerCompositeElement* clonedParent);

    //! Common interface.
    TSchedulerElementPtr Clone(TSchedulerCompositeElement* clonedParent) override;

    TString GetId() const override;

    TString GetLoggingString() const override;
    bool AreDetailedLogsEnabled() const final;

    ESchedulableStatus GetStatus(bool atUpdate = true) const override;

    void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;
    void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config);

    void InitOrUpdateSchedulingSegment(const TFairShareStrategySchedulingSegmentsConfigPtr& schedulingSegmentsConfig);

    const NVectorHdrf::TJobResourcesConfig* GetStrongGuaranteeResourcesConfig() const override;
    TResourceVector GetMaxShare() const override;

    //! Trunk node interface.
    int GetSlotIndex() const;

    const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    TString GetUserName() const;

    void Disable(bool markAsNonAlive);
    void Enable();

    void MarkOperationRunningInPool();
    bool IsOperationRunningInPool() const;

    void MarkPendingBy(TSchedulerCompositeElement* violatedPool);

    void AttachParent(TSchedulerCompositeElement* newParent, int slotIndex);
    void ChangeParent(TSchedulerCompositeElement* newParent, int slotIndex);
    void DetachParent();

    //! Shared state interface.
    bool OnJobStarted(
        TJobId jobId,
        const TJobResources& resourceUsage,
        const TJobResources& precommittedResources,
        bool force = false);
    void OnJobFinished(TJobId jobId);
    void SetJobResourceUsage(TJobId jobId, const TJobResources& resources);

    bool IsJobKnown(TJobId jobId) const;
    //! This method is deprecated and used only in unit tests. Use |GetJobPreemptionStatus| instead.
    bool IsJobPreemptable(TJobId jobId, bool aggressivePreemptionEnabled) const;

    EJobPreemptionStatus GetJobPreemptionStatus(TJobId jobId) const;
    TJobPreemptionStatusMap GetJobPreemptionStatusMap() const;

    int GetRunningJobCount() const;
    int GetPreemptableJobCount() const;
    int GetAggressivelyPreemptableJobCount() const;

    TEnumIndexedVector<EDeactivationReason, int> GetDeactivationReasons() const;
    TEnumIndexedVector<EDeactivationReason, int> GetDeactivationReasonsFromLastNonStarvingTime() const;
    TEnumIndexedVector<EJobResourceType, int> GetMinNeededResourcesUnsatisfiedCount() const;

    TPreemptionStatusStatisticsVector GetPreemptionStatusStatistics() const;

    TInstant GetLastScheduleJobSuccessTime() const;

    //! Pre fair share update methods.
    void PreUpdateBottomUp(NVectorHdrf::TFairShareUpdateContext* context) override;

    //! Fair share update methods that implements NVectorHdrf::TOperationElement interface.
    TResourceVector GetBestAllocationShare() const override;
    bool IsGang() const override;

    //! Post fair share update methods.
    TInstant GetLastNonStarvingTime() const;

    void PublishFairShareAndUpdatePreemptionSettings() override;
    void UpdatePreemptionAttributes() override;

    std::optional<double> GetSpecifiedFairShareStarvationTolerance() const override;
    std::optional<TDuration> GetSpecifiedFairShareStarvationTimeout() const override;

    //! Schedule job methods.
    void PrescheduleJob(TScheduleJobsContext* context, EOperationPreemptionPriority targetOperationPreemptionPriority) final;

    void UpdateDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap,
        bool checkLiveness) final;

    void ActualizeResourceUsageInDynamicAttributes(TDynamicAttributesList* attributesList);

    const TJobResources& FillResourceUsageInDynamicAttributes(
        TDynamicAttributesList* attributesList,
        const TResourceUsageSnapshotPtr& resourceUsageSnapshot) final;

    void CheckForDeactivation(TScheduleJobsContext* context, EOperationPreemptionPriority operationPreemptionPriority);

    void UpdateCurrentResourceUsage(TScheduleJobsContext* context);
    bool IsUsageOutdated(TScheduleJobsContext* context) const;

    TFairShareScheduleJobResult ScheduleJob(TScheduleJobsContext* context, bool ignorePacking) final;

    void ActivateOperation(TScheduleJobsContext* context);
    void DeactivateOperation(TScheduleJobsContext* context, EDeactivationReason reason);

    std::optional<bool> IsAggressiveStarvationEnabled() const override;
    std::optional<bool> IsAggressivePreemptionAllowed() const override;
    bool HasAggressivelyStarvingElements(TScheduleJobsContext* context) const override;

    const TSchedulerElement* FindPreemptionBlockingAncestor(
        EOperationPreemptionPriority operationPreemptionPriority,
        const TDynamicAttributesList& dynamicAttributesList,
        const TFairShareStrategyTreeConfigPtr& config) const;

    bool IsLimitingAncestorCheckEnabled() const;

    bool IsSchedulingSegmentCompatibleWithNode(ESchedulingSegment nodeSegment, const TSchedulingSegmentModule& nodeModule) const;

    void PrepareConditionalUsageDiscounts(TScheduleJobsContext* context, EOperationPreemptionPriority operationPreemptionPriority) const override;

    void CountOperationsByPreemptionPriority(TScheduleJobsContext* context) const override;

    //! Other methods.
    std::optional<TString> GetCustomProfilingTag() const;

protected:
    //! Pre update methods.
    void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const override;

    //! Post update methods.
    void SetStarvationStatus(EStarvationStatus starvationStatus) override;
    void CheckForStarvation(TInstant now) override;

    bool IsSchedulable() const override;
    void BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context) override;

    void CollectOperationSchedulingSegmentContexts(
        THashMap<TOperationId, TOperationSchedulingSegmentContext>* operationContexts) const override;
    void ApplyOperationSchedulingSegmentChanges(
        const THashMap<TOperationId, TOperationSchedulingSegmentContext>& operationContexts) override;

    void BuildElementMapping(TFairSharePostUpdateContext* context) override;

private:
    std::optional<double> GetSpecifiedWeight() const override;

    void DisableNonAliveElements() override;

    const TSchedulerOperationElementSharedStatePtr OperationElementSharedState_;
    const TFairShareStrategyOperationControllerPtr Controller_;

    //! Controller related methods.
    bool IsMaxScheduleJobCallsViolated() const;
    bool IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const;

    //! Pre fair share update methods.
    std::optional<EUnschedulableReason> ComputeUnschedulableReason() const;

    TJobResources ComputeResourceDemand() const;

    TJobResources GetSpecifiedResourceLimits() const override;

    //! Post fair share update methods.
    void UpdatePreemptableJobsList();

    //! Schedule job methods.
    TControllerScheduleJobResultPtr DoScheduleJob(
        TScheduleJobsContext* context,
        const TJobResources& availableResources,
        TJobResources* precommittedResources);

    std::optional<EDeactivationReason> TryStartScheduleJob(
        const TScheduleJobsContext& context,
        TJobResources* precommittedResourcesOutput,
        TJobResources* availableResourcesOutput);
    void FinishScheduleJob(const ISchedulingContextPtr& schedulingContext);

    TFairShareStrategyPackingConfigPtr GetPackingConfig() const;
    bool CheckPacking(const TPackingHeartbeatSnapshot& heartbeatSnapshot) const;

    void RecordHeartbeat(const TPackingHeartbeatSnapshot& heartbeatSnapshot);

    bool HasJobsSatisfyingResourceLimits(const TScheduleJobsContext& context) const;

    void UpdateAncestorsDynamicAttributes(
        TScheduleJobsContext* context,
        const TJobResources& resourceUsageDelta,
        bool checkAncestorsActiveness = true);

    EResourceTreeIncreaseResult TryIncreaseHierarchicalResourceUsagePrecommit(
        const TJobResources& delta,
        TJobResources* availableResourceLimitsOutput = nullptr);

    TJobResources GetHierarchicalAvailableResources(const TScheduleJobsContext& context) const;

    std::optional<EDeactivationReason> CheckBlocked(const ISchedulingContextPtr& schedulingContext) const;
    bool HasRecentScheduleJobFailure(NProfiling::TCpuInstant now) const;

    void OnOperationDeactivated(
        TScheduleJobsContext* context,
        EDeactivationReason reason,
        bool considerInOperationCounter);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerOperationElement)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerRootElementFixedState
{
public:
    // TODO(ignat): move it to TFairShareTreeSnapshot.
    DEFINE_BYVAL_RO_PROPERTY(int, TreeSize);
    DEFINE_BYVAL_RO_PROPERTY(int, SchedulableElementCount);
};

class TSchedulerRootElement
    : public NVectorHdrf::TRootElement
    , public TSchedulerCompositeElement
    , public TSchedulerRootElementFixedState
{
public:
    TSchedulerRootElement(
        ISchedulerStrategyHost* strategyHost,
        IFairShareTreeElementHost* treeElementHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        const TString& treeId,
        const NLogging::TLogger& logger);
    TSchedulerRootElement(const TSchedulerRootElement& other);

    //! Common interface.
    TString GetId() const override;

    TSchedulerRootElementPtr Clone();

    TSchedulerElementPtr Clone(TSchedulerCompositeElement* clonedParent) override;

    void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;

    // Used for diagnostics purposes.
    TJobResources GetSpecifiedStrongGuaranteeResources() const override;
    TResourceVector GetMaxShare() const override;

    std::vector<EFifoSortParameter> GetFifoSortParameters() const override;

    //! Trunk node interface.
    int GetMaxRunningOperationCount() const override;
    int GetMaxOperationCount() const override;

    TPoolIntegralGuaranteesConfigPtr GetIntegralGuaranteesConfig() const override;

    bool IsDefaultConfigured() const override;
    bool AreImmediateOperationsForbidden() const override;

    const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    //! Pre fair share update methods.
    // Computes various lightweight attributes in the tree. Must be called in control thread.
    void PreUpdate(NVectorHdrf::TFairShareUpdateContext* context);

    //! Fair share update methods that implements NVectorHdrf::TRootElement interface.
    double GetSpecifiedBurstRatio() const override;
    double GetSpecifiedResourceFlowRatio() const override;

    //! Post update methods.
    void PostUpdate(
        TFairSharePostUpdateContext* postUpdateContext,
        TManageTreeSchedulingSegmentsContext* manageSegmentsContext);

    std::optional<double> GetSpecifiedFairShareStarvationTolerance() const override;
    std::optional<TDuration> GetSpecifiedFairShareStarvationTimeout() const override;

    //! Schedule job methods.
    std::optional<bool> IsAggressiveStarvationEnabled() const override;
    std::optional<bool> IsAggressivePreemptionAllowed() const override;

    //! Other methods.
    THashSet<TString> GetAllowedProfilingTags() const override;

    bool IsFairShareTruncationInFifoPoolEnabled() const override;

    void BuildResourceMetering(
        const std::optional<TMeteringKey>& parentKey,
        const THashMap<TString, TResourceVolume>& poolResourceUsages,
        TMeteringMap* meteringMap) const override;

    TResourceDistributionInfo GetResourceDistributionInfo() const;
    void BuildResourceDistributionInfo(NYTree::TFluentMap fluent) const;

protected:
    //! Post update methods.
    void CheckForStarvation(TInstant now) override;

private:
    // Pre fair share update methods.
    std::optional<double> GetSpecifiedWeight() const override;

    TJobResources GetSpecifiedResourceLimits() const override;

    bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const override;
    THistoricUsageAggregationParameters GetHistoricUsageAggregationParameters() const override;

    // Post fair share update methods.
    void ManageSchedulingSegments(TManageTreeSchedulingSegmentsContext* manageSegmentsContext);
    void UpdateCachedJobPreemptionStatusesIfNecessary(TFairSharePostUpdateContext* context) const;
    void BuildSchedulableIndices(TDynamicAttributesList* dynamicAttributesList, TChildHeapMap* childHeapMap);

    bool CanAcceptFreeVolume() const override;
    bool ShouldDistributeFreeVolumeAmongChildren() const override;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerRootElement)

////////////////////////////////////////////////////////////////////////////////

class TChildHeap
{
public:
    TChildHeap(
        const std::vector<TSchedulerElementPtr>& children,
        TDynamicAttributesList* dynamicAttributesList,
        const TSchedulerCompositeElement* owningElement,
        ESchedulingMode mode);
    TSchedulerElement* GetTop() const;
    void Update(TSchedulerElement* child);

    // For testing purposes.
    const std::vector<TSchedulerElement*>& GetHeap() const;

private:
    TDynamicAttributesList& DynamicAttributesList_;
    const TSchedulerCompositeElement* OwningElement_;
    const ESchedulingMode Mode_;

    std::vector<TSchedulerElement*> ChildHeap_;

    bool Comparator(const TSchedulerElement* lhs, const TSchedulerElement* rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

////////////////////////////////////////////////////////////////////////////////

#define YT_ELEMENT_LOG_DETAILED(schedulerElement, ...) \
    do { \
        const auto& Logger = schedulerElement->GetLogger(); \
        if (schedulerElement->AreDetailedLogsEnabled()) { \
            YT_LOG_DEBUG(__VA_ARGS__); \
        } else { \
            YT_LOG_TRACE(__VA_ARGS__); \
        } \
    } while(false)
