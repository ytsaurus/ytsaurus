#pragma once

#include "fair_share_strategy_operation_controller.h"
#include "job.h"
#include "private.h"
#include "resource_vector.h"
#include "resource_tree.h"
#include "resource_tree_element.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "scheduling_segment_manager.h"
#include "fair_share_strategy_operation_controller.h"
#include "fair_share_tree_snapshot.h"
#include "fair_share_tree_snapshot_impl.h"
#include "fair_share_update.h"
#include "packing.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/job_metrics.h>
#include <yt/yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/yt/server/lib/scheduler/resource_metering.h>

#include <yt/yt/ytlib/scheduler/job_resources.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/misc/historic_usage_aggregator.h>

namespace NYT::NScheduler {

using NFairShare::TSchedulableAttributes;
using NFairShare::TDetailedFairShare;
using NFairShare::TIntegralResourcesState;

////////////////////////////////////////////////////////////////////////////////

static constexpr int UnassignedTreeIndex = -1;
static constexpr int UndefinedSlotIndex = -1;
static constexpr int EmptySchedulingTagFilterIndex = -1;

////////////////////////////////////////////////////////////////////////////////

static constexpr double InfiniteSatisfactionRatio = 1e+9;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPrescheduleJobOperationCriterion,
    (All)
    (AggressivelyStarvingOnly)
    (StarvingOnly)
);

////////////////////////////////////////////////////////////////////////////////

//! Attributes that are kept between fair share updates.
struct TPersistentAttributes
{
    bool Starving = false;
    TInstant LastNonStarvingTime = TInstant::Now();
    std::optional<TInstant> BelowFairShareSince;
    THistoricUsageAggregator HistoricUsageAggregator;

    TResourceVector BestAllocationShare = TResourceVector::Ones();
    TInstant LastBestAllocationRatioUpdateTime;

    TIntegralResourcesState IntegralResourcesState;

    TJobResources AppliedResourceLimits = TJobResources::Infinite();

    TDataCenter SchedulingSegmentDataCenter;
    std::optional<TInstant> FailingToScheduleAtDataCenterSince;

    void ResetOnElementEnabled();
};

////////////////////////////////////////////////////////////////////////////////

class TChildHeap;

static const int InvalidHeapIndex = -1;

struct TDynamicAttributes
{
    double SatisfactionRatio = 0.0;
    bool Active = false;
    TSchedulerElement* BestLeafDescendant = nullptr;
    TJobResources ResourceUsage;

    int HeapIndex = InvalidHeapIndex;
};

using TDynamicAttributesList = std::vector<TDynamicAttributes>;

using TChildHeapMap = THashMap<int, TChildHeap>;
using TJobResourcesMap = THashMap<int, TJobResources>;

////////////////////////////////////////////////////////////////////////////////

struct TFairSharePostUpdateContext
{
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
    TScheduleJobsProfilingCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter PrescheduleJobCount;
    NProfiling::TEventTimer PrescheduleJobTime;
    NProfiling::TEventTimer TotalControllerScheduleJobTime;
    NProfiling::TEventTimer ExecControllerScheduleJobTime;
    NProfiling::TEventTimer StrategyScheduleJobTime;
    NProfiling::TEventTimer PackingRecordHeartbeatTime;
    NProfiling::TEventTimer PackingCheckTime;
    NProfiling::TEventTimer AnalyzeJobsTime;
    NProfiling::TCounter ScheduleJobAttemptCount;
    NProfiling::TCounter ScheduleJobFailureCount;

    TEnumIndexedVector<NControllerAgent::EScheduleJobFailReason, NProfiling::TCounter> ControllerScheduleJobFail;
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
    TScheduleJobsStage(TString loggingName, TScheduleJobsProfilingCounters profilingCounters);

    const TString LoggingName;
    TScheduleJobsProfilingCounters ProfilingCounters;
};

////////////////////////////////////////////////////////////////////////////////

class TScheduleJobsContext
{
private:
    struct TStageState
    {
        TStageState(TScheduleJobsStage* schedulingStage, const TString& name);

        TScheduleJobsStage* const SchedulingStage;
        
        TString Name;

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
    };

    DEFINE_BYREF_RW_PROPERTY(std::vector<bool>, CanSchedule);

    DEFINE_BYREF_RW_PROPERTY(TDynamicAttributesList, DynamicAttributesList);

    DEFINE_BYREF_RW_PROPERTY(TChildHeapMap, ChildHeapMap);
    DEFINE_BYREF_RW_PROPERTY(TJobResourcesMap, UsageDiscountMap);

    DEFINE_BYREF_RO_PROPERTY(ISchedulingContextPtr, SchedulingContext);

    DEFINE_BYVAL_RW_PROPERTY(std::optional<bool>, HasAggressivelyStarvingElements);

    DEFINE_BYREF_RW_PROPERTY(TScheduleJobsStatistics, SchedulingStatistics);

    DEFINE_BYREF_RW_PROPERTY(std::vector<TSchedulerOperationElementPtr>, BadPackingOperations);

    DEFINE_BYREF_RW_PROPERTY(std::optional<TStageState>, StageState);

public:
    TScheduleJobsContext(
        ISchedulingContextPtr schedulingContext,
        int treeSize,
        std::vector<TSchedulingTagFilter> registeredSchedulingTagFilters,
        bool enableSchedulingInfoLogging,
        const NLogging::TLogger& logger);

    void PrepareForScheduling(const TSchedulerRootElementPtr& rootElement);

    TDynamicAttributes& DynamicAttributesFor(const TSchedulerElement* element);
    const TDynamicAttributes& DynamicAttributesFor(const TSchedulerElement* element) const;

    TJobResources GetUsageDiscountFor(const TSchedulerElement* element) const;

    void StartStage(TScheduleJobsStage* schedulingStage, const TString& stageName);

    void ProfileStageTimingsAndLogStatistics();

    void FinishStage();

private:
    const int SchedulableElementCount_;

    const std::vector<TSchedulingTagFilter> RegisteredSchedulingTagFilters_;

    const bool EnableSchedulingInfoLogging_;

    const NLogging::TLogger Logger;

    bool Initialized_ = false;

    void ProfileStageTimings();

    void LogStageStatistics();
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
    DEFINE_BYVAL_RO_PROPERTY(double, AdjustedFairShareStarvationTolerance, 1.0);
    DEFINE_BYVAL_RO_PROPERTY(TDuration, AdjustedFairSharePreemptionTimeout);

protected:
    TSchedulerElementFixedState(
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        TString treeId);

    ISchedulerStrategyHost* const Host_;
    IFairShareTreeHost* const TreeHost_;

    TFairShareStrategyTreeConfigPtr TreeConfig_;

    // These fields calculated in preupdate and used for update.
    TJobResources ResourceDemand_;
    TJobResources ResourceUsageAtUpdate_;
    TJobResources ResourceLimits_;

    // These attributes are calculated during fair share update and further used in schedule jobs.
    NFairShare::TSchedulableAttributes Attributes_;

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

    // Flag indicates that we can change fields of scheduler elements.
    bool Mutable_ = true;

    const TString TreeId_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerElement
    : public virtual NFairShare::TElement
    , public TSchedulerElementFixedState
{
public:
    //! Common interface.
    virtual TSchedulerElementPtr Clone(TSchedulerCompositeElement* clonedParent) = 0;

    virtual TString GetTreeId() const;

    virtual const NLogging::TLogger& GetLogger() const override;
    virtual bool AreDetailedLogsEnabled() const override;

    virtual TString GetLoggingString() const;

    TSchedulerCompositeElement* GetMutableParent();
    const TSchedulerCompositeElement* GetParent() const;

    void InitAccumulatedResourceVolume(TResourceVolume resourceVolume);

    bool IsAlive() const;
    void SetNonAlive();

    bool GetStarving() const;

    TJobResources GetInstantResourceUsage() const;

    virtual double GetFairShareStarvationTolerance() const = 0;
    virtual TDuration GetFairSharePreemptionTimeout() const = 0;

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
    // Used for manipulattions with SchedulingTagFilterIndex.
    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const;
    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config);

    //! Pre fair share update methods.
    // At this stage we prepare attributes that need to be computed in the control thread
    // in a thread-unsafe manner.
    virtual void PreUpdateBottomUp(NFairShare::TFairShareUpdateContext* context);

    TJobResources ComputeTotalResourcesOnSuitableNodes() const;
    TJobResources GetTotalResourceLimits() const;
    virtual TJobResources GetSpecifiedResourceLimits() const = 0;

    virtual void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const = 0;

    //! Fair share update methods that implements NFairShare::TElement interface.
    virtual const TJobResources& GetResourceDemand() const override;
    virtual const TJobResources& GetResourceUsageAtUpdate() const override;
    virtual const TJobResources& GetResourceLimits() const override;

    virtual double GetWeight() const override;

    virtual TSchedulableAttributes& Attributes() override;
    virtual const TSchedulableAttributes& Attributes() const override;

    virtual TElement* GetParentElement() const override;
    virtual TJobResourcesConfigPtr GetStrongGuaranteeResourcesConfig() const override;

    TInstant GetStartTime() const;

    //! Post fair share update methods.
    virtual void CheckForStarvation(TInstant now) = 0;
    virtual void MarkImmutable();

    //! Schedule jobs interface.
    virtual void PrescheduleJob(
        TScheduleJobsContext* context,
        EPrescheduleJobOperationCriterion operationCriterion,
        bool aggressiveStarvationEnabled) = 0;

    virtual void UpdateDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap) = 0;

    virtual TFairShareScheduleJobResult ScheduleJob(TScheduleJobsContext* context, bool ignorePacking) = 0;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const = 0;
    virtual bool HasAggressivelyStarvingElements(TScheduleJobsContext* context, bool aggressiveStarvationEnabled) const = 0;

    virtual const TJobResources& CalculateCurrentResourceUsage(TScheduleJobsContext* context) = 0;

    int GetTreeIndex() const;

    bool IsActive(const TDynamicAttributesList& dynamicAttributesList) const;
    bool AreResourceLimitsViolated() const;

    //! Other methods based on tree snapshot.
    virtual void BuildResourceMetering(const std::optional<TMeteringKey>& parentKey, TMeteringMap* meteringMap) const;

private:
    TResourceTreeElementPtr ResourceTreeElement_;

protected:
    NLogging::TLogger Logger;

    TSchedulerElement(
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
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
    TJobResources ComputeResourceLimits() const;

    //! Post update methods.
    virtual void SetStarving(bool starving);

    ESchedulableStatus GetStatusImpl(double defaultTolerance, bool atUpdate) const;
    void CheckForStarvationImpl(TDuration fairSharePreemptionTimeout, TInstant now);

    TResourceVector GetResourceUsageShare() const;

    // Publishes fair share and updates preemptable job lists of operations.
    virtual void PublishFairShareAndUpdatePreemption() = 0;
    virtual void UpdatePreemptionAttributes();

    // This method reuses common code with schedule jobs logic to calculate dynamic attributes.
    virtual void UpdateSchedulableAttributesFromDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap);

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
    bool CheckDemand(const TJobResources& delta, const TScheduleJobsContext& context);
    TJobResources GetLocalAvailableResourceLimits(const TScheduleJobsContext& context) const;

    double ComputeLocalSatisfactionRatio(const TJobResources& resourceUsage) const;
    bool IsResourceBlocked(EJobResourceType resource) const;
    bool AreAllResourcesBlocked() const;

    // Returns resource usage observed in current heartbeat.
    TJobResources GetCurrentResourceUsage(const TDynamicAttributesList& dynamicAttributesList) const;

    void IncreaseHierarchicalResourceUsage(const TJobResources& delta);

    virtual void DisableNonAliveElements() = 0;

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

    // Used only in schedule jobs.
    DEFINE_BYREF_RO_PROPERTY(double, AdjustedFairShareStarvationToleranceLimit);
    DEFINE_BYREF_RO_PROPERTY(TDuration, AdjustedFairSharePreemptionTimeoutLimit);

protected:
    // Used in fair share update.
    ESchedulingMode Mode_ = ESchedulingMode::Fifo;
    std::vector<EFifoSortParameter> FifoSortParameters_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerCompositeElement
    : public virtual NFairShare::TCompositeElement
    , public TSchedulerElement
    , public TSchedulerCompositeElementFixedState
{
public:
    TSchedulerCompositeElement(
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
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

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;

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
    virtual void PreUpdateBottomUp(NFairShare::TFairShareUpdateContext* context) override;

    //! Fair share update methods that implements NFairShare::TCompositeElement interface.
    virtual TElement* GetChild(int index) override final;
    virtual const TElement* GetChild(int index) const override final;
    virtual int GetChildrenCount() const override final;

    virtual ESchedulingMode GetMode() const;
    virtual bool HasHigherPriorityInFifoMode(const NFairShare::TElement* lhs, const NFairShare::TElement* rhs) const override final;

    //! Post fair share update methods.
    virtual void MarkImmutable() override;

    //! Schedule jobs related methods.
    virtual void PrescheduleJob(
        TScheduleJobsContext* context,
        EPrescheduleJobOperationCriterion operationCriterion,
        bool aggressiveStarvationEnabled) override final;

    virtual void UpdateDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap) override final;

    virtual TFairShareScheduleJobResult ScheduleJob(TScheduleJobsContext* context, bool ignorePacking) override final;

    virtual const TJobResources& CalculateCurrentResourceUsage(TScheduleJobsContext* context) override final;

    virtual bool IsAggressiveStarvationEnabled() const;
    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;
    virtual bool HasAggressivelyStarvingElements(TScheduleJobsContext* context, bool aggressiveStarvationEnabled) const override;

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
    virtual void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const override;

    //! Fair share update methods.
    virtual void DisableNonAliveElements() override;

    //! Post fair share update methods.
    virtual bool IsSchedulable() const override;
    virtual void BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context) override;

    virtual int EnumerateElements(int startIndex, bool isSchedulableValueFilter) override;

    virtual void PublishFairShareAndUpdatePreemption() override;
    virtual void UpdatePreemptionAttributes() override;

    virtual void UpdateSchedulableAttributesFromDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap) override;

    virtual void CollectOperationSchedulingSegmentContexts(
        THashMap<TOperationId, TOperationSchedulingSegmentContext>* operationContexts) const override;
    virtual void ApplyOperationSchedulingSegmentChanges(
        const THashMap<TOperationId, TOperationSchedulingSegmentContext>& operationContexts) override;

    virtual void BuildElementMapping(TFairSharePostUpdateContext* context) override;

    virtual double GetFairShareStarvationToleranceLimit() const;
    virtual TDuration GetFairSharePreemptionTimeoutLimit() const;

    // Used to implement GetWeight.
    virtual bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const = 0;
    virtual THistoricUsageAggregationParameters GetHistoricUsageAggregationParameters() const = 0;

    void UpdateChild(TScheduleJobsContext* context, TSchedulerElement* child);

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
    : public NFairShare::TPool
    , public TSchedulerCompositeElement
    , public TSchedulerPoolElementFixedState
{
public:
    TSchedulerPoolElement(
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
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
    virtual TSchedulerElementPtr Clone(TSchedulerCompositeElement* clonedParent) override;

    virtual TString GetId() const override;

    void AttachParent(TSchedulerCompositeElement* newParent);
    void ChangeParent(TSchedulerCompositeElement* newParent);
    void DetachParent();

    virtual ESchedulableStatus GetStatus(bool atUpdate = true) const override;

    // Used for diagnostics only.
    virtual TResourceVector GetMaxShare() const override;

    //! Trunk node interface.
    TPoolConfigPtr GetConfig() const;
    void SetConfig(TPoolConfigPtr config);
    void SetDefaultConfig();

    void SetUserName(const std::optional<TString>& userName);
    const std::optional<TString>& GetUserName() const;

    virtual int GetMaxOperationCount() const override;
    virtual int GetMaxRunningOperationCount() const override;

    virtual TPoolIntegralGuaranteesConfigPtr GetIntegralGuaranteesConfig() const override;

    void SetEphemeralInDefaultParentPool();
    bool IsEphemeralInDefaultParentPool() const;

    virtual bool IsExplicit() const override;
    virtual bool IsDefaultConfigured() const override;
    virtual bool AreImmediateOperationsForbidden() const override;

    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const override;

    // Used to manipulate with SchedulingTagFilterIndex.
    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    //! Fair share update methods that implements NFairShare::TPool interface.
    virtual bool AreDetailedLogsEnabled() const override final;
    virtual TJobResourcesConfigPtr GetStrongGuaranteeResourcesConfig() const override;

    virtual double GetSpecifiedBurstRatio() const override;
    virtual double GetSpecifiedResourceFlowRatio() const override;

    virtual EIntegralGuaranteeType GetIntegralGuaranteeType() const override;
    virtual TResourceVector GetIntegralShareLimitForRelaxedPool() const override;

    virtual const TIntegralResourcesState& IntegralResourcesState() const override;
    virtual TIntegralResourcesState& IntegralResourcesState() override;

    //! Post fair share update methods.
    virtual void CheckForStarvation(TInstant now) override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual double GetFairShareStarvationToleranceLimit() const override;
    virtual TDuration GetFairSharePreemptionTimeoutLimit() const override;

    //! Schedule job methods.
    virtual bool IsAggressiveStarvationEnabled() const override;
    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;

    //! Other methods.
    virtual void BuildResourceMetering(const std::optional<TMeteringKey>& parentKey, TMeteringMap* meteringMap) const override;

    virtual THashSet<TString> GetAllowedProfilingTags() const override;

protected:
    //! Pre fair share update methods.
    virtual TJobResources GetSpecifiedResourceLimits() const override;
    
    //! Post fair share update methods.
    virtual void SetStarving(bool starving) override;
    
    virtual void BuildElementMapping(TFairSharePostUpdateContext* context) override;

private:
    TPoolConfigPtr Config_;

    virtual bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const override;
    virtual THistoricUsageAggregationParameters GetHistoricUsageAggregationParameters() const override;

    virtual std::optional<double> GetSpecifiedWeight() const override;

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
    DEFINE_BYREF_RW_PROPERTY(std::optional<THashSet<TString>>, SpecifiedSchedulingSegmentDataCenters);

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

    // Used in schedule jobs.
    TJobResourcesWithQuotaList DetailedMinNeededJobResources_;
    TJobResources AggregatedMinNeededJobResources_;

    // Used to compute operation demand.
    TJobResources TotalNeededResources_;

    // Used in trunk node.
    bool RunningInThisPoolTree_ = false;

    // Fixed in preupdate and used to calculate resource limits.
    TSchedulingTagFilter SchedulingTagFilter_;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOperationPreemptionStatus,
    (Allowed)
    (ForbiddenSinceStarvingParentOrSelf)
    (ForbiddenSinceUnsatisfiedParentOrSelf)
    (ForbiddenSinceLowJobCount)
);

using TPreemptionStatusStatisticsVector = TEnumIndexedVector<EOperationPreemptionStatus, int>;

class TSchedulerOperationElementSharedState
    : public TRefCounted
{
public:
    TSchedulerOperationElementSharedState(
        ISchedulerStrategyHost* host,
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

    bool IsJobPreemptable(TJobId jobId, bool aggressivePreemptionEnabled) const;

    int GetRunningJobCount() const;
    int GetPreemptableJobCount() const;
    int GetAggressivelyPreemptableJobCount() const;

    bool AddJob(TJobId jobId, const TJobResources& resourceUsage, bool force);
    std::optional<TJobResources> RemoveJob(TJobId jobId);

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
    const ISchedulerStrategyHost* Host_;

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
        TJobProperties(
            bool preemptable,
            bool aggressivelyPreemptable,
            TJobIdList::iterator jobIdListIterator,
            const TJobResources& resourceUsage)
            : Preemptable(preemptable)
            , AggressivelyPreemptable(aggressivelyPreemptable)
            , JobIdListIterator(jobIdListIterator)
            , ResourceUsage(resourceUsage)
        { }

        //! Determines whether job belongs to the list of preemptable or aggressively preemptable jobs of operation.
        bool Preemptable;

        //! Determines whether job belongs to the list of aggressively preemptable (but not all preemptable) jobs of operation.
        bool AggressivelyPreemptable;

        //! Iterator in the per-operation list pointing to this particular job.
        TJobIdList::iterator JobIdListIterator;

        TJobResources ResourceUsage;
    };

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, JobPropertiesMapLock_);
    THashMap<TJobId, TJobProperties> JobPropertiesMap_;
    TInstant LastScheduleJobSuccessTime_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, PreemptionStatusStatisticsLock_);
    TPreemptionStatusStatisticsVector PreemptionStatusStatistics_;

    const NLogging::TLogger Logger;

    struct TStateShard
    {
        TEnumIndexedVector<EDeactivationReason, std::atomic<int>> DeactivationReasons;
        TEnumIndexedVector<EDeactivationReason, std::atomic<int>> DeactivationReasonsFromLastNonStarvingTime;
        TEnumIndexedVector<EJobResourceType, std::atomic<int>> MinNeededResourcesUnsatisfiedCount;
        TEnumIndexedVector<EDeactivationReason, int> DeactivationReasonsLocal;
        TEnumIndexedVector<EDeactivationReason, int> DeactivationReasonsFromLastNonStarvingTimeLocal;
        TEnumIndexedVector<EJobResourceType, int> MinNeededResourcesUnsatisfiedCountLocal;
        char Padding[64];
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
    : public NFairShare::TOperationElement
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
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
        IOperationStrategyHost* operation,
        const TString& treeId,
        const NLogging::TLogger& logger);
    TSchedulerOperationElement(
        const TSchedulerOperationElement& other,
        TSchedulerCompositeElement* clonedParent);

    //! Common interface.
    virtual TSchedulerElementPtr Clone(TSchedulerCompositeElement* clonedParent) override;

    virtual TString GetId() const override;

    virtual TString GetLoggingString() const override;
    virtual bool AreDetailedLogsEnabled() const override final;

    virtual ESchedulableStatus GetStatus(bool atUpdate = true) const override;

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;
    void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config);

    void InitOrUpdateSchedulingSegment(ESegmentedSchedulingMode mode);

    virtual TJobResourcesConfigPtr GetStrongGuaranteeResourcesConfig() const override;
    virtual TResourceVector GetMaxShare() const override;

    //! Trunk node interface.
    int GetSlotIndex() const;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

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
    bool IsJobPreemptable(TJobId jobId, bool aggressivePreemptionEnabled) const;

    int GetRunningJobCount() const;
    int GetPreemptableJobCount() const;
    int GetAggressivelyPreemptableJobCount() const;

    TEnumIndexedVector<EDeactivationReason, int> GetDeactivationReasons() const;
    TEnumIndexedVector<EDeactivationReason, int> GetDeactivationReasonsFromLastNonStarvingTime() const;
    TEnumIndexedVector<EJobResourceType, int> GetMinNeededResourcesUnsatisfiedCount() const;

    TPreemptionStatusStatisticsVector GetPreemptionStatusStatistics() const;

    TInstant GetLastScheduleJobSuccessTime() const;

    //! Pre fair share update methods.
    virtual void PreUpdateBottomUp(NFairShare::TFairShareUpdateContext* context) override;

    //! Fair share update methods that implements NFairShare::TOperationElement interface.
    virtual TResourceVector GetBestAllocationShare() const override;

    //! Post fair share update methods.
    virtual void SetStarving(bool starving) override;
    virtual void CheckForStarvation(TInstant now) override;

    TInstant GetLastNonStarvingTime() const;


    virtual void PublishFairShareAndUpdatePreemption() override;
    virtual void UpdatePreemptionAttributes() override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    //! Schedule job methods.
    virtual void PrescheduleJob(
        TScheduleJobsContext* context,
        EPrescheduleJobOperationCriterion operationCriterion,
        bool aggressiveStarvationEnabled) override final;

    virtual void UpdateDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap) override final;

    virtual const TJobResources& CalculateCurrentResourceUsage(TScheduleJobsContext* context) override final;

    virtual TFairShareScheduleJobResult ScheduleJob(TScheduleJobsContext* context, bool ignorePacking) override final;

    void ActivateOperation(TScheduleJobsContext* context);
    void DeactivateOperation(TScheduleJobsContext* context, EDeactivationReason reason);

    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;
    virtual bool HasAggressivelyStarvingElements(TScheduleJobsContext* context, bool aggressiveStarvationEnabled) const override;

    bool IsPreemptionAllowed(
        bool isAggressivePreemption,
        const TDynamicAttributesList& dynamicAttributesList,
        const TFairShareStrategyTreeConfigPtr& config) const;

    bool IsLimitingAncestorCheckEnabled() const;

    bool IsSchedulingSegmentCompatibleWithNode(ESchedulingSegment nodeSegment, const TDataCenter& nodeDataCenter) const;

    // Other methods.
    std::optional<TString> GetCustomProfilingTag() const;

    // Used in orchid.
    TJobResourcesWithQuotaList GetDetailedMinNeededJobResources() const;

protected:
    // Pre update methods.
    virtual void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const override;

    // Post update methods.
    virtual bool IsSchedulable() const override;
    virtual void BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context) override;

    virtual void CollectOperationSchedulingSegmentContexts(
        THashMap<TOperationId, TOperationSchedulingSegmentContext>* operationContexts) const override;
    virtual void ApplyOperationSchedulingSegmentChanges(
        const THashMap<TOperationId, TOperationSchedulingSegmentContext>& operationContexts) override;

    virtual void BuildElementMapping(TFairSharePostUpdateContext* context) override;

private:
    virtual std::optional<double> GetSpecifiedWeight() const override;

    virtual void DisableNonAliveElements() override;

    const TSchedulerOperationElementSharedStatePtr OperationElementSharedState_;
    const TFairShareStrategyOperationControllerPtr Controller_;

    //! Controller related methods.
    bool IsMaxScheduleJobCallsViolated() const;
    bool IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const;

    //! Pre fair share update methods.
    std::optional<EUnschedulableReason> ComputeUnschedulableReason() const;

    TJobResources ComputeResourceDemand() const;

    virtual TJobResources GetSpecifiedResourceLimits() const override;

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
    TJobResources GetLocalAvailableResourceDemand(const TScheduleJobsContext& context) const;

    std::optional<EDeactivationReason> CheckBlocked(const ISchedulingContextPtr& schedulingContext) const;
    bool HasRecentScheduleJobFailure(NProfiling::TCpuInstant now) const;

    void OnOperationDeactivated(TScheduleJobsContext* context, EDeactivationReason reason);

    void OnMinNeededResourcesUnsatisfied(
        const TScheduleJobsContext& context,
        const TJobResources& availableResources,
        const TJobResources& minNeededResources);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerOperationElement)

////////////////////////////////////////////////////////////////////////////////

class TSchedulerRootElementFixedState
{
public:
    // TODO(ignat): move it to TFairShareTreeSnapshotImpl.
    DEFINE_BYVAL_RO_PROPERTY(int, TreeSize);
    DEFINE_BYVAL_RO_PROPERTY(int, SchedulableElementCount);
};

class TSchedulerRootElement
    : public NFairShare::TRootElement
    , public TSchedulerCompositeElement
    , public TSchedulerRootElementFixedState
{
public:
    TSchedulerRootElement(
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        const TString& treeId,
        const NLogging::TLogger& logger);
    TSchedulerRootElement(const TSchedulerRootElement& other);

    //! Common interface.
    virtual TString GetId() const override;

    TSchedulerRootElementPtr Clone();

    virtual TSchedulerElementPtr Clone(TSchedulerCompositeElement* clonedParent) override;

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;
    
    // Used for diagnostics purposes.
    virtual TJobResources GetSpecifiedStrongGuaranteeResources() const override;
    virtual TResourceVector GetMaxShare() const override;

    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const override;

    //! Trunk node interface.
    virtual int GetMaxRunningOperationCount() const override;
    virtual int GetMaxOperationCount() const override;

    virtual TPoolIntegralGuaranteesConfigPtr GetIntegralGuaranteesConfig() const override;

    virtual bool IsDefaultConfigured() const override;
    virtual bool AreImmediateOperationsForbidden() const override;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    //! Pre fair share update methods.
    // Computes various lightweight attributes in the tree. Must be called in control thread.
    void PreUpdate(NFairShare::TFairShareUpdateContext* context);

    //! Fair share update methods that implements NFairShare::TRootElement interface.
    virtual double GetSpecifiedBurstRatio() const override;
    virtual double GetSpecifiedResourceFlowRatio() const override;

    //! Post share update methods.
    void PostUpdate(
        TFairSharePostUpdateContext* postUpdateContext,
        TManageTreeSchedulingSegmentsContext* manageSegmentsContext);

    virtual void CheckForStarvation(TInstant now) override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    //! Schedule job methods.
    virtual bool IsAggressiveStarvationEnabled() const override;

    //! Other methods.
    virtual THashSet<TString> GetAllowedProfilingTags() const override;

    virtual void BuildResourceMetering(const std::optional<TMeteringKey>& parentKey, TMeteringMap* meteringMap) const override;

    TResourceDistributionInfo GetResourceDistributionInfo() const;
    void BuildResourceDistributionInfo(NYTree::TFluentMap fluent) const;

private:
    // Pre fair share update methods.
    virtual std::optional<double> GetSpecifiedWeight() const override;

    virtual TJobResources GetSpecifiedResourceLimits() const override;

    virtual bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const override;
    virtual THistoricUsageAggregationParameters GetHistoricUsageAggregationParameters() const override;

    // Post fair share update methods.
    void ManageSchedulingSegments(TManageTreeSchedulingSegmentsContext* manageSegmentsContext);
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

////////////////////////////////////////////////////////////////////////////////

#define FAIR_SHARE_TREE_ELEMENT_INL_H_
#include "fair_share_tree_element-inl.h"
#undef FAIR_SHARE_TREE_ELEMENT_INL_H_
