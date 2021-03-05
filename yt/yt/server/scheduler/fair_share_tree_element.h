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
#include "packing.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/job_metrics.h>
#include <yt/yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/yt/server/lib/scheduler/resource_metering.h>

#include <yt/yt/ytlib/scheduler/job_resources.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/misc/historic_usage_aggregator.h>

#include <yt/yt/core/profiling/metrics_accumulator.h>

namespace NYT::NScheduler {

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

struct TDetailedFairShare
{
    TResourceVector StrongGuarantee = {};
    TResourceVector IntegralGuarantee = {};
    TResourceVector WeightProportional = {};
    TResourceVector Total = {};
};

TString ToString(const TDetailedFairShare& detailedFairShare);

void FormatValue(TStringBuilderBase* builder, const TDetailedFairShare& detailedFairShare, TStringBuf /* format */);

void Serialize(const TDetailedFairShare& detailedFairShare, NYson::IYsonConsumer* consumer);
void SerializeDominant(const TDetailedFairShare& detailedFairShare, NYTree::TFluentAny fluent);

////////////////////////////////////////////////////////////////////////////////

struct TSchedulableAttributes
{
    EJobResourceType DominantResource = EJobResourceType::Cpu;

    TDetailedFairShare FairShare;
    TResourceVector UsageShare;
    TResourceVector DemandShare;
    TResourceVector LimitsShare;
    TResourceVector StrongGuaranteeShare;
    TResourceVector ProposedIntegralShare;
    TResourceVector PromisedFairShare;

    TJobResources UnschedulableOperationsResourceUsage;
    TJobResources EffectiveStrongGuaranteeResources;

    double BurstRatio = 0.0;
    double TotalBurstRatio = 0.0;
    double ResourceFlowRatio = 0.0;
    double TotalResourceFlowRatio = 0.0;

    int FifoIndex = -1;

    // These values are computed at FairShareUpdate and used for diagnostics purposes.
    bool Alive = false;
    double SatisfactionRatio = 0.0;
    double LocalSatisfactionRatio = 0.0;

    TResourceVector GetGuaranteeShare() const
    {
        return StrongGuaranteeShare + ProposedIntegralShare;
    }

    void SetFairShare(const TResourceVector& fairShare)
    {
        FairShare.Total = fairShare;
        FairShare.StrongGuarantee = TResourceVector::Min(fairShare, StrongGuaranteeShare);
        FairShare.IntegralGuarantee = TResourceVector::Min(fairShare - FairShare.StrongGuarantee, ProposedIntegralShare);
        FairShare.WeightProportional = fairShare - FairShare.StrongGuarantee - FairShare.IntegralGuarantee;
    }
};

//! Attributes that are kept between fair share updates.
struct TPersistentAttributes
{
    bool Starving = false;
    TInstant LastNonStarvingTime = TInstant::Now();
    std::optional<TInstant> BelowFairShareSince;
    THistoricUsageAggregator HistoricUsageAggregator;

    TResourceVector BestAllocationShare = TResourceVector::Ones();
    TInstant LastBestAllocationRatioUpdateTime;

    TResourceVolume AccumulatedResourceVolume = {};
    double LastIntegralShareRatio = 0.0;
    TJobResources AppliedResourceLimits = TJobResources::Infinite();

    TDataCenter SchedulingSegmentDataCenter;
    std::optional<TInstant> FailingToScheduleAtDataCenterSince;

    void ResetOnElementEnabled();
};

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

struct TUpdateFairShareContext
{
    std::vector<TError> Errors;
    TInstant Now;

    NProfiling::TCpuDuration PrepareFairShareByFitFactorTotalTime = {};
    NProfiling::TCpuDuration PrepareFairShareByFitFactorOperationsTotalTime = {};
    NProfiling::TCpuDuration PrepareFairShareByFitFactorFifoTotalTime = {};
    NProfiling::TCpuDuration PrepareFairShareByFitFactorNormalTotalTime = {};
    NProfiling::TCpuDuration PrepareMaxFitFactorBySuggestionTotalTime = {};
    NProfiling::TCpuDuration PointwiseMinTotalTime = {};
    NProfiling::TCpuDuration ComposeTotalTime = {};
    NProfiling::TCpuDuration CompressFunctionTotalTime = {};

    TJobResources TotalResourceLimits;

    std::vector<TPoolPtr> RelaxedPools;
    std::vector<TPoolPtr> BurstPools;

    std::optional<TInstant> PreviousUpdateTime;
};

struct TFairSharePostUpdateContext
{
    TEnumIndexedVector<EUnschedulableReason, int> UnschedulableReasons;
        
    TNonOwningOperationElementMap EnabledOperationIdToElement;
    TNonOwningOperationElementMap DisabledOperationIdToElement;
    TNonOwningPoolMap PoolNameToElement;
};

////////////////////////////////////////////////////////////////////////////////

struct TScheduleJobsProfilingCounters
{
    TScheduleJobsProfilingCounters(const NProfiling::TRegistry& profiler);

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

    DEFINE_BYREF_RW_PROPERTY(std::vector<TOperationElementPtr>, BadPackingOperations);

    DEFINE_BYREF_RW_PROPERTY(std::optional<TStageState>, StageState);

public:
    TScheduleJobsContext(
        ISchedulingContextPtr schedulingContext,
        int treeSize,
        std::vector<TSchedulingTagFilter> registeredSchedulingTagFilters,
        bool enableSchedulingInfoLogging,
        const NLogging::TLogger& logger);

    void PrepareForScheduling(const TRootElementPtr& rootElement);

    TDynamicAttributes& DynamicAttributesFor(const TSchedulerElement* element);
    const TDynamicAttributes& DynamicAttributesFor(const TSchedulerElement* element) const;

    TJobResources GetUsageDiscountFor(const TSchedulerElement* element) const;

    void StartStage(TScheduleJobsStage* schedulingStage);

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
    // These fields are input for fair share update and may be used in schedule jobs.
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceDemand);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceUsageAtUpdate);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits, TJobResources::Infinite());

    // These attributes are calculated during fair share update and used further in schedule jobs.
    DEFINE_BYREF_RW_PROPERTY(TSchedulableAttributes, Attributes);

    // These fields are persistent between updates.
    DEFINE_BYREF_RW_PROPERTY(TPersistentAttributes, PersistentAttributes);

    // This field used as both in fair share update and in schedule jobs.
    DEFINE_BYVAL_RW_PROPERTY(int, SchedulingTagFilterIndex, EmptySchedulingTagFilterIndex);

    // These fields are used only during fair share update.
    DEFINE_BYREF_RO_PROPERTY(std::optional<TVectorPiecewiseLinearFunction>, FairShareByFitFactor);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TVectorPiecewiseLinearFunction>, FairShareBySuggestion);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TScalarPiecewiseLinearFunction>, MaxFitFactorBySuggestion);
    
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

    // Used everywhere.
    TCompositeSchedulerElement* Parent_ = nullptr;

    // Assigned in preupdate, used in fair share update.
    TJobResources TotalResourceLimits_;
    bool HasSpecifiedResourceLimits_ = false;
    TInstant StartTime_;

    // Operation specific field, aggregated in composite elements and used in fair share update.
    int PendingJobCount_ = 0;

    // Assigned in preupdate, used in schedule jobs.
    bool Tentative_ = false;

    // Assigned in postupdate, used in schedule jobs.
    int TreeIndex_ = UnassignedTreeIndex;

    // Used only during fair share update.
    bool AreFairShareFunctionsPrepared_ = false;

    // Flag indicates that we can change this fields.
    bool Mutable_ = true;

    const TString TreeId_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerElement
    : public TSchedulerElementFixedState
    , public TRefCounted
{
public:
    //! === Common interface.
    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) = 0;

    virtual TString GetId() const = 0;
    virtual TString GetTreeId() const;

    const NLogging::TLogger& GetLogger() const;
    virtual TString GetLoggingString() const;
    virtual bool AreDetailedLogsEnabled() const;

    TCompositeSchedulerElement* GetMutableParent();
    const TCompositeSchedulerElement* GetParent() const;

    virtual bool IsRoot() const;
    virtual bool IsOperation() const;

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config);

    virtual EIntegralGuaranteeType GetIntegralGuaranteeType() const;
    void InitAccumulatedResourceVolume(TResourceVolume resourceVolume);

    bool IsAlive() const;
    void SetNonAlive();

    TJobResources GetInstantResourceUsage() const;

    // Used for diagnostics and for fair share update.
    virtual TJobResources GetSpecifiedStrongGuaranteeResources() const;
    virtual TResourceLimitsConfigPtr GetStrongGuaranteeResourcesConfig() const;

    // Used for diagnostics and for preemption decisions.
    virtual ESchedulableStatus GetStatus(bool atUpdate = true) const;

    // Used for diagnostics only.
    double GetMaxShareRatio() const;
    virtual TResourceVector GetMaxShare() const = 0;
    TResourceVector GetFairShare() const;
    double GetResourceDominantUsageShareAtUpdate() const;
    // =======================================

    //! === Trunk node interface.
    // Used to manipulate with SchedulingTagFilterIndex.
    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const;
    // =======================================

    //! === Pre fair share update methods.
    //! Prepares attributes that need to be computed in the control thread in a thread-unsafe manner.
    //! For example: TotalResourceLimits.
    virtual void PreUpdateBottomUp(TUpdateFairShareContext* context);

    TJobResources ComputeTotalResourcesOnSuitableNodes() const;
    TJobResources GetTotalResourceLimits() const;
    virtual TJobResources GetSpecifiedResourceLimits() const = 0;

    virtual void DisableNonAliveElements() = 0;

    virtual void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const = 0;
    // =======================================

    //! === Fair share update methods.
    virtual TResourceVector DoUpdateFairShare(double suggestion, TUpdateFairShareContext* context) = 0;

    virtual void UpdateCumulativeAttributes(TUpdateFairShareContext* context);
    virtual void DetermineEffectiveStrongGuaranteeResources();

    TJobResources ComputeResourceLimits() const;
    virtual TResourceVector ComputeLimitsShare() const;

    TResourceVector GetResourceUsageShare() const;

    double GetAccumulatedResourceRatioVolume() const;
    TResourceVolume GetAccumulatedResourceVolume() const;
    double GetIntegralShareRatioByVolume() const;
    void IncreaseHierarchicalIntegralShare(const TResourceVector& delta);

    TInstant GetStartTime() const;
    int GetPendingJobCount() const;

    virtual std::optional<double> GetSpecifiedWeight() const = 0;
    virtual double GetWeight() const;

    virtual double GetFairShareStarvationTolerance() const = 0;
    virtual TDuration GetFairSharePreemptionTimeout() const = 0;

    virtual void AdjustStrongGuarantees();
    virtual void InitIntegralPoolLists(TUpdateFairShareContext* context);

    virtual void PrepareFairShareFunctions(TUpdateFairShareContext* context);
    void ResetFairShareFunctions();
    virtual void PrepareFairShareByFitFactor(TUpdateFairShareContext* context) = 0;
    void PrepareMaxFitFactorBySuggestion(TUpdateFairShareContext* context);
    // =======================================

    //! === Post fair share update methods.
    bool GetStarving() const;
    virtual void SetStarving(bool starving);
    virtual void CheckForStarvation(TInstant now) = 0;

    // Used to compute starvation status.
    bool IsStrictlyDominatesNonBlocked(const TResourceVector& lhs, const TResourceVector& rhs) const;

    virtual void BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context) = 0;

    // Publishes fair share and updates preemptable job lists of operations.
    virtual void PublishFairShareAndUpdatePreemption() = 0;
    virtual void UpdatePreemptionAttributes();

    // Manage operation segments.
    virtual void CollectOperationSchedulingSegmentContexts(
        THashMap<TOperationId, TOperationSchedulingSegmentContext>* operationContexts) const = 0;
    virtual void ApplyOperationSchedulingSegmentChanges(
        const THashMap<TOperationId, TOperationSchedulingSegmentContext>& operationContexts) = 0;

    //! This method reuses common code with schedule jobs logic to calculate dynamic attributes.
    virtual void UpdateSchedulableAttributesFromDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap);

    // Enumerates elements of the tree using inorder traversal. Returns first unused index.
    virtual int EnumerateElements(int startIndex, bool isSchedulableValueFilter);

    int GetTreeIndex() const;
    void SetTreeIndex(int treeIndex);

    virtual void MarkImmutable();

    virtual void BuildElementMapping(TFairSharePostUpdateContext* context) = 0;
    // =======================================

    //! === Schedule jobs related methods.
    virtual void PrescheduleJob(
        TScheduleJobsContext* context,
        EPrescheduleJobOperationCriterion operationCriterion,
        bool aggressiveStarvationEnabled) = 0;
    virtual void UpdateDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap) = 0;

    double ComputeLocalSatisfactionRatio(const TJobResources& resourceUsage) const;

    virtual TFairShareScheduleJobResult ScheduleJob(TScheduleJobsContext* context, bool ignorePacking) = 0;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const = 0;
    virtual bool HasAggressivelyStarvingElements(TScheduleJobsContext* context, bool aggressiveStarvationEnabled) const = 0;

    // Returns resource usage observed in current heartbeat.
    TJobResources GetCurrentResourceUsage(const TDynamicAttributesList& dynamicAttributesList) const;

    virtual void CalculateCurrentResourceUsage(TScheduleJobsContext* context) = 0;
    void IncreaseHierarchicalResourceUsage(const TJobResources& delta);

    virtual bool IsSchedulable() const = 0;

    bool IsResourceBlocked(EJobResourceType resource) const;
    bool AreAllResourcesBlocked() const;

    bool IsActive(const TDynamicAttributesList& dynamicAttributesList) const;

    bool AreResourceLimitsViolated() const;
    // =======================================

    //! === Other mehtods based on snapshotted version of element.
    virtual void BuildResourceMetering(const std::optional<TMeteringKey>& parentKey, TMeteringMap* meteringMap) const;

private:
    TResourceTreeElementPtr ResourceTreeElement_;

protected:
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
        TCompositeSchedulerElement* clonedParent);

    ISchedulerStrategyHost* GetHost() const;

    ESchedulableStatus GetStatusImpl(double defaultTolerance, bool atUpdate) const;

    void CheckForStarvationImpl(TDuration fairSharePreemptionTimeout, TInstant now);

    void SetOperationAlert(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout);

    TString GetLoggingAttributesString() const;

    TJobResources GetLocalAvailableResourceLimits(const TScheduleJobsContext& context) const;

    bool CheckDemand(const TJobResources& delta, const TScheduleJobsContext& context);

    TResourceVector GetVectorSuggestion(double suggestion) const;

protected:
    NLogging::TLogger Logger;

private:
    void UpdateAttributes();

    friend class TCompositeSchedulerElement;
    friend class TOperationElement;
    friend class TPool;
};

DEFINE_REFCOUNTED_TYPE(TSchedulerElement)

////////////////////////////////////////////////////////////////////////////////

class TCompositeSchedulerElementFixedState
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

class TCompositeSchedulerElement
    : public TSchedulerElement
    , public TCompositeSchedulerElementFixedState
{
public:
    TCompositeSchedulerElement(
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        NProfiling::TTagId profilingTag,
        const TString& treeId,
        const TString& id,
        EResourceTreeElementKind elementKind,
        const NLogging::TLogger& logger);
    TCompositeSchedulerElement(
        const TCompositeSchedulerElement& other,
        TCompositeSchedulerElement* clonedParent);


    //! === Common interface.
    void AddChild(TSchedulerElement* child, bool enabled = true);
    void EnableChild(const TSchedulerElementPtr& child);
    void DisableChild(const TSchedulerElementPtr& child);
    void RemoveChild(TSchedulerElement* child);
    bool IsEnabledChild(TSchedulerElement* child);

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;

    // For testing only.
    std::vector<TSchedulerElementPtr> GetEnabledChildren();
    std::vector<TSchedulerElementPtr> GetDisabledChildren();
    // =======================================

    //! === Trunk node interface.
    virtual int GetMaxOperationCount() const = 0;
    virtual int GetMaxRunningOperationCount() const = 0;
    int GetAvailableRunningOperationCount() const;

    void IncreaseOperationCount(int delta);
    void IncreaseRunningOperationCount(int delta);

    virtual bool IsExplicit() const;
    virtual bool IsDefaultConfigured() const = 0;
    virtual bool AreImmediateOperationsForbidden() const = 0;

    bool IsEmpty() const;
    // =======================================

    //! === Pre fair share update methods.
    virtual void PreUpdateBottomUp(TUpdateFairShareContext* context) override;
    virtual void DisableNonAliveElements() override;

    virtual void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const override;
    // =======================================

    //! === Fair share update methods.
    virtual TResourceVector DoUpdateFairShare(double suggestion, TUpdateFairShareContext* context) override;

    virtual void DetermineEffectiveStrongGuaranteeResources() override;

    virtual void UpdateCumulativeAttributes(TUpdateFairShareContext* context) override;

    TResourceVolume GetIntegralPoolCapacity() const;

    TResourceVector GetHierarchicalAvailableLimitsShare() const;

    virtual double GetSpecifiedBurstRatio() const = 0;
    virtual double GetSpecifiedResourceFlowRatio() const = 0;

    virtual void PrepareFairShareFunctions(TUpdateFairShareContext* context) override;
    virtual void PrepareFairShareByFitFactor(TUpdateFairShareContext* context) override;

    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const = 0;

    virtual bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const = 0;
    virtual THistoricUsageAggregationParameters GetHistoricUsageAggregationParameters() const = 0;

    virtual double GetFairShareStarvationToleranceLimit() const;
    virtual TDuration GetFairSharePreemptionTimeoutLimit() const;
    // =======================================

    //! === Post fair share update methods.
    virtual void BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context) override;

    virtual void PublishFairShareAndUpdatePreemption() override;
    virtual void UpdatePreemptionAttributes() override;

    virtual void UpdateSchedulableAttributesFromDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap) override;

    virtual void CollectOperationSchedulingSegmentContexts(
        THashMap<TOperationId, TOperationSchedulingSegmentContext>* operationContexts) const override;
    virtual void ApplyOperationSchedulingSegmentChanges(
        const THashMap<TOperationId, TOperationSchedulingSegmentContext>& operationContexts) override;

    virtual int EnumerateElements(int startIndex, bool isSchedulableValueFilter) override;

    virtual void MarkImmutable() override;

    virtual void BuildElementMapping(TFairSharePostUpdateContext* context) override;
    // =======================================

    //! === Schedule jobs related methods.
    virtual void PrescheduleJob(
        TScheduleJobsContext* context,
        EPrescheduleJobOperationCriterion operationCriterion,
        bool aggressiveStarvationEnabled) override;
    virtual void UpdateDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap) override;

    virtual TFairShareScheduleJobResult ScheduleJob(TScheduleJobsContext* context, bool ignorePacking) override;

    virtual bool IsAggressiveStarvationEnabled() const;
    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;
    virtual bool HasAggressivelyStarvingElements(TScheduleJobsContext* context, bool aggressiveStarvationEnabled) const override;

    virtual void CalculateCurrentResourceUsage(TScheduleJobsContext* context) override;

    virtual bool IsSchedulable() const override;

    void InitializeChildHeap(TScheduleJobsContext* context);
    void UpdateChild(TScheduleJobsContext* context, TSchedulerElement* child);
    // =======================================

    virtual THashSet<TString> GetAllowedProfilingTags() const = 0;

    // Mode_ is used in fair share update, also we publish this parameter to orchid.
    ESchedulingMode GetMode() const;

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

    //! === Fair share update methods.
    virtual void InitIntegralPoolLists(TUpdateFairShareContext* context) override;
    virtual void AdjustStrongGuarantees() override;

private:
    /// strict_mode = true means that a caller guarantees that the sum predicate is true at least for fit factor = 0.0.
    /// strict_mode = false means that if the sum predicate is false for any fit factor, we fit children to the least possible sum
    /// (i. e. use fit factor = 0.0)
    template <class TValue, class TGetter, class TSetter>
    TValue ComputeByFitting(
        const TGetter& getter,
        const TSetter& setter,
        TValue maxSum,
        bool strictMode = true);


    using TChildSuggestions = std::vector<double>;
    TChildSuggestions GetEnabledChildSuggestionsFifo(double fitFactor);
    TChildSuggestions GetEnabledChildSuggestionsNormal(double fitFactor);

    TSchedulerElement* GetBestActiveChild(const TDynamicAttributesList& dynamicAttributesList, const TChildHeapMap& childHeapMap) const;
    TSchedulerElement* GetBestActiveChildFifo(const TDynamicAttributesList& dynamicAttributesList) const;
    TSchedulerElement* GetBestActiveChildFairShare(const TDynamicAttributesList& dynamicAttributesList) const;

    void PrepareFifoPool();
    bool HasHigherPriorityInFifoMode(const TSchedulerElement* lhs, const TSchedulerElement* rhs) const;

    void PrepareFairShareByFitFactorFifo(TUpdateFairShareContext* context);
    void PrepareFairShareByFitFactorNormal(TUpdateFairShareContext* context);

    static double GetMinChildWeight(const TChildList& children);
};

DEFINE_REFCOUNTED_TYPE(TCompositeSchedulerElement)

////////////////////////////////////////////////////////////////////////////////

class TPoolFixedState
{
protected:
    explicit TPoolFixedState(TString id);

    const TString Id_;
    // Used only in trunk node.
    bool DefaultConfigured_ = true;
    bool EphemeralInDefaultParentPool_ = false;
    std::optional<TString> UserName_;

    // Used in preupdate.
    TSchedulingTagFilter SchedulingTagFilter_;
};

////////////////////////////////////////////////////////////////////////////////

class TPool
    : public TCompositeSchedulerElement
    , public TPoolFixedState
{
public:
    TPool(
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
        const TString& id,
        TPoolConfigPtr config,
        bool defaultConfigured,
        TFairShareStrategyTreeConfigPtr treeConfig,
        NProfiling::TTagId profilingTag,
        const TString& treeId,
        const NLogging::TLogger& logger);
    TPool(
        const TPool& other,
        TCompositeSchedulerElement* clonedParent);

    //! === Common interface.
    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) override;

    virtual TString GetId() const override;

    virtual bool AreDetailedLogsEnabled() const override;

    void AttachParent(TCompositeSchedulerElement* newParent);
    void ChangeParent(TCompositeSchedulerElement* newParent);
    void DetachParent();

    virtual EIntegralGuaranteeType GetIntegralGuaranteeType() const override;

    virtual TResourceLimitsConfigPtr GetStrongGuaranteeResourcesConfig() const override;

    virtual ESchedulableStatus GetStatus(bool atUpdate = true) const override;

    // Used for diagnostics only.
    virtual TResourceVector GetMaxShare() const override;
    // =======================================

    //! === Trunk node interface.
    TPoolConfigPtr GetConfig() const;
    void SetConfig(TPoolConfigPtr config);
    void SetDefaultConfig();

    void SetUserName(const std::optional<TString>& userName);
    const std::optional<TString>& GetUserName() const;

    virtual int GetMaxOperationCount() const override;
    virtual int GetMaxRunningOperationCount() const override;

    void SetEphemeralInDefaultParentPool();
    bool IsEphemeralInDefaultParentPool() const;

    virtual bool IsExplicit() const override;
    virtual bool IsDefaultConfigured() const override;
    virtual bool AreImmediateOperationsForbidden() const override;

    // Used to manipulate with SchedulingTagFilterIndex.
    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;
    // =======================================

    //! === Pre fair share update methods.
    virtual TJobResources GetSpecifiedResourceLimits() const override;
    // =======================================

    //! === Fair share update methods.
    virtual double GetSpecifiedBurstRatio() const override;
    virtual double GetSpecifiedResourceFlowRatio() const override;

    virtual std::optional<double> GetSpecifiedWeight() const override;

    virtual bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const override;
    virtual THistoricUsageAggregationParameters GetHistoricUsageAggregationParameters() const override;

    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual double GetFairShareStarvationToleranceLimit() const override;
    virtual TDuration GetFairSharePreemptionTimeoutLimit() const override;
    
    virtual void InitIntegralPoolLists(TUpdateFairShareContext* context) override;

    void UpdateAccumulatedResourceVolume(TDuration periodSinceLastUpdate);

    void ApplyLimitsForRelaxedPool();
    // =======================================

    //! === Post fair share update methods.
    virtual void SetStarving(bool starving) override;
    virtual void CheckForStarvation(TInstant now) override;

    virtual void BuildElementMapping(TFairSharePostUpdateContext* context) override;
    // =======================================

    //! === Schedule jobs related methods.
    virtual bool IsAggressiveStarvationEnabled() const override;
    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;
    // =======================================

    //! === Other mehtods based on snapshotted version of element.
    virtual void BuildResourceMetering(const std::optional<TMeteringKey>& parentKey, TMeteringMap* meteringMap) const override;

    // =======================================
    virtual THashSet<TString> GetAllowedProfilingTags() const override;

private:
    TPoolConfigPtr Config_;

    void DoSetConfig(TPoolConfigPtr newConfig);

    TResourceVector GetIntegralShareLimitForRelaxedPool() const;
};

DEFINE_REFCOUNTED_TYPE(TPool)

////////////////////////////////////////////////////////////////////////////////

class TOperationElementFixedState
{
public:
    // Used by trunk node.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TString>, PendingByPool);

    DEFINE_BYREF_RW_PROPERTY(std::optional<ESchedulingSegment>, SchedulingSegment);
    DEFINE_BYREF_RW_PROPERTY(std::optional<THashSet<TString>>, SpecifiedSchedulingSegmentDataCenters);

protected:
    TOperationElementFixedState(
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

class TOperationElementSharedState
    : public TRefCounted
{
public:
    TOperationElementSharedState(
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
        TOperationElement* operationElement);

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
    TEnumIndexedVector<EJobResourceType, int> GetMinNeededResourcesUnsatisfiedCount() const;

    void OnOperationDeactivated(const TScheduleJobsContext& context, EDeactivationReason reason);
    TEnumIndexedVector<EDeactivationReason, int> GetDeactivationReasons() const;
    void ResetDeactivationReasonsFromLastNonStarvingTime();
    TEnumIndexedVector<EDeactivationReason, int> GetDeactivationReasonsFromLastNonStarvingTime() const;

    TInstant GetLastScheduleJobSuccessTime() const;

    TJobResources Disable();
    void Enable();
    bool Enabled();

    void RecordHeartbeat(
        const TPackingHeartbeatSnapshot& heartbeatSnapshot,
        const TFairShareStrategyPackingConfigPtr& config);
    bool CheckPacking(
        const TOperationElement* operationElement,
        const TPackingHeartbeatSnapshot& heartbeatSnapshot,
        const TJobResourcesWithQuota& jobResources,
        const TJobResources& totalResourceLimits,
        const TFairShareStrategyPackingConfigPtr& config);

private:
    using TJobIdList = std::list<TJobId>;

    TJobIdList NonpreemptableJobs_;
    TJobIdList AggressivelyPreemptableJobs_;
    TJobIdList PreemptableJobs_;

    std::atomic<bool> Preemptable_ = {true};

    std::atomic<int> RunningJobCount_ = {0};
    TJobResources NonpreemptableResourceUsage_;
    TJobResources AggressivelyPreemptableResourceUsage_;
    TJobResources PreemptableResourceUsage_;

    std::atomic<int> UpdatePreemptableJobsListCount_ = {0};
    const int UpdatePreemptableJobsListLoggingPeriod_;

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

        //! Determines whether job belongs to list of preemptable or aggressively preemptable jobs of operation.
        bool Preemptable;

        //! Determines whether job belongs to list of preemptable (but not aggressively preemptable) jobs of operation.
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
        char Padding[64];
    };
    std::array<TStateShard, MaxNodeShardCount> StateShards_;

    bool Enabled_ = false;

    TPackingStatistics HeartbeatStatistics_;

    TJobResources SetJobResourceUsage(TJobProperties* properties, const TJobResources& resources);

    TJobProperties* GetJobProperties(TJobId jobId);
    const TJobProperties* GetJobProperties(TJobId jobId) const;
};

DEFINE_REFCOUNTED_TYPE(TOperationElementSharedState)

////////////////////////////////////////////////////////////////////////////////

class TOperationElement
    : public TSchedulerElement
    , public TOperationElementFixedState
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TOperationFairShareTreeRuntimeParametersPtr, RuntimeParameters);

    DEFINE_BYVAL_RO_PROPERTY(TStrategyOperationSpecPtr, Spec);

public:
    TOperationElement(
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
    TOperationElement(
        const TOperationElement& other,
        TCompositeSchedulerElement* clonedParent);

    //! === Common interface.
    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) override;

    virtual TString GetId() const override;

    virtual TString GetLoggingString() const override;
    virtual bool AreDetailedLogsEnabled() const override;

    virtual bool IsOperation() const override;

    virtual ESchedulableStatus GetStatus(bool atUpdate = true) const override;

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;
    void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config);

    void InitOrUpdateSchedulingSegment(ESegmentedSchedulingMode mode);

    virtual TResourceLimitsConfigPtr GetStrongGuaranteeResourcesConfig() const override;
    virtual TResourceVector GetMaxShare() const override;
    // =======================================

    //! === Trunk node interface.
    int GetSlotIndex() const;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    TString GetUserName() const;

    void Disable(bool markAsNonAlive);
    void Enable();

    void MarkOperationRunningInPool();
    bool IsOperationRunningInPool() const;

    void MarkPendingBy(TCompositeSchedulerElement* violatedPool);

    void AttachParent(TCompositeSchedulerElement* newParent, int slotIndex);
    void ChangeParent(TCompositeSchedulerElement* newParent, int slotIndex);
    void DetachParent();
    // =======================================

    //! === Shared state interface.
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

    TPreemptionStatusStatisticsVector GetPreemptionStatusStatistics() const;

    TInstant GetLastScheduleJobSuccessTime() const;
    // =======================================

    //! === Pre fair share update methods.
    virtual void PreUpdateBottomUp(TUpdateFairShareContext* context) override;
    virtual void DisableNonAliveElements() override;

    virtual void CollectResourceTreeOperationElements(std::vector<TResourceTreeElementPtr>* elements) const override;
    // =======================================

    //! === Fair share update methods.
    virtual TResourceVector DoUpdateFairShare(double suggestion, TUpdateFairShareContext* context) override;

    virtual TResourceVector ComputeLimitsShare() const override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual std::optional<double> GetSpecifiedWeight() const override;

    virtual void PrepareFairShareByFitFactor(TUpdateFairShareContext* context) override;

    // =======================================

    //! === Post fair share update methods.
    virtual void SetStarving(bool starving) override;
    virtual void CheckForStarvation(TInstant now) override;

    TInstant GetLastNonStarvingTime() const;
    
    virtual void BuildSchedulableChildrenLists(TFairSharePostUpdateContext* context) override;

    virtual void PublishFairShareAndUpdatePreemption() override;
    virtual void UpdatePreemptionAttributes() override;

    virtual void CollectOperationSchedulingSegmentContexts(
        THashMap<TOperationId, TOperationSchedulingSegmentContext>* operationContexts) const override;
    virtual void ApplyOperationSchedulingSegmentChanges(
        const THashMap<TOperationId, TOperationSchedulingSegmentContext>& operationContexts) override;

    virtual void BuildElementMapping(TFairSharePostUpdateContext* context) override;
    // =======================================

    //! === Schedule jobs related methods.
    virtual void PrescheduleJob(
        TScheduleJobsContext* context,
        EPrescheduleJobOperationCriterion operationCriterion,
        bool aggressiveStarvationEnabled) override;
    virtual void UpdateDynamicAttributes(
        TDynamicAttributesList* dynamicAttributesList,
        const TChildHeapMap& childHeapMap) override;

    virtual void CalculateCurrentResourceUsage(TScheduleJobsContext* context) override;

    virtual TFairShareScheduleJobResult ScheduleJob(TScheduleJobsContext* context, bool ignorePacking) override;

    void ActivateOperation(TScheduleJobsContext* context);
    void DeactivateOperation(TScheduleJobsContext* context, EDeactivationReason reason);

    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;
    virtual bool HasAggressivelyStarvingElements(TScheduleJobsContext* context, bool aggressiveStarvationEnabled) const override;

    bool IsPreemptionAllowed(
        bool isAggressivePreemption,
        const TDynamicAttributesList& dynamicAttributesList,
        const TFairShareStrategyTreeConfigPtr& config) const;

    virtual bool IsSchedulable() const override;

    TEnumIndexedVector<EJobResourceType, int> GetMinNeededResourcesUnsatisfiedCount() const;

    bool IsLimitingAncestorCheckEnabled() const;

    bool IsSchedulingSegmentCompatibleWithNode(ESchedulingSegment nodeSegment, const TDataCenter& nodeDataCenter) const;
    // =======================================

    std::optional<TString> GetCustomProfilingTag() const;


private:
    const TOperationElementSharedStatePtr OperationElementSharedState_;
    const TFairShareStrategyOperationControllerPtr Controller_;

    //! === Controller related methods.
    bool IsMaxScheduleJobCallsViolated() const;
    bool IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const;
    // =======================================

    //! === Pre fair share update methods.
    std::optional<EUnschedulableReason> ComputeUnschedulableReason() const;

    TJobResources ComputeResourceDemand() const;

    virtual TJobResources GetSpecifiedResourceLimits() const override;
    // =======================================

    //! === Post fair share update methods.
    void UpdatePreemptableJobsList();
    // =======================================

    //! === Schedule jobs related methods.
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
    // =======================================
};

DEFINE_REFCOUNTED_TYPE(TOperationElement)

////////////////////////////////////////////////////////////////////////////////

class TRootElementFixedState
{
public:
    // TODO(ignat): move it to TFairShareTreeSnapshotImpl.
    DEFINE_BYVAL_RO_PROPERTY(int, TreeSize);
    DEFINE_BYVAL_RO_PROPERTY(int, SchedulableElementCount);
};

class TRootElement
    : public TCompositeSchedulerElement
    , public TRootElementFixedState
{
public:
    TRootElement(
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        NProfiling::TTagId profilingTag,
        const TString& treeId,
        const NLogging::TLogger& logger);
    TRootElement(const TRootElement& other);
    
    //! === Common methods.
    virtual TString GetId() const override;

    TRootElementPtr Clone();

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) override;
    
    virtual bool IsRoot() const override;

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;
    
    // Used for diagnostics purposes.
    virtual TJobResources GetSpecifiedStrongGuaranteeResources() const override;
    virtual TResourceVector GetMaxShare() const override;
    // =======================================

    //! === Trunk node methods.
    virtual int GetMaxRunningOperationCount() const override;
    virtual int GetMaxOperationCount() const override;
    
    virtual bool IsDefaultConfigured() const override;
    virtual bool AreImmediateOperationsForbidden() const override;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;
    // =======================================

    //! === Pre fair share update methods.
    // Computes various lightweight attributes in the tree. Must be called in control thread.
    void PreUpdate(TUpdateFairShareContext* context);

    virtual TJobResources GetSpecifiedResourceLimits() const override;
    // =======================================
    
    //! === Fair share update methods.
    //! Computes fair shares in the tree. Thread-safe.
    void Update(TUpdateFairShareContext* context);

    void UpdateFairShare(TUpdateFairShareContext* context);
    void UpdateRootFairShare();

    virtual std::optional<double> GetSpecifiedWeight() const override;
    
    virtual void DetermineEffectiveStrongGuaranteeResources() override;
    
    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const override;
    
    virtual bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const override;
    virtual THistoricUsageAggregationParameters GetHistoricUsageAggregationParameters() const override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;
    // =======================================
    
    //! === Post share update methods.
    void PostUpdate(
        TFairSharePostUpdateContext* postUpdateContext,
        TManageTreeSchedulingSegmentsContext* manageSegmentsContext);

    virtual void CheckForStarvation(TInstant now) override;
    // =======================================

    //! === Schedule jobs methods.
    virtual bool IsAggressiveStarvationEnabled() const override;

    // =======================================

    //! === Other methods.
    virtual THashSet<TString> GetAllowedProfilingTags() const override;

    virtual void BuildResourceMetering(const std::optional<TMeteringKey>& parentKey, TMeteringMap* meteringMap) const override;

    void BuildResourceDistributionInfo(NYTree::TFluentMap fluent) const;

private:
    virtual void UpdateCumulativeAttributes(TUpdateFairShareContext* context) override;
    void ValidateAndAdjustSpecifiedGuarantees(TUpdateFairShareContext* context);
    void UpdateBurstPoolIntegralShares(TUpdateFairShareContext* context);
    void UpdateRelaxedPoolIntegralShares(TUpdateFairShareContext* context, const TResourceVector& availableShare);
    TResourceVector EstimateAvailableShare();
    void ConsumeAndRefillIntegralPools(TUpdateFairShareContext* context);
    void ManageSchedulingSegments(TManageTreeSchedulingSegmentsContext* manageSegmentsContext);

    virtual double GetSpecifiedBurstRatio() const override;
    virtual double GetSpecifiedResourceFlowRatio() const override;
};

DEFINE_REFCOUNTED_TYPE(TRootElement)

////////////////////////////////////////////////////////////////////////////////

class TChildHeap
{
public:
    using TComparator = std::function<bool(TSchedulerElement*, TSchedulerElement*)>;

    TChildHeap(
        const std::vector<TSchedulerElementPtr>& children,
        TDynamicAttributesList* dynamicAttributesList,
        TComparator comparator);
    TSchedulerElement* GetTop() const;
    void Update(TSchedulerElement* child);

    // For testing purposes.
    const std::vector<TSchedulerElement*>& GetHeap() const;

private:
    TDynamicAttributesList& DynamicAttributesList_;
    const TComparator Comparator_;

    std::vector<TSchedulerElement*> ChildHeap_;

    void OnAssign(size_t offset);
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
