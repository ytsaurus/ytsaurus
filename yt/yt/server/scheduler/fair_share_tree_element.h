#pragma once

#include "fair_share_strategy_operation_controller.h"
#include "job.h"
#include "private.h"
#include "resource_vector.h"
#include "resource_tree.h"
#include "resource_tree_element.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "fair_share_strategy_operation_controller.h"
#include "fair_share_tree_snapshot.h"
#include "packing.h"

#include <yt/server/lib/scheduler/config.h>
#include <yt/server/lib/scheduler/job_metrics.h>
#include <yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/server/lib/scheduler/resource_metering.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/spinlock.h>

#include <yt/core/misc/historic_usage_aggregator.h>

#include <yt/core/profiling/metrics_accumulator.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

static constexpr int UnassignedTreeIndex = -1;
static constexpr int EmptySchedulingTagFilterIndex = -1;

////////////////////////////////////////////////////////////////////////////////

static constexpr double InfiniteSatisfactionRatio = 1e+9;

////////////////////////////////////////////////////////////////////////////////

using TRawOperationElementMap = THashMap<TOperationId, TOperationElement*>;
using TOperationElementMap = THashMap<TOperationId, TOperationElementPtr>;

using TRawPoolMap = THashMap<TString, TPool*>;
using TPoolMap = THashMap<TString, TPoolPtr>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPrescheduleJobOperationCriterion,
    (All)
    (AggressivelyStarvingOnly)
    (StarvingOnly)
);

////////////////////////////////////////////////////////////////////////////////

struct TScheduleJobsProfilingCounters
{
    TScheduleJobsProfilingCounters(const NProfiling::TRegistry& profiler);

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

    double BurstRatio = 0.0;
    double TotalBurstRatio = 0.0;
    double ResourceFlowRatio = 0.0;
    double TotalResourceFlowRatio = 0.0;

    int FifoIndex = -1;

    double AdjustedFairShareStarvationTolerance = 1.0;
    TDuration AdjustedFairSharePreemptionTimeout;

    // These values are computed at FairShareUpdate and used for diagnostics purposes.
    bool Alive = false;
    double SatisfactionRatio = 0.0;
    double LocalSatisfactionRatio = 0.0;


    // TODO(eshcherbin): Rethink whether we want to use |MaxComponent| here or the share of |DominantResource|.
    TResourceVector GetFairShare() const
    {
        return FairShare.Total;
    }

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

//! Attributes that persistent between fair share updates.
struct TPersistentAttributes
{
    bool Starving = false;
    TInstant LastNonStarvingTime = TInstant::Now();
    std::optional<TInstant> BelowFairShareSince;
    THistoricUsageAggregator HistoricUsageAggregator;

    TResourceVector BestAllocationShare = TResourceVector::Ones();
    TInstant LastBestAllocationRatioUpdateTime;

    TJobResources AccumulatedResourceVolume = {};
    double LastIntegralShareRatio = 0.0;
    TJobResources AppliedResourceLimits = TJobResources::Infinite();

    // NB: we don't want to reset all attributes.
    void ResetOnElementEnabled()
    {
        auto resetAttributes = TPersistentAttributes();
        resetAttributes.AccumulatedResourceVolume = AccumulatedResourceVolume;
        resetAttributes.LastNonStarvingTime = TInstant::Now();
        *this = resetAttributes;
    }
};

class TChildHeap;

static const int InvalidHeapIndex = -1;

struct TDynamicAttributes
{
    double SatisfactionRatio = 0.0;
    bool Active = false;
    TSchedulerElement* BestLeafDescendant = nullptr;
    TJobResources ResourceUsageDiscount;

    std::unique_ptr<TChildHeap> ChildHeap;
    int HeapIndex = InvalidHeapIndex;
};

using TDynamicAttributesList = std::vector<TDynamicAttributes>;

////////////////////////////////////////////////////////////////////////////////

struct TUpdateFairShareContext
{
    std::vector<TError> Errors;
    THashMap<TString, int> ElementIndexes;
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
    TEnumIndexedVector<EUnschedulableReason, int> UnschedulableReasons;

    std::vector<TPoolPtr> RelaxedPools;
    std::vector<TPoolPtr> BurstPools;

    std::optional<TInstant> PreviousUpdateTime;
};

////////////////////////////////////////////////////////////////////////////////

struct TFairShareSchedulingStage
{
    TFairShareSchedulingStage(TString loggingName, TScheduleJobsProfilingCounters profilingCounters);

    const TString LoggingName;
    TScheduleJobsProfilingCounters ProfilingCounters;
};

////////////////////////////////////////////////////////////////////////////////

class TFairShareContext
{
private:
    struct TStageState
    {
        explicit TStageState(TFairShareSchedulingStage* schedulingStage);

        TFairShareSchedulingStage* const SchedulingStage;

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

    DEFINE_BYREF_RO_PROPERTY(ISchedulingContextPtr, SchedulingContext);

    DEFINE_BYVAL_RW_PROPERTY(std::optional<bool>, HasAggressivelyStarvingElements);

    DEFINE_BYREF_RW_PROPERTY(TFairShareSchedulingStatistics, SchedulingStatistics);

    DEFINE_BYREF_RW_PROPERTY(std::vector<TOperationElementPtr>, BadPackingOperations);

    DEFINE_BYREF_RW_PROPERTY(std::optional<TStageState>, StageState);

public:
    TFairShareContext(
        ISchedulingContextPtr schedulingContext,
        int treeSize,
        std::vector<TSchedulingTagFilter> registeredSchedulingTagFilters,
        bool enableSchedulingInfoLogging,
        const NLogging::TLogger& logger);

    void PrepareForScheduling();

    TDynamicAttributes& DynamicAttributesFor(const TSchedulerElement* element);
    const TDynamicAttributes& DynamicAttributesFor(const TSchedulerElement* element) const;

    void StartStage(TFairShareSchedulingStage* schedulingStage);

    void ProfileStageTimingsAndLogStatistics();

    void FinishStage();

private:
    const int TreeSize_;

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
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceDemand);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceUsageAtUpdate);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits, TJobResources::Infinite());
    DEFINE_BYREF_RW_PROPERTY(TSchedulableAttributes, Attributes);
    DEFINE_BYREF_RW_PROPERTY(TPersistentAttributes, PersistentAttributes);
    DEFINE_BYVAL_RW_PROPERTY(int, SchedulingTagFilterIndex, EmptySchedulingTagFilterIndex);

    DEFINE_BYREF_RO_PROPERTY(std::optional<TVectorPiecewiseLinearFunction>, FairShareByFitFactor);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TVectorPiecewiseLinearFunction>, FairShareBySuggestion);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TScalarPiecewiseLinearFunction>, MaxFitFactorBySuggestion);

protected:
    TSchedulerElementFixedState(
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        TString treeId);

    ISchedulerStrategyHost* const Host_;
    IFairShareTreeHost* const TreeHost_;

    TFairShareStrategyTreeConfigPtr TreeConfig_;

    TCompositeSchedulerElement* Parent_ = nullptr;

    TJobResources TotalResourceLimits_;

    int PendingJobCount_ = 0;
    TInstant StartTime_;

    int TreeIndex_ = UnassignedTreeIndex;

    bool AreFairShareFunctionsPrepared_ = false;

    // TODO(ignat): it is not used anymore, consider deleting.
    bool Cloned_ = false;
    bool Mutable_ = true;

    const TString TreeId_;
};

////////////////////////////////////////////////////////////////////////////////

struct TFairShareScheduleJobResult
{
    TFairShareScheduleJobResult(bool finished, bool scheduled)
        : Finished(finished)
        , Scheduled(scheduled)
    { }

    bool Finished;
    bool Scheduled;
};

class TSchedulerElement
    : public TSchedulerElementFixedState
    , public TRefCounted
{
public:
    //! Enumerates elements of the tree using inorder traversal. Returns first unused index.
    virtual int EnumerateElements(int startIndex, TUpdateFairShareContext* context);

    int GetTreeIndex() const;
    void SetTreeIndex(int treeIndex);

    virtual void DisableNonAliveElements() = 0;

    virtual void MarkImmutable();

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config);

    //! Prepares attributes that need to be computed in the control thread in a thread-unsafe manner.
    //! For example: TotalResourceLimits.
    virtual void PreUpdateBottomUp(TUpdateFairShareContext* context);

    virtual void UpdateCumulativeAttributes(TUpdateFairShareContext* context);

    //! Publishes fair share and updates preemptable job lists of operations.
    virtual void PublishFairShareAndUpdatePreemption() = 0;

    virtual void UpdatePreemptionAttributes();
    virtual void UpdateSchedulableAttributesFromDynamicAttributes(TDynamicAttributesList* dynamicAttributesList);

    virtual void UpdateDynamicAttributes(TDynamicAttributesList* dynamicAttributesList) = 0;

    virtual void PrescheduleJob(TFairShareContext* context, EPrescheduleJobOperationCriterion operationCriterion, bool aggressiveStarvationEnabled) = 0;
    virtual TFairShareScheduleJobResult ScheduleJob(TFairShareContext* context, bool ignorePacking) = 0;

    virtual bool HasAggressivelyStarvingElements(TFairShareContext* context, bool aggressiveStarvationEnabled) const = 0;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const;

    virtual bool IsRoot() const;
    virtual bool IsOperation() const;
    virtual TPool* AsPool();

    virtual TString GetLoggingString() const;

    bool IsActive(const TDynamicAttributesList& dynamicAttributesList) const;

    virtual bool IsSchedulable() const = 0;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const = 0;

    bool IsAlive() const;
    void SetNonAlive();

    TResourceVector GetFairShare() const;

    virtual TString GetId() const = 0;

    virtual std::optional<double> GetSpecifiedWeight() const = 0;
    virtual double GetWeight() const;

    virtual TJobResources GetStrongGuaranteeResources() const = 0;

    virtual TResourceVector GetMaxShare() const = 0;

    // Used for diagnostics.
    double GetMaxShareRatio() const;

    virtual EIntegralGuaranteeType GetIntegralGuaranteeType() const;
    double GetAccumulatedResourceRatioVolume() const;
    TJobResources GetAccumulatedResourceVolume() const;
    void InitAccumulatedResourceVolume(TJobResources resourceVolume);
    double GetIntegralShareRatioByVolume() const;
    void IncreaseHierarchicalIntegralShare(const TResourceVector& delta);

    virtual double GetFairShareStarvationTolerance() const = 0;
    virtual TDuration GetFairSharePreemptionTimeout() const = 0;

    TCompositeSchedulerElement* GetMutableParent();
    const TCompositeSchedulerElement* GetParent() const;

    TInstant GetStartTime() const;
    int GetPendingJobCount() const;

    virtual ESchedulableStatus GetStatus(bool atUpdate = false) const;

    bool GetStarving() const;
    virtual void SetStarving(bool starving);
    virtual void CheckForStarvation(TInstant now) = 0;

    TJobResources GetInstantResourceUsage() const;
    TJobMetrics GetJobMetrics() const;
    TResourceVector GetResourceUsageShare() const;
    TResourceVector GetResourceUsageShareWithPrecommit() const;

    // Used for diagnostics.
    double GetResourceDominantUsageShareAtUpdate() const;

    virtual TString GetTreeId() const;

    void IncreaseHierarchicalResourceUsage(const TJobResources& delta);

    virtual void BuildElementMapping(
        TRawOperationElementMap* enabledOperationMap,
        TRawOperationElementMap* disabledOperationMap,
        TRawPoolMap* poolMap) = 0;

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) = 0;

    double ComputeLocalSatisfactionRatio() const;

    const NLogging::TLogger& GetLogger() const;

    virtual void PrepareFairShareFunctions(TUpdateFairShareContext* context);
    void ResetFairShareFunctions();

    virtual TResourceVector DoUpdateFairShare(double suggestion, TUpdateFairShareContext* context) = 0;

    virtual void PrepareFairShareByFitFactor(TUpdateFairShareContext* context) = 0;
    void PrepareMaxFitFactorBySuggestion(TUpdateFairShareContext* context);

    bool IsResourceBlocked(EJobResourceType resource) const;

    bool AreAllResourcesBlocked() const;

    bool IsStrictlyDominatesNonBlocked(const TResourceVector& lhs, const TResourceVector& rhs) const;

    virtual TJobResources GetSpecifiedResourceLimits() const = 0;

    TJobResources ComputeTotalResourcesOnSuitableNodes() const;

    TJobResources ComputeResourceLimits() const;
    virtual TResourceVector ComputeLimitsShare() const;

    TJobResources GetTotalResourceLimits() const;

    virtual std::optional<TMeteringKey> GetMeteringKey() const;
    virtual void BuildResourceMetering(const std::optional<TMeteringKey>& parentKey, TMeteringMap* statistics) const;

    void Profile(NProfiling::ISensorWriter* writer, bool profilingCompatibilityEnabled) const;

    virtual bool AreDetailedLogsEnabled() const;

private:
    TResourceTreeElementPtr ResourceTreeElement_;

protected:
    TSchedulerElement(
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
        TFairShareStrategyTreeConfigPtr treeConfig,
        TString treeId,
        TString id,
        const NLogging::TLogger& logger);
    TSchedulerElement(
        const TSchedulerElement& other,
        TCompositeSchedulerElement* clonedParent);

    ISchedulerStrategyHost* GetHost() const;

    IFairShareTreeHost* GetTreeHost() const;

    ESchedulableStatus GetStatusImpl(double defaultTolerance, bool atUpdate) const;

    void CheckForStarvationImpl(TDuration fairSharePreemptionTimeout, TInstant now);

    void SetOperationAlert(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout);

    TString GetLoggingAttributesString() const;

    TJobResources GetLocalAvailableResourceDemand(const TFairShareContext& context) const;
    TJobResources GetLocalAvailableResourceLimits(const TFairShareContext& context) const;

    bool CheckDemand(const TJobResources& delta, const TFairShareContext& context);

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
    DEFINE_BYREF_RW_PROPERTY(int, RunningOperationCount);
    DEFINE_BYREF_RW_PROPERTY(int, OperationCount);
    DEFINE_BYREF_RW_PROPERTY(std::list<TOperationId>, PendingOperationIds);

    DEFINE_BYREF_RO_PROPERTY(double, AdjustedFairShareStarvationToleranceLimit);
    DEFINE_BYREF_RO_PROPERTY(TDuration, AdjustedFairSharePreemptionTimeoutLimit);

protected:
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
        const NLogging::TLogger& logger);
    TCompositeSchedulerElement(
        const TCompositeSchedulerElement& other,
        TCompositeSchedulerElement* clonedParent);

    virtual int EnumerateElements(int startIndex, TUpdateFairShareContext* context) override;

    virtual void DisableNonAliveElements() override;

    virtual void MarkImmutable() override;

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;

    virtual void PreUpdateBottomUp(TUpdateFairShareContext* context) override;
    virtual void UpdateCumulativeAttributes(TUpdateFairShareContext* context) override;
    virtual void PublishFairShareAndUpdatePreemption() override;
    virtual void UpdatePreemptionAttributes() override;
    virtual void UpdateSchedulableAttributesFromDynamicAttributes(TDynamicAttributesList* dynamicAttributesList) override;

    virtual double GetFairShareStarvationToleranceLimit() const;
    virtual TDuration GetFairSharePreemptionTimeoutLimit() const;

    virtual void UpdateDynamicAttributes(TDynamicAttributesList* dynamicAttributesList) override;

    virtual void PrescheduleJob(TFairShareContext* context, EPrescheduleJobOperationCriterion operationCriterion, bool aggressiveStarvationEnabled) override;
    virtual TFairShareScheduleJobResult ScheduleJob(TFairShareContext* context, bool ignorePacking) override;

    virtual bool IsSchedulable() const override;

    virtual bool HasAggressivelyStarvingElements(TFairShareContext* context, bool aggressiveStarvationEnabled) const override;

    virtual bool IsExplicit() const;
    virtual bool IsAggressiveStarvationEnabled() const;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;

    void AddChild(TSchedulerElement* child, bool enabled = true);
    void EnableChild(const TSchedulerElementPtr& child);
    void DisableChild(const TSchedulerElementPtr& child);
    void RemoveChild(TSchedulerElement* child);
    bool IsEnabledChild(TSchedulerElement* child);

    // For testing only.
    std::vector<TSchedulerElementPtr> GetEnabledChildren();

    bool IsEmpty() const;

    ESchedulingMode GetMode() const;
    void SetMode(ESchedulingMode);

    void RegisterProfiler(const NProfiling::TRegistry& profiler);
    void ProfileFull(bool profilingCompatibilityEnabled);

    virtual int GetMaxOperationCount() const = 0;
    virtual int GetMaxRunningOperationCount() const = 0;
    int GetAvailableRunningOperationCount() const;

    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const = 0;
    virtual bool AreImmediateOperationsForbidden() const = 0;

    virtual void BuildElementMapping(TRawOperationElementMap* enabledOperationMap, TRawOperationElementMap* disabledOperationMap, TRawPoolMap* poolMap) override;

    void IncreaseOperationCount(int delta);
    void IncreaseRunningOperationCount(int delta);

    virtual THashSet<TString> GetAllowedProfilingTags() const = 0;

    virtual void PrepareFairShareFunctions(TUpdateFairShareContext* context) override;

    virtual void PrepareFairShareByFitFactor(TUpdateFairShareContext* context) override;

    virtual TResourceVector DoUpdateFairShare(double suggestion, TUpdateFairShareContext* context) override;

    virtual bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const = 0;
    virtual THistoricUsageAggregationParameters GetHistoricUsageAggregationParameters() const = 0;

    virtual bool IsDefaultConfigured() const = 0;

    virtual void BuildResourceMetering(const std::optional<TMeteringKey>& parentKey, TMeteringMap* statistics) const override;

    virtual double GetSpecifiedBurstRatio() const = 0;
    virtual double GetSpecifiedResourceFlowRatio() const = 0;

    TResourceVector GetHierarchicalAvailableLimitsShare() const;

    TJobResources GetIntegralPoolCapacity() const;

    void InitializeChildHeap(TFairShareContext* context);
    void UpdateChild(TFairShareContext* context, TSchedulerElement* child);

protected:
    NProfiling::TBufferedProducerPtr ProducerBuffer_;

    using TChildMap = THashMap<TSchedulerElementPtr, int>;
    using TChildList = std::vector<TSchedulerElementPtr>;

    TChildMap EnabledChildToIndex_;
    TChildList EnabledChildren_;
    TChildList SortedEnabledChildren_;

    TChildMap DisabledChildToIndex_;
    TChildList DisabledChildren_;

    TChildList SchedulableChildren_;

    /// strict_mode = true means that a caller guarantees that the sum predicate is true at least for fit factor = 0.0.
    /// strict_mode = false means that if the sum predicate is false for any fit factor, we fit children to the least possible sum
    /// (i. e. use fit factor = 0.0)
    template <class TValue, class TGetter, class TSetter>
    TValue ComputeByFitting(
        const TGetter& getter,
        const TSetter& setter,
        TValue maxSum,
        bool strictMode = true);

    void InitIntegralPoolLists(TUpdateFairShareContext* context);
    void AdjustStrongGuarantees();

    void PrepareFairShareByFitFactorFifo(TUpdateFairShareContext* context);
    void PrepareFairShareByFitFactorNormal(TUpdateFairShareContext* context);

    void PrepareFifoPool();
    void UpdateStrongGuaranteeNormal(TUpdateFairShareContext* context);

    using TChildSuggestions = std::vector<double>;
    TChildSuggestions GetEnabledChildSuggestionsFifo(double fitFactor);
    TChildSuggestions GetEnabledChildSuggestionsNormal(double fitFactor);

    TSchedulerElement* GetBestActiveChild(const TDynamicAttributesList& dynamicAttributesList) const;
    TSchedulerElement* GetBestActiveChildFifo(const TDynamicAttributesList& dynamicAttributesList) const;
    TSchedulerElement* GetBestActiveChildFairShare(const TDynamicAttributesList& dynamicAttributesList) const;

    static void AddChild(TChildMap* map, TChildList* list, const TSchedulerElementPtr& child);
    static void RemoveChild(TChildMap* map, TChildList* list, const TSchedulerElementPtr& child);
    static bool ContainsChild(const TChildMap& map, const TSchedulerElementPtr& child);

private:
    bool HasHigherPriorityInFifoMode(const TSchedulerElement* lhs, const TSchedulerElement* rhs) const;

    static double GetMinChildWeight(const TChildList& children);
};

DEFINE_REFCOUNTED_TYPE(TCompositeSchedulerElement)

////////////////////////////////////////////////////////////////////////////////

class TPoolFixedState
{
protected:
    explicit TPoolFixedState(TString id);

    const TString Id_;
    bool DefaultConfigured_ = true;
    bool EphemeralInDefaultParentPool_ = false;
    std::optional<TString> UserName_;
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

    void SetUserName(const std::optional<TString>& userName);
    const std::optional<TString>& GetUserName() const;

    TPoolConfigPtr GetConfig() const;
    void SetConfig(TPoolConfigPtr config);
    void SetDefaultConfig();
    void SetEphemeralInDefaultParentPool();

    bool IsEphemeralInDefaultParentPool() const;

    virtual bool IsDefaultConfigured() const override;
    virtual bool IsExplicit() const override;
    virtual bool IsAggressiveStarvationEnabled() const override;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;

    virtual TPool* AsPool() override;

    virtual TString GetId() const override;

    virtual std::optional<double> GetSpecifiedWeight() const override;
    virtual TJobResources GetStrongGuaranteeResources() const override;
    virtual TResourceVector GetMaxShare() const override;
    virtual EIntegralGuaranteeType GetIntegralGuaranteeType() const override;

    virtual ESchedulableStatus GetStatus(bool atUpdate = false) const override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual double GetFairShareStarvationToleranceLimit() const override;
    virtual TDuration GetFairSharePreemptionTimeoutLimit() const override;

    virtual TJobResources GetSpecifiedResourceLimits() const override;

    virtual void SetStarving(bool starving) override;
    virtual void CheckForStarvation(TInstant now) override;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    virtual int GetMaxRunningOperationCount() const override;
    virtual int GetMaxOperationCount() const override;

    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const override;
    virtual bool AreImmediateOperationsForbidden() const override;

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) override;

    void AttachParent(TCompositeSchedulerElement* newParent);
    void ChangeParent(TCompositeSchedulerElement* newParent);
    void DetachParent();

    virtual THashSet<TString> GetAllowedProfilingTags() const override;

    virtual bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const override;
    virtual THistoricUsageAggregationParameters GetHistoricUsageAggregationParameters() const override;

    virtual void BuildElementMapping(TRawOperationElementMap* enabledOperationMap, TRawOperationElementMap* disabledOperationMap, TRawPoolMap* poolMap) override;

    virtual std::optional<TMeteringKey> GetMeteringKey() const override;

    virtual double GetSpecifiedBurstRatio() const override;
    virtual double GetSpecifiedResourceFlowRatio() const override;

    void UpdateAccumulatedResourceVolume(TDuration periodSinceLastUpdate);

    void ApplyLimitsForRelaxedPool();
    TResourceVector GetIntegralShareLimitForRelaxedPool() const;

    virtual bool AreDetailedLogsEnabled() const override;

private:
    TPoolConfigPtr Config_;
    TSchedulingTagFilter SchedulingTagFilter_;

    void DoSetConfig(TPoolConfigPtr newConfig);
};

DEFINE_REFCOUNTED_TYPE(TPool)

////////////////////////////////////////////////////////////////////////////////

class TOperationElementFixedState
{
protected:
    TOperationElementFixedState(
        IOperationStrategyHost* operation,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig);

    const TOperationId OperationId_;
    std::optional<EUnschedulableReason> UnschedulableReason_;
    std::optional<int> SlotIndex_;
    TString UserName_;
    IOperationStrategyHost* const Operation_;
    TFairShareStrategyOperationControllerConfigPtr ControllerConfig_;
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
        const TFairShareContext& context,
        const TJobResources& availableResources,
        const TJobResources& minNeededResources);
    TEnumIndexedVector<EJobResourceType, int> GetMinNeededResourcesUnsatisfiedCount() const;

    void OnOperationDeactivated(const TFairShareContext& context, EDeactivationReason reason);
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

    virtual void DisableNonAliveElements() override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual void PreUpdateBottomUp(TUpdateFairShareContext* context) override;
    virtual void UpdateCumulativeAttributes(TUpdateFairShareContext* context) override;
    virtual void PublishFairShareAndUpdatePreemption() override;
    virtual void UpdatePreemptionAttributes() override;

    virtual void PrepareFairShareByFitFactor(TUpdateFairShareContext* context) override;

    virtual TResourceVector DoUpdateFairShare(double suggestion, TUpdateFairShareContext* context) override;

    virtual bool IsOperation() const override;

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;
    void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config);

    virtual void UpdateDynamicAttributes(TDynamicAttributesList* dynamicAttributesList) override;

    virtual void PrescheduleJob(TFairShareContext* context, EPrescheduleJobOperationCriterion operationCriterion, bool aggressiveStarvationEnabled) override;
    virtual TFairShareScheduleJobResult ScheduleJob(TFairShareContext* context, bool ignorePacking) override;

    virtual bool HasAggressivelyStarvingElements(TFairShareContext* context, bool aggressiveStarvationEnabled) const override;

    virtual TString GetLoggingString() const override;

    virtual TString GetId() const override;

    virtual bool IsSchedulable() const override;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;

    virtual std::optional<double> GetSpecifiedWeight() const override;
    virtual TJobResources GetStrongGuaranteeResources() const override;
    virtual TResourceVector GetMaxShare() const override;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    virtual ESchedulableStatus GetStatus(bool atUpdate = false) const override;

    virtual void SetStarving(bool starving) override;
    virtual void CheckForStarvation(TInstant now) override;
    bool IsPreemptionAllowed(bool isAggressivePreemption, const TFairShareStrategyTreeConfigPtr& config) const;

    void ApplyJobMetricsDelta(const TJobMetrics& delta);

    void SetJobResourceUsage(TJobId jobId, const TJobResources& resources);

    bool IsJobKnown(TJobId jobId) const;

    bool IsJobPreemptable(TJobId jobId, bool aggressivePreemptionEnabled) const;

    int GetRunningJobCount() const;
    int GetPreemptableJobCount() const;
    int GetAggressivelyPreemptableJobCount() const;

    TPreemptionStatusStatisticsVector GetPreemptionStatusStatistics() const;

    TInstant GetLastNonStarvingTime() const;
    TInstant GetLastScheduleJobSuccessTime() const;

    std::optional<int> GetMaybeSlotIndex() const;

    void RegisterProfiler(std::optional<int> slotIndex, const NProfiling::TRegistry& profiler);
    void ProfileFull(bool profilingCompatibilityEnabled);

    TString GetUserName() const;

    virtual TResourceVector ComputeLimitsShare() const override;

    bool OnJobStarted(
        TJobId jobId,
        const TJobResources& resourceUsage,
        const TJobResources& precommittedResources,
        bool force = false);
    void OnJobFinished(TJobId jobId);

    virtual void BuildElementMapping(TRawOperationElementMap* enabledOperationMap, TRawOperationElementMap* disabledOperationMap, TRawPoolMap* poolMap) override;

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) override;

    void OnMinNeededResourcesUnsatisfied(
        const TFairShareContext& context,
        const TJobResources& availableResources,
        const TJobResources& minNeededResources);
    TEnumIndexedVector<EJobResourceType, int> GetMinNeededResourcesUnsatisfiedCount() const;

    void ActivateOperation(TFairShareContext* context);
    void DeactivateOperation(TFairShareContext* context, EDeactivationReason reason);

    void OnOperationDeactivated(TFairShareContext* context, EDeactivationReason reason);
    TEnumIndexedVector<EDeactivationReason, int> GetDeactivationReasons() const;
    TEnumIndexedVector<EDeactivationReason, int> GetDeactivationReasonsFromLastNonStarvingTime() const;

    std::optional<TString> GetCustomProfilingTag() const;

    void Disable(bool markAsNonAlive);
    void Enable();

    EResourceTreeIncreaseResult TryIncreaseHierarchicalResourceUsagePrecommit(
        const TJobResources& delta,
        TJobResources* availableResourceLimitsOutput = nullptr);

    void AttachParent(TCompositeSchedulerElement* newParent, bool enabled);
    void ChangeParent(TCompositeSchedulerElement* newParent);
    void DetachParent();

    void MarkOperationRunningInPool();
    bool IsOperationRunningInPool();

    void UpdateAncestorsDynamicAttributes(TFairShareContext* context, bool checkAncestorsActiveness = true);

    void MarkPendingBy(TCompositeSchedulerElement* violatedPool);

    void InitOrUpdateSchedulingSegment(ESegmentedSchedulingMode mode);

    bool IsLimitingAncestorCheckEnabled() const;

    virtual bool AreDetailedLogsEnabled() const override;

    DEFINE_BYVAL_RW_PROPERTY(TOperationFairShareTreeRuntimeParametersPtr, RuntimeParameters);

    DEFINE_BYVAL_RO_PROPERTY(TStrategyOperationSpecPtr, Spec);

    DEFINE_BYREF_RW_PROPERTY(std::optional<TString>, PendingByPool);

    DEFINE_BYREF_RW_PROPERTY(std::optional<ESchedulingSegment>, SchedulingSegment);

private:
    const TOperationElementSharedStatePtr OperationElementSharedState_;
    const TFairShareStrategyOperationControllerPtr Controller_;

    bool RunningInThisPoolTree_ = false;
    TSchedulingTagFilter SchedulingTagFilter_;

    NProfiling::TBufferedProducerPtr ProducerBuffer_;

    bool HasJobsSatisfyingResourceLimits(const TFairShareContext& context) const;

    std::optional<EUnschedulableReason> ComputeUnschedulableReason() const;

    bool IsMaxScheduleJobCallsViolated() const;
    bool IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(const ISchedulingContextPtr& schedulingContext) const;
    bool HasRecentScheduleJobFailure(NProfiling::TCpuInstant now) const;
    std::optional<EDeactivationReason> CheckBlocked(const ISchedulingContextPtr& schedulingContext) const;

    void RecordHeartbeat(const TPackingHeartbeatSnapshot& heartbeatSnapshot);
    bool CheckPacking(const TPackingHeartbeatSnapshot& heartbeatSnapshot) const;

    TJobResources GetHierarchicalAvailableResources(const TFairShareContext& context) const;

    std::optional<EDeactivationReason> TryStartScheduleJob(
        const TFairShareContext& context,
        TJobResources* precommittedResourcesOutput,
        TJobResources* availableResourcesOutput);

    void FinishScheduleJob(const ISchedulingContextPtr& schedulingContext);

    TControllerScheduleJobResultPtr DoScheduleJob(
        TFairShareContext* context,
        const TJobResources& availableResources,
        TJobResources* precommittedResources);

    virtual TJobResources GetSpecifiedResourceLimits() const override;
    TJobResources ComputeResourceDemand() const;
    int ComputePendingJobCount() const;

    void UpdatePreemptableJobsList();

    TFairShareStrategyPackingConfigPtr GetPackingConfig() const;
};

DEFINE_REFCOUNTED_TYPE(TOperationElement)

////////////////////////////////////////////////////////////////////////////////

class TRootElementFixedState
{
public:
    DEFINE_BYVAL_RO_PROPERTY(int, TreeSize);
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

    //! Computes various lightweight attributes in the tree. Thread-unsafe.
    void PreUpdate(TUpdateFairShareContext* context);
    //! Computes fair shares in the tree. Thread-safe.
    void Update(TUpdateFairShareContext* context);

    void UpdateFairShare(TUpdateFairShareContext* context);
    void UpdateRootFairShare();

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;

    virtual bool IsRoot() const override;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    virtual TString GetId() const override;

    virtual std::optional<double> GetSpecifiedWeight() const override;
    virtual TJobResources GetStrongGuaranteeResources() const override;
    virtual TResourceVector GetMaxShare() const override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual TJobResources GetSpecifiedResourceLimits() const override;

    virtual bool IsAggressiveStarvationEnabled() const override;

    virtual void CheckForStarvation(TInstant now) override;

    virtual int GetMaxRunningOperationCount() const override;
    virtual int GetMaxOperationCount() const override;

    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const override;
    virtual bool AreImmediateOperationsForbidden() const override;

    virtual THashSet<TString> GetAllowedProfilingTags() const override;

    virtual bool IsInferringChildrenWeightsFromHistoricUsageEnabled() const override;
    virtual THistoricUsageAggregationParameters GetHistoricUsageAggregationParameters() const override;

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) override;
    virtual bool IsDefaultConfigured() const override;

    TRootElementPtr Clone();

    virtual std::optional<TMeteringKey> GetMeteringKey() const override;

    void BuildResourceDistributionInfo(NYTree::TFluentMap fluent) const;

private:
    virtual void UpdateCumulativeAttributes(TUpdateFairShareContext* context) override;
    void ValidateAndAdjustSpecifiedGuarantees(TUpdateFairShareContext* context);
    void UpdateBurstPoolIntegralShares(TUpdateFairShareContext* context);
    void UpdateRelaxedPoolIntegralShares(TUpdateFairShareContext* context, const TResourceVector& availableShare);
    TResourceVector EstimateAvailableShare();
    void ConsumeAndRefillIntegralPools(TUpdateFairShareContext* context);

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
    TSchedulerElement* GetTop();
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
