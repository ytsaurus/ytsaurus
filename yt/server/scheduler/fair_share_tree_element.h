#pragma once

#include "private.h"
#include "job.h"
#include "scheduler_strategy.h"
#include "scheduling_context.h"
#include "fair_share_strategy_operation_controller.h"

#include <yt/server/lib/scheduler/config.h>
#include <yt/server/lib/scheduler/scheduling_tag.h>
#include <yt/server/lib/scheduler/job_metrics.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TSchedulableAttributes
{
    NNodeTrackerClient::EResourceType DominantResource = NNodeTrackerClient::EResourceType::Cpu;
    double DemandRatio = 0.0;
    double FairShareRatio = 0.0;
    double AdjustedMinShareRatio = 0.0;
    double RecursiveMinShareRatio = 0.0;
    double MaxPossibleUsageRatio = 1.0;
    double BestAllocationRatio = 1.0;
    double GuaranteedResourcesRatio = 0.0;
    double DominantLimit = 0;
    int FifoIndex = -1;

    double AdjustedFairShareStarvationTolerance = 1.0;
    TDuration AdjustedMinSharePreemptionTimeout;
    TDuration AdjustedFairSharePreemptionTimeout;
};

struct TDynamicAttributes
{
    double SatisfactionRatio = 0.0;
    bool Active = false;
    TSchedulerElement* BestLeafDescendant = nullptr;
    TJobResources ResourceUsageDiscount = ZeroJobResources();
};

typedef std::vector<TDynamicAttributes> TDynamicAttributesList;


////////////////////////////////////////////////////////////////////////////////

//! This interface must be thread-safe.
struct IFairShareTreeHost
{
    virtual NProfiling::TAggregateGauge& GetProfilingCounter(const TString& name) = 0;

    // NB(renadeen): see TSchedulerElementSharedState for explanation
    virtual NConcurrency::TReaderWriterSpinLock* GetSharedStateTreeLock() = 0;
};
////////////////////////////////////////////////////////////////////////////////

struct TUpdateFairShareContext
{
    std::vector<TError> Errors;
};

////////////////////////////////////////////////////////////////////////////////

struct TScheduleJobsProfilingCounters
{
    TScheduleJobsProfilingCounters(const TString& prefix, const NProfiling::TTagIdList& treeIdProfilingTags);

    NProfiling::TAggregateGauge PrescheduleJobTime;
    NProfiling::TAggregateGauge TotalControllerScheduleJobTime;
    NProfiling::TAggregateGauge ExecControllerScheduleJobTime;
    NProfiling::TAggregateGauge StrategyScheduleJobTime;
    NProfiling::TMonotonicCounter ScheduleJobCount;
    NProfiling::TMonotonicCounter ScheduleJobFailureCount;
    TEnumIndexedVector<NProfiling::TMonotonicCounter, NControllerAgent::EScheduleJobFailReason> ControllerScheduleJobFail;
};

////////////////////////////////////////////////////////////////////////////////

struct TFairShareSchedulingStage
{
    TFairShareSchedulingStage(const TString& loggingName, TScheduleJobsProfilingCounters profilingCounters);

    const TString LoggingName;
    TScheduleJobsProfilingCounters ProfilingCounters;
};

////////////////////////////////////////////////////////////////////////////////

class TFairShareContext
{
public:
    TFairShareContext(const ISchedulingContextPtr& schedulingContext, bool enableSchedulingInfoLogging);

    void Initialize(int treeSize, const std::vector<TSchedulingTagFilter>& registeredSchedulingTagFilters);

    TDynamicAttributes& DynamicAttributesFor(const TSchedulerElement* element);
    const TDynamicAttributes& DynamicAttributesFor(const TSchedulerElement* element) const;

    void StartStage(TFairShareSchedulingStage* schedulingStage);

    void ProfileStageTimingsAndLogStatistics();

    void FinishStage();

    bool Initialized = false;

    std::vector<bool> CanSchedule;
    TDynamicAttributesList DynamicAttributesList;

    const ISchedulingContextPtr SchedulingContext;
    const bool EnableSchedulingInfoLogging;

    // Used to avoid unnecessary calculation of HasAggressivelyStarvingElements.
    bool PrescheduleCalled = false;

    TFairShareSchedulingStatistics SchedulingStatistics;

    struct TStageState
    {
        explicit TStageState(TFairShareSchedulingStage* schedulingStage);

        TFairShareSchedulingStage* const SchedulingStage;

        TDuration TotalDuration;
        TDuration PrescheduleDuration;
        TDuration TotalScheduleJobDuration;
        TDuration ExecScheduleJobDuration;
        TEnumIndexedVector<int, NControllerAgent::EScheduleJobFailReason> FailedScheduleJob;

        int ActiveOperationCount = 0;
        int ActiveTreeSize = 0;
        int ScheduleJobAttempts = 0;
        int ScheduleJobFailureCount = 0;
        TEnumIndexedVector<int, EDeactivationReason> DeactivationReasons;
    };

    std::optional<TStageState> StageState;

private:
    void ProfileStageTimings();

    void LogStageStatistics();
};

////////////////////////////////////////////////////////////////////////////////

const int UnassignedTreeIndex = -1;
const int EmptySchedulingTagFilterIndex = -1;

class TSchedulerElementFixedState
{
public:
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceDemand);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, ResourceLimits);
    DEFINE_BYREF_RO_PROPERTY(TJobResources, MaxPossibleResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(TSchedulableAttributes, Attributes);
    DEFINE_BYVAL_RW_PROPERTY(int, SchedulingTagFilterIndex, EmptySchedulingTagFilterIndex);

protected:
    TSchedulerElementFixedState(
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
        const TFairShareStrategyTreeConfigPtr& treeConfig,
        const TString& treeId);

    ISchedulerStrategyHost* const Host_;
    IFairShareTreeHost* const TreeHost_;

    TFairShareStrategyTreeConfigPtr TreeConfig_;

    TCompositeSchedulerElement* Parent_ = nullptr;

    std::optional<TInstant> BelowFairShareSince_;
    bool Starving_ = false;

    TJobResources TotalResourceLimits_;

    int PendingJobCount_ = 0;
    TInstant StartTime_;

    int TreeIndex_ = UnassignedTreeIndex;

    bool Cloned_ = false;

    const TString TreeId_;
};

class TSchedulerElementSharedState
    : public TIntrinsicRefCounted
{
    // This is node of shared state tree.
    // Each state corresponds to some scheduler element and all its snapshots.
    // There are three general scenarios:

    // 1. Get local resource usage of particular element - we take read lock on ResourceUsageLock_ of the state.

    // 2. Apply update of some property from leaf to root (increase or decrease of resource usage for example)
    // - we take read lock on SharedStateTreeLock provided by IFairShareTreeHost for whole operation
    // and make local updates of particular states under write lock on corresponding ResourceUsageLock_.

    // 3. Modify tree structure (attach, change or detach parent of element)
    // - we take write lock on SharedStateTreeLock for whole operation.
    // If we need to update properties of the particular state we also take the write lock on ResourceUsageLock_ for this update

    // Essentially SharedStateTreeLock protects tree structure.
    // We take this lock if we need to access the parent link of some element

public:
    explicit TSchedulerElementSharedState(IFairShareTreeHost* host);
    TJobResources GetResourceUsage();
    TJobResources GetTotalResourceUsageWithPrecommit();
    TJobMetrics GetJobMetrics();

    void AttachParent(TSchedulerElementSharedState* parent);
    void ChangeParent(TSchedulerElementSharedState* newParent);
    void DetachParent();
    void ReleaseResources();

    bool TryIncreaseHierarchicalResourceUsagePrecommit(
        const TJobResources &delta,
        TJobResources *availableResourceLimitsOutput);
    void IncreaseHierarchicalResourceUsagePrecommit(const TJobResources& delta);
    void CommitHierarchicalResourceUsage(
        const TJobResources& resourceUsageDelta,
        const TJobResources& precommittedResources);
    void IncreaseHierarchicalResourceUsage(const TJobResources& delta);
    void ApplyHierarchicalJobMetricsDelta(const TJobMetrics& delta);

    bool CheckDemand(
        const TJobResources& delta,
        const TJobResources& resourceDemand,
        const TJobResources& resourceDiscount);
    double GetResourceUsageRatio(NNodeTrackerClient::EResourceType dominantResource, double dominantResourceLimit);

    bool GetAlive() const;
    void SetAlive(bool alive);

    double GetFairShareRatio() const;
    void SetFairShareRatio(double fairShareRatio);

    void SetResourceLimits(TJobResources resourceLimits);

private:
    IFairShareTreeHost* FairShareTreeHost_;
    TJobResources ResourceUsage_;
    TJobResources ResourceLimits_ = InfiniteJobResources();
    TJobResources ResourceUsagePrecommit_;
    TJobMetrics JobMetrics_;

    NConcurrency::TReaderWriterSpinLock ResourceUsageLock_;

    NConcurrency::TReaderWriterSpinLock JobMetricsLock_;

    TSchedulerElementSharedStatePtr Parent_ = nullptr;

    // NB: Avoid false sharing between ResourceUsageLock_ and others.
    [[maybe_unused]] char Padding[64];

    std::atomic<bool> Alive_ = {true};

    std::atomic<double> FairShareRatio_ = {0.0};

    bool IncreaseLocalResourceUsagePrecommitWithCheck(
        const TJobResources& delta,
        TJobResources* availableResourceLimitsOutput);
    void IncreaseLocalResourceUsagePrecommit(const TJobResources& delta);
    void CommitLocalResourceUsage(
        const TJobResources& resourceUsageDelta,
        const TJobResources& precommittedResources);
    void IncreaseLocalResourceUsage(const TJobResources& delta);
    void ApplyLocalJobMetricsDelta(const TJobMetrics& delta);

    void DoIncreaseHierarchicalResourceUsagePrecommit(const TJobResources& delta);
    void DoIncreaseHierarchicalResourceUsage(const TJobResources& delta);

    void CheckCycleAbsence(TSchedulerElementSharedState* newParent);
    TJobResources GetResourceUsagePrecommit();
};

DEFINE_REFCOUNTED_TYPE(TSchedulerElementSharedState)

class TSchedulerElement
    : public TSchedulerElementFixedState
    , public TIntrinsicRefCounted
{
public:
    //! Enumerates elements of the tree using inorder traversal. Returns first unused index.
    virtual int EnumerateElements(int startIndex);

    int GetTreeIndex() const;
    void SetTreeIndex(int treeIndex);

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config);

    virtual void Update(TDynamicAttributesList& dynamicAttributesList, TUpdateFairShareContext* context);

    //! Updates attributes that need to be computed from leafs up to root.
    //! For example: |parent->ResourceDemand = Sum(child->ResourceDemand)|.
    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList);

    //! Updates attributes that are propagated from root down to leafs.
    //! For example: |child->FairShareRatio = fraction(parent->FairShareRatio)|.
    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList, TUpdateFairShareContext* context);

    virtual TJobResources ComputePossibleResourceUsage(TJobResources limit) const = 0;

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList);

    virtual void PrescheduleJob(TFairShareContext* context, bool starvingOnly, bool aggressiveStarvationEnabled);
    virtual bool ScheduleJob(TFairShareContext* context) = 0;

    virtual bool HasAggressivelyStarvingElements(TFairShareContext* context, bool aggressiveStarvationEnabled) const = 0;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const;

    virtual bool IsRoot() const;
    virtual bool IsOperation() const;

    virtual TString GetLoggingString(const TDynamicAttributesList& dynamicAttributesList) const;

    bool IsActive(const TDynamicAttributesList& dynamicAttributesList) const;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const = 0;

    bool IsAlive() const;
    void SetAlive(bool alive);

    double GetFairShareRatio() const;
    void SetFairShareRatio(double fairShareRatio);

    virtual TString GetId() const = 0;

    virtual std::optional<double> GetSpecifiedWeight() const = 0;
    virtual double GetWeight() const;

    virtual double GetMinShareRatio() const = 0;
    virtual TJobResources GetMinShareResources() const = 0;

    virtual double GetMaxShareRatio() const = 0;

    virtual double GetFairShareStarvationTolerance() const = 0;
    virtual TDuration GetMinSharePreemptionTimeout() const = 0;
    virtual TDuration GetFairSharePreemptionTimeout() const = 0;

    TCompositeSchedulerElement* GetMutableParent();
    const TCompositeSchedulerElement* GetParent() const;

    TInstant GetStartTime() const;
    int GetPendingJobCount() const;

    virtual ESchedulableStatus GetStatus() const;

    bool GetStarving() const;
    virtual void SetStarving(bool starving);
    virtual void CheckForStarvation(TInstant now) = 0;

    TJobResources GetLocalResourceUsage() const;
    TJobResources GetTotalLocalResourceUsageWithPrecommit() const;
    TJobMetrics GetJobMetrics() const;
    double GetLocalResourceUsageRatio() const;

    virtual TString GetTreeId() const;

    void IncreaseHierarchicalResourceUsage(const TJobResources& delta);

    virtual void BuildOperationToElementMapping(TOperationElementByIdMap* operationElementByIdMap) = 0;

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) = 0;

    double ComputeLocalSatisfactionRatio() const;

private:
    TSchedulerElementSharedStatePtr SharedState_;

protected:
    TSchedulerElement(
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
        const TFairShareStrategyTreeConfigPtr& treeConfig,
        const TString& treeId);
    TSchedulerElement(
        const TSchedulerElement& other,
        TCompositeSchedulerElement* clonedParent);

    ISchedulerStrategyHost* GetHost() const;

    IFairShareTreeHost* GetTreeHost() const;

    ESchedulableStatus GetStatus(double defaultTolerance) const;

    void CheckForStarvationImpl(
        TDuration minSharePreemptionTimeout,
        TDuration fairSharePreemptionTimeout,
        TInstant now);

    void SetOperationAlert(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert,
        std::optional<TDuration> timeout);

    TString GetLoggingAttributesString(const TDynamicAttributesList& dynamicAttributesList) const;

    TJobResources GetLocalAvailableResourceDemand(const TFairShareContext& context) const;
    TJobResources GetLocalAvailableResourceLimits(const TFairShareContext& context) const;

    bool CheckDemand(const TJobResources& delta, const TFairShareContext& context);

    TJobResources ComputeResourceLimitsBase(const TResourceLimitsConfigPtr& resourceLimitsConfig) const;

private:
    void UpdateAttributes();

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

    DEFINE_BYREF_RO_PROPERTY(double, AdjustedFairShareStarvationToleranceLimit);
    DEFINE_BYREF_RO_PROPERTY(TDuration, AdjustedMinSharePreemptionTimeoutLimit);
    DEFINE_BYREF_RO_PROPERTY(TDuration, AdjustedFairSharePreemptionTimeoutLimit);

protected:
    ESchedulingMode Mode_ = ESchedulingMode::Fifo;
    std::vector<EFifoSortParameter> FifoSortParameters_;
};

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
        const TString& treeId);
    TCompositeSchedulerElement(
        const TCompositeSchedulerElement& other,
        TCompositeSchedulerElement* clonedParent);

    virtual int EnumerateElements(int startIndex) override;

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;

    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override;
    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList, TUpdateFairShareContext* context) override;

    virtual TJobResources ComputePossibleResourceUsage(TJobResources limit) const override;

    virtual double GetFairShareStarvationToleranceLimit() const;
    virtual TDuration GetMinSharePreemptionTimeoutLimit() const;
    virtual TDuration GetFairSharePreemptionTimeoutLimit() const;

    void UpdatePreemptionSettingsLimits();
    void UpdateChildPreemptionSettings(const TSchedulerElementPtr& child);

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList) override;

    virtual void PrescheduleJob(TFairShareContext* context, bool starvingOnly, bool aggressiveStarvationEnabled) override;
    virtual bool ScheduleJob(TFairShareContext* context) override;

    virtual bool HasAggressivelyStarvingElements(TFairShareContext* context, bool aggressiveStarvationEnabled) const override;

    virtual bool IsExplicit() const;
    virtual bool IsAggressiveStarvationEnabled() const;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;

    void AddChild(const TSchedulerElementPtr& child, bool enabled = true);
    void EnableChild(const TSchedulerElementPtr& child);
    void DisableChild(const TSchedulerElementPtr& child);
    void RemoveChild(const TSchedulerElementPtr& child);

    bool IsEmpty() const;

    ESchedulingMode GetMode() const;
    void SetMode(ESchedulingMode);

    NProfiling::TTagId GetProfilingTag() const;

    virtual int GetMaxOperationCount() const = 0;
    virtual int GetMaxRunningOperationCount() const = 0;

    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const = 0;
    virtual bool AreImmediateOperationsForbidden() const = 0;

    virtual void BuildOperationToElementMapping(TOperationElementByIdMap* operationElementByIdMap) override;

    void IncreaseOperationCount(int delta);
    void IncreaseRunningOperationCount(int delta);

    virtual THashSet<TString> GetAllowedProfilingTags() const = 0;

protected:
    const NProfiling::TTagId ProfilingTag_;

    using TChildMap = THashMap<TSchedulerElementPtr, int>;
    using TChildList = std::vector<TSchedulerElementPtr>;

    TChildMap EnabledChildToIndex_;
    TChildList EnabledChildren_;

    TChildMap DisabledChildToIndex_;
    TChildList DisabledChildren_;

    template <class TGetter, class TSetter>
    void ComputeByFitting(const TGetter& getter, const TSetter& setter, double sum);

    void UpdateFifo(TDynamicAttributesList& dynamicAttributesList, TUpdateFairShareContext* context);
    void UpdateFairShare(TDynamicAttributesList& dynamicAttributesList, TUpdateFairShareContext* context);

    TSchedulerElementPtr GetBestActiveChild(const TDynamicAttributesList& dynamicAttributesList) const;
    TSchedulerElementPtr GetBestActiveChildFifo(const TDynamicAttributesList& dynamicAttributesList) const;
    TSchedulerElementPtr GetBestActiveChildFairShare(const TDynamicAttributesList& dynamicAttributesList) const;

    static void AddChild(TChildMap* map, TChildList* list, const TSchedulerElementPtr& child);
    static void RemoveChild(TChildMap* map, TChildList* list, const TSchedulerElementPtr& child);
    static bool ContainsChild(const TChildMap& map, const TSchedulerElementPtr& child);

private:
    bool HasHigherPriorityInFifoMode(const TSchedulerElementPtr& lhs, const TSchedulerElementPtr& rhs) const;
};

DEFINE_REFCOUNTED_TYPE(TCompositeSchedulerElement)

////////////////////////////////////////////////////////////////////////////////

class TPoolFixedState
{
protected:
    explicit TPoolFixedState(const TString& id);

    const TString Id_;
    bool DefaultConfigured_ = true;
    std::optional<TString> UserName_;
};

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
        const TString& treeId);
    TPool(
        const TPool& other,
        TCompositeSchedulerElement* clonedParent);

    bool IsDefaultConfigured() const;

    void SetUserName(const std::optional<TString>& userName);
    const std::optional<TString>& GetUserName() const;

    TPoolConfigPtr GetConfig();
    void SetConfig(TPoolConfigPtr config);
    void SetDefaultConfig();

    virtual bool IsExplicit() const override;
    virtual bool IsAggressiveStarvationEnabled() const override;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;

    virtual TString GetId() const override;

    virtual std::optional<double> GetSpecifiedWeight() const override;
    virtual double GetMinShareRatio() const override;
    virtual TJobResources GetMinShareResources() const override;
    virtual double GetMaxShareRatio() const override;

    virtual ESchedulableStatus GetStatus() const override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetMinSharePreemptionTimeout() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual double GetFairShareStarvationToleranceLimit() const override;
    virtual TDuration GetMinSharePreemptionTimeoutLimit() const override;
    virtual TDuration GetFairSharePreemptionTimeoutLimit() const override;

    virtual void SetStarving(bool starving) override;
    virtual void CheckForStarvation(TInstant now) override;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override;

    virtual int GetMaxRunningOperationCount() const override;
    virtual int GetMaxOperationCount() const override;

    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const override;
    virtual bool AreImmediateOperationsForbidden() const override;

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) override;

    void AttachParent(TCompositeSchedulerElement* newParent);
    void ChangeParent(TCompositeSchedulerElement* newParent);
    void DetachParent();

    virtual THashSet<TString> GetAllowedProfilingTags() const override;

private:
    TPoolConfigPtr Config_;
    TSchedulingTagFilter SchedulingTagFilter_;

    void DoSetConfig(TPoolConfigPtr newConfig);

    TJobResources ComputeResourceLimits() const;
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
    bool Schedulable_;
    IOperationStrategyHost* const Operation_;
    TFairShareStrategyOperationControllerConfigPtr ControllerConfig_;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOperationPreemptionStatus,
    (Allowed)
    (ForbiddenSinceStarvingParent)
    (ForbiddenSinceUnsatisfiedParentOrSelf)
    (ForbiddenSinceLowJobCount)
);

using TPreemptionStatusStatisticsVector = TEnumIndexedVector<int, EOperationPreemptionStatus>;

class TOperationElementSharedState
    : public TIntrinsicRefCounted
{
public:
    explicit TOperationElementSharedState(int updatePreemptableJobsListLoggingPeriod);

    TJobResources IncreaseJobResourceUsage(
        TJobId jobId,
        const TJobResources& resourcesDelta);

    void UpdatePreemptableJobsList(
        double fairShareRatio,
        const TJobResources& totalResourceLimits,
        double preemptionSatisfactionThreshold,
        double aggressivePreemptionSatisfactionThreshold,
        int* moveCount);

    bool IsJobKnown(TJobId jobId) const;

    bool IsJobPreemptable(TJobId jobId, bool aggressivePreemptionEnabled) const;

    int GetRunningJobCount() const;
    int GetPreemptableJobCount() const;
    int GetAggressivelyPreemptableJobCount() const;

    std::optional<TJobResources> AddJob(TJobId jobId, const TJobResources& resourceUsage, bool force);
    std::optional<TJobResources> RemoveJob(TJobId jobId);

    void UpdatePreemptionStatusStatistics(EOperationPreemptionStatus status);
    TPreemptionStatusStatisticsVector GetPreemptionStatusStatistics() const;

    void OnOperationDeactivated(EDeactivationReason reason);
    TEnumIndexedVector<int, EDeactivationReason> GetDeactivationReasons() const;
    void ResetDeactivationReasonsFromLastNonStarvingTime();
    TEnumIndexedVector<int, EDeactivationReason> GetDeactivationReasonsFromLastNonStarvingTime() const;

    TInstant GetLastScheduleJobSuccessTime() const;

    TJobResources Disable();
    void Enable();

private:
    using TJobIdList = std::list<TJobId>;

    TJobIdList NonpreemptableJobs_;
    TJobIdList AggressivelyPreemptableJobs_;
    TJobIdList PreemptableJobs_;

    std::atomic<int> RunningJobCount_ = {0};
    TJobResources NonpreemptableResourceUsage_;
    TJobResources AggressivelyPreemptableResourceUsage_;

    std::atomic<int> UpdatePreemptableJobsListCount_ = {0};
    int UpdatePreemptableJobsListLoggingPeriod_;

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

    NConcurrency::TReaderWriterSpinLock JobPropertiesMapLock_;
    THashMap<TJobId, TJobProperties> JobPropertiesMap_;
    TInstant LastScheduleJobSuccessTime_;

    TSpinLock PreemptionStatusStatisticsLock_;
    TPreemptionStatusStatisticsVector PreemptionStatusStatistics_;

    TEnumIndexedVector<std::atomic<int>, EDeactivationReason> DeactivationReasons_;
    TEnumIndexedVector<std::atomic<int>, EDeactivationReason> DeactivationReasonsFromLastNonStarvingTime_;

    bool Enabled_ = false;

    void IncreaseJobResourceUsage(TJobProperties* properties, const TJobResources& resourcesDelta);

    TJobProperties* GetJobProperties(TJobId jobId);
    const TJobProperties* GetJobProperties(TJobId jobId) const;
};

DEFINE_REFCOUNTED_TYPE(TOperationElementSharedState)

class TOperationElement
    : public TSchedulerElement
    , public TOperationElementFixedState
{
public:
    TOperationElement(
        TFairShareStrategyTreeConfigPtr treeConfig,
        TStrategyOperationSpecPtr spec,
        TOperationFairShareTreeRuntimeParametersPtr runtimeParams,
        TFairShareStrategyOperationControllerPtr controller,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        ISchedulerStrategyHost* host,
        IFairShareTreeHost* treeHost,
        IOperationStrategyHost* operation,
        const TString& treeId);
    TOperationElement(
        const TOperationElement& other,
        TCompositeSchedulerElement* clonedParent);

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetMinSharePreemptionTimeout() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override;
    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList, TUpdateFairShareContext* context) override;

    virtual bool IsOperation() const override;

    void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config);

    virtual TJobResources ComputePossibleResourceUsage(TJobResources limit) const override;

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList) override;

    virtual void PrescheduleJob(TFairShareContext* context, bool starvingOnly, bool aggressiveStarvationEnabled) override;
    virtual bool ScheduleJob(TFairShareContext* context) override;

    virtual bool HasAggressivelyStarvingElements(TFairShareContext* context, bool aggressiveStarvationEnabled) const override;

    virtual TString GetLoggingString(const TDynamicAttributesList& dynamicAttributesList) const override;

    virtual TString GetId() const override;

    bool IsSchedulable() const;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;

    virtual std::optional<double> GetSpecifiedWeight() const override;
    virtual double GetMinShareRatio() const override;
    virtual TJobResources GetMinShareResources() const override;
    virtual double GetMaxShareRatio() const override;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    virtual ESchedulableStatus GetStatus() const override;

    virtual void SetStarving(bool starving) override;
    virtual void CheckForStarvation(TInstant now) override;
    bool IsPreemptionAllowed(const TFairShareContext& context, const TFairShareStrategyTreeConfigPtr& config) const;

    void ApplyJobMetricsDelta(const TJobMetrics& delta);

    void IncreaseJobResourceUsage(TJobId jobId, const TJobResources& resourcesDelta);

    bool IsJobKnown(TJobId jobId) const;

    bool IsJobPreemptable(TJobId jobId, bool aggressivePreemptionEnabled) const;

    int GetRunningJobCount() const;
    int GetPreemptableJobCount() const;
    int GetAggressivelyPreemptableJobCount() const;

    TPreemptionStatusStatisticsVector GetPreemptionStatusStatistics() const;

    TInstant GetLastNonStarvingTime() const;
    TInstant GetLastScheduleJobSuccessTime() const;

    int GetSlotIndex() const;

    TString GetUserName() const;

    bool OnJobStarted(
        TJobId jobId,
        const TJobResources& resourceUsage,
        const TJobResources& precommittedResources,
        bool force = false);
    void OnJobFinished(TJobId jobId);

    virtual void BuildOperationToElementMapping(TOperationElementByIdMap* operationElementByIdMap) override;

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) override;

    void OnOperationDeactivated(EDeactivationReason reason);
    TEnumIndexedVector<int, EDeactivationReason> GetDeactivationReasons() const;
    TEnumIndexedVector<int, EDeactivationReason> GetDeactivationReasonsFromLastNonStarvingTime() const;

    std::optional<NProfiling::TTagId> GetCustomProfilingTag();

    void Disable();
    void Enable();

    bool TryIncreaseHierarchicalResourceUsagePrecommit(
        const TJobResources& delta,
        TJobResources* availableResourceLimitsOutput = nullptr);

    void AttachParent(TCompositeSchedulerElement* newParent, bool enabled);
    void ChangeParent(TCompositeSchedulerElement* newParent);
    void DetachParent();

    void MarkOperationRunningInPool();

    DEFINE_BYVAL_RW_PROPERTY(TOperationFairShareTreeRuntimeParametersPtr, RuntimeParams);

    DEFINE_BYVAL_RO_PROPERTY(TStrategyOperationSpecPtr, Spec);

private:
    TOperationElementSharedStatePtr OperationElementSharedState_;
    TFairShareStrategyOperationControllerPtr Controller_;

    bool IsRunningInThisPoolTree_ = false;
    TSchedulingTagFilter SchedulingTagFilter_;

    TInstant LastNonStarvingTime_;
    TInstant LastScheduleJobSuccessTime_;

    bool HasJobsSatisfyingResourceLimits(const TFairShareContext& context) const;

    bool IsBlocked(NProfiling::TCpuInstant now) const;

    TJobResources GetHierarchicalAvailableResources(const TFairShareContext& context) const;

    std::optional<EDeactivationReason> TryStartScheduleJob(
        NProfiling::TCpuInstant now,
        const TFairShareContext& context,
        TJobResources* precommittedResourcesOutput,
        TJobResources* availableResourcesOutput);

    void FinishScheduleJob(
        bool enableBackoff,
        NProfiling::TCpuInstant now);

    TControllerScheduleJobResultPtr DoScheduleJob(
        TFairShareContext* context,
        const TJobResources& availableResources,
        TJobResources* precommittedResources);

    TJobResources ComputeResourceDemand() const;
    TJobResources ComputeResourceLimits() const;
    TJobResources ComputeMaxPossibleResourceUsage() const;
    int ComputePendingJobCount() const;

    void UpdatePreemptableJobsList();
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
        const TString& treeId);
    TRootElement(const TRootElement& other);

    virtual void Update(TDynamicAttributesList& dynamicAttributesList, TUpdateFairShareContext* context) override;

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;

    virtual bool IsRoot() const override;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    virtual TString GetId() const override;

    virtual std::optional<double> GetSpecifiedWeight() const override;
    virtual double GetMinShareRatio() const override;
    virtual TJobResources GetMinShareResources() const override;
    virtual double GetMaxShareRatio() const override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetMinSharePreemptionTimeout() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual bool IsAggressiveStarvationEnabled() const override;

    virtual void CheckForStarvation(TInstant now) override;

    virtual int GetMaxRunningOperationCount() const override;
    virtual int GetMaxOperationCount() const override;

    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const override;
    virtual bool AreImmediateOperationsForbidden() const override;

    virtual THashSet<TString> GetAllowedProfilingTags() const override;

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) override;
    TRootElementPtr Clone();
};

DEFINE_REFCOUNTED_TYPE(TRootElement)

////////////////////////////////////////////////////////////////////////////////

inline TJobResources ComputeAvailableResources(
    const TJobResources& resourceLimits,
    const TJobResources& resourceUsage,
    const TJobResources& resourceDiscount)
{
    return resourceLimits - resourceUsage + resourceDiscount;
}

} // namespace NYT::NScheduler

#define FAIR_SHARE_TREE_ELEMENT_INL_H_
#include "fair_share_tree_element-inl.h"
#undef FAIR_SHARE_TREE_ELEMENT_INL_H_
