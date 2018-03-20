#pragma once

#include "private.h"
#include "config.h"
#include "job.h"
#include "job_metrics.h"
#include "scheduler_strategy.h"
#include "scheduling_tag.h"
#include "fair_share_strategy_operation_controller.h"

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDeactivationReason,
    (IsNotAlive)
    (UnmatchedSchedulingTag)
    (IsNotStarving)
    (IsBlocked)
    (TryStartScheduleJobFailed)
    (ScheduleJobFailed)
    (NoBestLeafDescendant)
    (MinNeededResourcesUnsatisfied)
);

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

struct TFairShareContext
{
    explicit TFairShareContext(const ISchedulingContextPtr& schedulingContext);

    void Initialize(int treeSize, const std::vector<TSchedulingTagFilter>& registeredSchedulingTagFilters);

    TDynamicAttributes& DynamicAttributes(const TSchedulerElement* element);
    const TDynamicAttributes& DynamicAttributes(const TSchedulerElement* element) const;

    bool Initialized = false;

    std::vector<bool> CanSchedule;
    TDynamicAttributesList DynamicAttributesList;

    const ISchedulingContextPtr SchedulingContext;
    TDuration TotalScheduleJobDuration;
    TDuration ExecScheduleJobDuration;
    TEnumIndexedVector<int, NControllerAgent::EScheduleJobFailReason> FailedScheduleJob;
    bool HasAggressivelyStarvingNodes = false;

    int ActiveOperationCount = 0;
    int ActiveTreeSize = 0;
    TEnumIndexedVector<int, EDeactivationReason> DeactivationReasons;

    // Used to avoid unnecessary calculation of HasAggressivelyStarvingNodes.
    bool PrescheduledCalled = false;

    // Information saved for logging.
    int ControllerScheduleJobCount = 0;
    int PreemptiveScheduleJobAttempts = 0;
    int NonPreemptiveScheduleJobAttempts = 0;
    TJobResources ResourceUsageDiscount = ZeroJobResources();
    int ScheduledDuringPreemption = 0;
    int PreemptableJobCount = 0;
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
    explicit TSchedulerElementFixedState(
        ISchedulerStrategyHost* host,
        const TFairShareStrategyTreeConfigPtr& treeConfig,
        const TString& treeId);

    ISchedulerStrategyHost* const Host_;

    TFairShareStrategyTreeConfigPtr TreeConfig_;

    TCompositeSchedulerElement* Parent_ = nullptr;

    TNullable<TInstant> BelowFairShareSince_;
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
public:
    TJobResources GetResourceUsage();
    TJobResources GetResourceUsagePrecommit();
    TJobMetrics GetJobMetrics();
    void IncreaseResourceUsage(const TJobResources& delta);
    void IncreaseResourceUsagePrecommit(const TJobResources& delta);
    void ApplyJobMetricsDelta(const TJobMetrics& delta);

    double GetResourceUsageRatio(NNodeTrackerClient::EResourceType dominantResource, double dominantResourceLimit);

    bool GetAlive() const;
    void SetAlive(bool alive);

    double GetFairShareRatio() const;
    void SetFairShareRatio(double fairShareRatio);

private:
    TJobResources ResourceUsage_;
    TJobResources ResourceUsagePrecommit_;
    TJobMetrics JobMetrics_;
    NConcurrency::TReaderWriterSpinLock ResourceUsageLock_;
    NConcurrency::TReaderWriterSpinLock JobMetricsLock_;

    // NB: Avoid false sharing between ResourceUsageLock_ and others.
    char Padding[64];

    std::atomic<bool> Alive_ = {true};

    std::atomic<double> FairShareRatio_ = {0.0};
};

DEFINE_REFCOUNTED_TYPE(TSchedulerElementSharedState)

class TSchedulerElement
    : public TSchedulerElementFixedState
    , public TIntrinsicRefCounted
{
public:
    //! Enumerates nodes of the tree using inorder traversal. Returns first unused index.
    virtual int EnumerateNodes(int startIndex);

    int GetTreeIndex() const;

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config);

    virtual void Update(TDynamicAttributesList& dynamicAttributesList);

    //! Updates attributes that need to be computed from leafs up to root.
    //! For example: |parent->ResourceDemand = Sum(child->ResourceDemand)|.
    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList);

    //! Updates attributes that are propagated from root down to leafs.
    //! For example: |child->FairShareRatio = fraction(parent->FairShareRatio)|.
    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList);

    virtual TJobResources ComputePossibleResourceUsage(TJobResources limit) const = 0;

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList);

    virtual void PrescheduleJob(TFairShareContext* context, bool starvingOnly, bool aggressiveStarvationEnabled);
    virtual bool ScheduleJob(TFairShareContext* context) = 0;

    virtual bool HasAggressivelyStarvingNodes(TFairShareContext* context, bool aggressiveStarvationEnabled) const = 0;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const;

    bool IsActive(const TDynamicAttributesList& dynamicAttributesList) const;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const = 0;

    bool IsAlive() const;
    void SetAlive(bool alive);

    double GetFairShareRatio() const;
    void SetFairShareRatio(double fairShareRatio);

    virtual TString GetId() const = 0;

    virtual double GetWeight() const = 0;
    virtual double GetMinShareRatio() const = 0;
    virtual TJobResources GetMinShareResources() const = 0;
    virtual double GetMaxShareRatio() const = 0;

    virtual double GetFairShareStarvationTolerance() const = 0;
    virtual TDuration GetMinSharePreemptionTimeout() const = 0;
    virtual TDuration GetFairSharePreemptionTimeout() const = 0;

    TCompositeSchedulerElement* GetParent() const;
    void SetParent(TCompositeSchedulerElement* parent);

    TInstant GetStartTime() const;
    int GetPendingJobCount() const;

    virtual ESchedulableStatus GetStatus() const;

    bool GetStarving() const;
    virtual void SetStarving(bool starving);
    virtual void CheckForStarvation(TInstant now) = 0;

    TJobResources GetResourceUsage() const;
    TJobResources GetResourceUsagePrecommit() const;
    TJobMetrics GetJobMetrics() const;
    double GetResourceUsageRatio() const;

    virtual TString GetTreeId() const;

    void IncreaseLocalResourceUsage(const TJobResources& delta);
    void IncreaseLocalResourceUsagePrecommit(const TJobResources& delta);
    void ApplyJobMetricsDeltaLocal(const TJobMetrics& delta);
    virtual void IncreaseResourceUsage(const TJobResources& delta) = 0;
    virtual void IncreaseResourceUsagePrecommit(const TJobResources& delta) = 0;
    virtual void ApplyJobMetricsDelta(const TJobMetrics& delta) = 0;

    virtual void BuildOperationToElementMapping(TOperationElementByIdMap* operationElementByIdMap) = 0;

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) = 0;

private:
    TSchedulerElementSharedStatePtr SharedState_;

protected:
    TSchedulerElement(
        ISchedulerStrategyHost* host,
        const TFairShareStrategyTreeConfigPtr& treeConfig,
        const TString& treeId);
    TSchedulerElement(
        const TSchedulerElement& other,
        TCompositeSchedulerElement* clonedParent);

    ISchedulerStrategyHost* GetHost() const;

    double ComputeLocalSatisfactionRatio() const;

    ESchedulableStatus GetStatus(double defaultTolerance) const;

    void CheckForStarvationImpl(
        TDuration minSharePreemptionTimeout,
        TDuration fairSharePreemptionTimeout,
        TInstant now);

    void SetOperationAlert(
        const TOperationId& operationId,
        EOperationAlertType alertType,
        const TError& alert);

private:
    void UpdateAttributes();
};

DEFINE_REFCOUNTED_TYPE(TSchedulerElement)

////////////////////////////////////////////////////////////////////////////////

class TCompositeSchedulerElementFixedState
{
public:
    DEFINE_BYREF_RW_PROPERTY(int, RunningOperationCount);
    DEFINE_BYREF_RW_PROPERTY(int, OperationCount);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TError>, UpdateFairShareAlerts);

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
        TFairShareStrategyTreeConfigPtr treeConfig,
        NProfiling::TTagId profilingTag,
        const TString& treeId);
    TCompositeSchedulerElement(
        const TCompositeSchedulerElement& other,
        TCompositeSchedulerElement* clonedParent);

    virtual int EnumerateNodes(int startIndex) override;

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;

    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override;
    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList) override;

    virtual TJobResources ComputePossibleResourceUsage(TJobResources limit) const override;

    virtual double GetFairShareStarvationToleranceLimit() const;
    virtual TDuration GetMinSharePreemptionTimeoutLimit() const;
    virtual TDuration GetFairSharePreemptionTimeoutLimit() const;

    void UpdatePreemptionSettingsLimits();
    void UpdateChildPreemptionSettings(const TSchedulerElementPtr& child);

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList) override;

    virtual void PrescheduleJob(TFairShareContext* context, bool starvingOnly, bool aggressiveStarvationEnabled) override;
    virtual bool ScheduleJob(TFairShareContext* context) override;

    virtual bool HasAggressivelyStarvingNodes(TFairShareContext* context, bool aggressiveStarvationEnabled) const override;

    virtual void IncreaseResourceUsage(const TJobResources& delta) override;
    virtual void IncreaseResourceUsagePrecommit(const TJobResources& delta) override;
    virtual void ApplyJobMetricsDelta(const TJobMetrics& delta) override;

    virtual bool IsRoot() const;
    virtual bool IsExplicit() const;
    virtual bool IsAggressiveStarvationEnabled() const;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;

    void AddChild(const TSchedulerElementPtr& child, bool enabled = true);
    void EnableChild(const TSchedulerElementPtr& child);
    void RemoveChild(const TSchedulerElementPtr& child);

    bool IsEmpty() const;

    ESchedulingMode GetMode() const;
    void SetMode(ESchedulingMode);

    NProfiling::TTagId GetProfilingTag() const;

    virtual int GetMaxOperationCount() const = 0;
    virtual int GetMaxRunningOperationCount() const = 0;

    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const = 0;
    virtual bool AreImmediateOperationsFobidden() const = 0;

    virtual void BuildOperationToElementMapping(TOperationElementByIdMap* operationElementByIdMap) override;

    virtual void IncreaseOperationCount(int delta);
    virtual void IncreaseRunningOperationCount(int delta);
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

    void UpdateFifo(TDynamicAttributesList& dynamicAttributesList);
    void UpdateFairShare(TDynamicAttributesList& dynamicAttributesList);

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
    TNullable<TString> UserName_;
};

class TPool
    : public TCompositeSchedulerElement
    , public TPoolFixedState
{
public:
    TPool(
        ISchedulerStrategyHost* host,
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

    void SetUserName(const TNullable<TString>& userName);
    const TNullable<TString>& GetUserName() const;

    TPoolConfigPtr GetConfig();
    void SetConfig(TPoolConfigPtr config);
    void SetDefaultConfig();

    virtual bool IsExplicit() const override;
    virtual bool IsAggressiveStarvationEnabled() const override;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;

    virtual TString GetId() const override;

    virtual double GetWeight() const override;
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
    virtual bool AreImmediateOperationsFobidden() const override;

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) override;

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
    explicit TOperationElementFixedState(
        IOperationStrategyHost* operation,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig);

    const TOperationId OperationId_;
    bool Schedulable_;
    IOperationStrategyHost* const Operation_;
    TFairShareStrategyOperationControllerConfigPtr ControllerConfig_;
};

////////////////////////////////////////////////////////////////////////////////

class TOperationElementSharedState
    : public TIntrinsicRefCounted
{
public:
    TJobResources IncreaseJobResourceUsage(
        const TJobId& jobId,
        const TJobResources& resourcesDelta);

    void UpdatePreemptableJobsList(
        double fairShareRatio,
        const TJobResources& totalResourceLimits,
        double preemptionSatisfactionThreshold,
        double aggressivePreemptionSatisfactionThreshold,
        int* moveCount);

    bool IsJobKnown(const TJobId& jobId) const;

    bool IsJobPreemptable(const TJobId& jobId, bool aggressivePreemptionEnabled) const;

    int GetRunningJobCount() const;
    int GetPreemptableJobCount() const;
    int GetAggressivelyPreemptableJobCount() const;

    TJobResources AddJob(const TJobId& jobId, const TJobResources& resourceUsage);
    TJobResources RemoveJob(const TJobId& jobId);

    TJobResources Finalize();

private:
    template <typename T>
    class TListWithSize
    {
    public:
        using iterator = typename std::list<T>::iterator;

        void push_front(const T& value)
        {
            Impl_.push_front(value);
            ++Size_;
        }

        void push_back(const T& value)
        {
            Impl_.push_back(value);
            ++Size_;
        }

        void pop_front()
        {
            Impl_.pop_front();
            --Size_;
        }

        void pop_back()
        {
            Impl_.pop_back();
            --Size_;
        }

        void erase(iterator it)
        {
            Impl_.erase(it);
            --Size_;
        }

        iterator begin()
        {
            return Impl_.begin();
        }

        iterator end()
        {
            return Impl_.end();
        }

        const T& front() const
        {
            return Impl_.front();
        }

        const T& back() const
        {
            return Impl_.back();
        }

        size_t size() const
        {
            return Size_;
        }

        bool empty() const
        {
            return Size_ == 0;
        }

    private:
        std::list<T> Impl_;
        size_t Size_ = 0;

    };

    typedef TListWithSize<TJobId> TJobIdList;

    TJobIdList NonpreemptableJobs_;
    TJobIdList AggressivelyPreemptableJobs_;
    TJobIdList PreemptableJobs_;
    std::atomic<int> RunningJobCount_ = {0};

    TJobResources NonpreemptableResourceUsage_;
    TJobResources AggressivelyPreemptableResourceUsage_;

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

        //! Determines whether job belongs to list of preemptable or aggressively preemtable jobs of operation.
        bool Preemptable;

        //! Determines whether job belongs to list of preemptable (but not aggressively preemptable) jobs of operation.
        bool AggressivelyPreemptable;

        //! Iterator in the per-operation list pointing to this particular job.
        TJobIdList::iterator JobIdListIterator;

        TJobResources ResourceUsage;
    };

    THashMap<TJobId, TJobProperties> JobPropertiesMap_;
    NConcurrency::TReaderWriterSpinLock JobPropertiesMapLock_;

    bool Finalized_ = false;

    void IncreaseJobResourceUsage(TJobProperties* properties, const TJobResources& resourcesDelta);

    TJobProperties* GetJobProperties(const TJobId& jobId);
    const TJobProperties* GetJobProperties(const TJobId& jobId) const;
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
        TOperationFairShareStrategyTreeOptionsPtr runtimeParams,
        TFairShareStrategyOperationControllerPtr controller,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        ISchedulerStrategyHost* host,
        IOperationStrategyHost* operation,
        const TString& treeId);
    TOperationElement(
        const TOperationElement& other,
        TCompositeSchedulerElement* clonedParent);

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetMinSharePreemptionTimeout() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override;
    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList) override;

    void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config);

    virtual TJobResources ComputePossibleResourceUsage(TJobResources limit) const override;

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList) override;

    virtual void PrescheduleJob(TFairShareContext* context, bool starvingOnly, bool aggressiveStarvationEnabled) override;
    virtual bool ScheduleJob(TFairShareContext* context) override;

    virtual bool HasAggressivelyStarvingNodes(TFairShareContext* context, bool aggressiveStarvationEnabled) const override;

    virtual TString GetId() const override;

    bool IsSchedulable() const;

    virtual bool IsAggressiveStarvationPreemptionAllowed() const override;

    virtual double GetWeight() const override;
    virtual double GetMinShareRatio() const override;
    virtual TJobResources GetMinShareResources() const override;
    virtual double GetMaxShareRatio() const override;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    virtual ESchedulableStatus GetStatus() const override;

    virtual void SetStarving(bool starving) override;
    virtual void CheckForStarvation(TInstant now) override;
    bool IsPreemptionAllowed(const TFairShareContext& context) const;

    virtual void IncreaseResourceUsage(const TJobResources& delta) override;
    virtual void IncreaseResourceUsagePrecommit(const TJobResources& delta) override;
    virtual void ApplyJobMetricsDelta(const TJobMetrics& delta) override;

    void IncreaseJobResourceUsage(const TJobId& jobId, const TJobResources& resourcesDelta);

    bool IsJobKnown(const TJobId& jobId) const;

    bool IsJobPreemptable(const TJobId& jobId, bool aggressivePreemptionEnabled) const;

    int GetRunningJobCount() const;
    int GetPreemptableJobCount() const;
    int GetAggressivelyPreemptableJobCount() const;

    int GetSlotIndex() const;

    void OnJobStarted(const TJobId& jobId, const TJobResources& resourceUsage);
    void OnJobFinished(const TJobId& jobId);

    virtual void BuildOperationToElementMapping(TOperationElementByIdMap* operationElementByIdMap) override;

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) override;

    TJobResources Finalize();

    DEFINE_BYVAL_RW_PROPERTY(TOperationFairShareStrategyTreeOptionsPtr, RuntimeParams);

    DEFINE_BYVAL_RO_PROPERTY(TStrategyOperationSpecPtr, Spec);

    TSchedulingTagFilter SchedulingTagFilter_;

private:
    TOperationElementSharedStatePtr SharedState_;
    TFairShareStrategyOperationControllerPtr Controller_;

    bool HasJobsSatisfyingResourceLimits(const TFairShareContext& context) const;

    bool IsBlocked(NProfiling::TCpuInstant now) const;

    TJobResources GetHierarchicalResourceLimits(const TFairShareContext& context) const;

    bool TryStartScheduleJob(
        NProfiling::TCpuInstant now,
        const TJobResources& jobLimits,
        const TJobResources& minNeededResources);

    void FinishScheduleJob(
        bool enableBackoff,
        NProfiling::TCpuInstant now,
        const TJobResources& minNeededResources);

    NControllerAgent::TScheduleJobResultPtr DoScheduleJob(
        TFairShareContext* context,
        const TJobResources& jobLimits,
        const TJobResources& jobResourceDiscount);

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
        TFairShareStrategyTreeConfigPtr treeConfig,
        NProfiling::TTagId profilingTag,
        const TString& treeId);
    TRootElement(const TRootElement& other);

    virtual void Update(TDynamicAttributesList& dynamicAttributesList) override;

    virtual void UpdateTreeConfig(const TFairShareStrategyTreeConfigPtr& config) override;

    virtual bool IsRoot() const override;

    virtual const TSchedulingTagFilter& GetSchedulingTagFilter() const override;

    virtual TString GetId() const override;

    virtual double GetWeight() const override;
    virtual double GetMinShareRatio() const override;
    virtual TJobResources GetMinShareResources() const override;
    virtual double GetMaxShareRatio() const override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetMinSharePreemptionTimeout() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual void CheckForStarvation(TInstant now) override;

    virtual int GetMaxRunningOperationCount() const override;
    virtual int GetMaxOperationCount() const override;

    virtual std::vector<EFifoSortParameter> GetFifoSortParameters() const override;
    virtual bool AreImmediateOperationsFobidden() const override;

    virtual TSchedulerElementPtr Clone(TCompositeSchedulerElement* clonedParent) override;
    TRootElementPtr Clone();
};

DEFINE_REFCOUNTED_TYPE(TRootElement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

#define FAIR_SHARE_TREE_ELEMENT_INL_H_
#include "fair_share_tree_element-inl.h"
#undef FAIR_SHARE_TREE_ELEMENT_INL_H_
