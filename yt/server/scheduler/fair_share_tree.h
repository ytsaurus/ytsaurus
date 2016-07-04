#include "public.h"
#include "config.h"
#include "job.h"
#include "job_resources.h"
#include "scheduler_strategy.h"

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

struct ISchedulerElement;
typedef TIntrusivePtr<ISchedulerElement> ISchedulerElementPtr;

class TOperationElement;
typedef TIntrusivePtr<TOperationElement> TOperationElementPtr;

class TCompositeSchedulerElement;
typedef TIntrusivePtr<TCompositeSchedulerElement> TCompositeSchedulerElementPtr;

class TPool;
typedef TIntrusivePtr<TPool> TPoolPtr;

class TRootElement;
typedef TIntrusivePtr<TRootElement> TRootElementPtr;

struct TFairShareContext;

////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESchedulableStatus,
    (Normal)
    (BelowMinShare)
    (BelowFairShare)
);

////////////////////////////////////////////////////////////////////

struct TSchedulableAttributes
{
    NNodeTrackerClient::EResourceType DominantResource = NNodeTrackerClient::EResourceType::Cpu;
    double DemandRatio = 0.0;
    double FairShareRatio = 0.0;
    double AdjustedMinShareRatio = 0.0;
    double MaxPossibleUsageRatio = 1.0;
    double BestAllocationRatio = 1.0;
    i64 DominantLimit = 0;

    double AdjustedFairShareStarvationTolerance = 1.0;
    TDuration AdjustedMinSharePreemptionTimeout;
    TDuration AdjustedFairSharePreemptionTimeout;
};

struct TDynamicAttributes
{
    double SatisfactionRatio = 0.0;
    bool Active = false;
    ISchedulerElement* BestLeafDescendant = nullptr;
    TInstant MinSubtreeStartTime;
    TJobResources ResourceUsageDiscount = ZeroJobResources();
};

typedef std::vector<TDynamicAttributes> TDynamicAttributesList;

////////////////////////////////////////////////////////////////////

struct ISchedulerElement
    : public TIntrinsicRefCounted
{
    // Enumerates nodes of the tree using inorder traversal. Returns first unused index.
    virtual int EnumerateNodes(int startIndex) = 0;
    virtual int GetTreeIndex() const = 0;

    virtual void Update(TDynamicAttributesList& dynamicAttributesList) = 0;
    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) = 0;
    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList) = 0;

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList) = 0;

    virtual void BuildJobToOperationMapping(TFairShareContext& context) = 0;
    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) = 0;
    virtual bool ScheduleJob(TFairShareContext& context) = 0;

    virtual const TSchedulableAttributes& Attributes() const = 0;
    virtual TSchedulableAttributes& Attributes() = 0;
    virtual void UpdateAttributes() = 0;

    virtual TNullable<Stroka> GetNodeTag() const = 0;

    virtual bool IsActive(const TDynamicAttributesList& dynamicAttributesList) const = 0;

    virtual bool IsAlive() const = 0;
    virtual void SetAlive(bool alive) = 0;

    virtual int GetPendingJobCount() const = 0;

    virtual Stroka GetId() const = 0;

    virtual double GetWeight() const = 0;
    virtual double GetMinShareRatio() const = 0;
    virtual double GetMaxShareRatio() const = 0;

    virtual double GetFairShareStarvationTolerance() const = 0;
    virtual TDuration GetMinSharePreemptionTimeout() const = 0;
    virtual TDuration GetFairSharePreemptionTimeout() const = 0;

    virtual ESchedulableStatus GetStatus() const = 0;

    virtual bool GetStarving() const = 0;
    virtual void SetStarving(bool starving) = 0;
    virtual void CheckForStarvation(TInstant now) = 0;

    virtual const TJobResources& ResourceDemand() const = 0;
    virtual const TJobResources& ResourceLimits() const = 0;
    virtual const TJobResources& MaxPossibleResourceUsage() const = 0;

    virtual TJobResources GetResourceUsage() const = 0;
    virtual double GetResourceUsageRatio() const = 0;
    virtual void IncreaseLocalResourceUsage(const TJobResources& delta) = 0;
    virtual void IncreaseResourceUsage(const TJobResources& delta) = 0;

    virtual TCompositeSchedulerElement* GetParent() const = 0;
    virtual void SetParent(TCompositeSchedulerElement* parent) = 0;

    virtual ISchedulerElementPtr Clone() = 0;
    virtual void SetCloned(bool cloned) = 0;
};

////////////////////////////////////////////////////////////////////

struct TFairShareContext
{
    TFairShareContext(const ISchedulingContextPtr& schedulingContext, int treeSize);

    TDynamicAttributes& DynamicAttributes(ISchedulerElement* element);
    const TDynamicAttributes& DynamicAttributes(ISchedulerElement* element) const;

    const ISchedulingContextPtr SchedulingContext;
    TDynamicAttributesList DynamicAttributesList;
    TDuration TotalScheduleJobDuration;
    TDuration ExecScheduleJobDuration;
    TEnumIndexedVector<int, EScheduleJobFailReason> FailedScheduleJob;
    yhash_map<TJobPtr, TOperationElementPtr> JobToOperationElement;
    bool HasAggressivelyStarvingNodes = false;
};

////////////////////////////////////////////////////////////////////

const int UnassignedTreeIndex = -1;

class TSchedulerElementBaseFixedState
{
protected:
    explicit TSchedulerElementBaseFixedState(ISchedulerStrategyHost* host);

    ISchedulerStrategyHost* const Host_;

    TSchedulableAttributes Attributes_;

    TCompositeSchedulerElement* Parent_ = nullptr;

    TNullable<TInstant> BelowFairShareSince_;
    bool Starving_ = false;

    TJobResources ResourceDemand_;
    TJobResources ResourceLimits_;
    TJobResources MaxPossibleResourceUsage_;
    TJobResources TotalResourceLimits_;

    int PendingJobCount_ = 0;

    int TreeIndex_ = UnassignedTreeIndex;

    bool Cloned_ = false;

};

class TSchedulerElementBaseSharedState
    : public TIntrinsicRefCounted
{
public:
    TSchedulerElementBaseSharedState();

    TJobResources GetResourceUsage();
    void IncreaseResourceUsage(const TJobResources& delta);

    double GetResourceUsageRatio(NNodeTrackerClient::EResourceType dominantResource, double dominantResourceLimit);

    bool GetAlive() const;
    void SetAlive(bool alive);

private:
    TJobResources ResourceUsage_;
    NConcurrency::TReaderWriterSpinLock ResourceUsageLock_;

    std::atomic<bool> Alive_ = {true};

};

typedef TIntrusivePtr<TSchedulerElementBaseSharedState> TSchedulerElementBaseSharedStatePtr;

class TSchedulerElementBase
    : public ISchedulerElement
    , public TSchedulerElementBaseFixedState
{
public:
    virtual int EnumerateNodes(int startIndex) override;

    virtual int GetTreeIndex() const override;

    virtual void Update(TDynamicAttributesList& dynamicAttributesList) override;

    // Updates attributes that need to be computed from leafs up to root.
    // For example: parent->ResourceDemand = Sum(child->ResourceDemand).
    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override;

    // Updates attributes that are propagated from root down to leafs.
    // For example: child->FairShareRatio = fraction(parent->FairShareRatio).
    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList) override;

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList) override;

    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) override;

    virtual const TSchedulableAttributes& Attributes() const override;
    virtual TSchedulableAttributes& Attributes() override;
    virtual void UpdateAttributes() override;

    virtual TNullable<Stroka> GetNodeTag() const override;

    virtual bool IsActive(const TDynamicAttributesList& dynamicAttributesList) const override;

    virtual bool IsAlive() const override;
    virtual void SetAlive(bool alive) override;

    virtual TCompositeSchedulerElement* GetParent() const override;
    virtual void SetParent(TCompositeSchedulerElement* parent) override;

    virtual int GetPendingJobCount() const override;

    virtual ESchedulableStatus GetStatus() const override;

    virtual bool GetStarving() const override;
    virtual void SetStarving(bool starving) override;

    virtual const TJobResources& ResourceDemand() const override;
    virtual const TJobResources& ResourceLimits() const override;
    virtual const TJobResources& MaxPossibleResourceUsage() const override;

    TJobResources GetResourceUsage() const override;
    virtual double GetResourceUsageRatio() const override;
    virtual void IncreaseLocalResourceUsage(const TJobResources& delta) override;

    virtual void SetCloned(bool cloned) override;

protected:
    const TFairShareStrategyConfigPtr StrategyConfig_;

    TSchedulerElementBaseSharedStatePtr SharedState_;

    TSchedulerElementBase(ISchedulerStrategyHost* host, TFairShareStrategyConfigPtr strategyConfig);
    TSchedulerElementBase(const TSchedulerElementBase& other);

    ISchedulerStrategyHost* GetHost() const;

    double ComputeLocalSatisfactionRatio() const;

    ESchedulableStatus GetStatus(double defaultTolerance) const;

    void CheckForStarvationImpl(
        TDuration minSharePreemptionTimeout,
        TDuration fairSharePreemptionTimeout,
        TInstant now);

};

////////////////////////////////////////////////////////////////////

class TCompositeSchedulerElementFixedState
{
protected:
    TCompositeSchedulerElementFixedState();

    ESchedulingMode Mode_ = ESchedulingMode::Fifo;
    std::vector<EFifoSortParameter> FifoSortParameters_;

    DEFINE_BYREF_RW_PROPERTY(int, RunningOperationCount);
    DEFINE_BYREF_RW_PROPERTY(int, OperationCount);

    DEFINE_BYREF_RO_PROPERTY(double, AdjustedFairShareStarvationToleranceLimit);
    DEFINE_BYREF_RO_PROPERTY(TDuration, AdjustedMinSharePreemptionTimeoutLimit);
    DEFINE_BYREF_RO_PROPERTY(TDuration, AdjustedFairSharePreemptionTimeoutLimit);

};

class TCompositeSchedulerElement
    : public TSchedulerElementBase
    , public TCompositeSchedulerElementFixedState
{
public:
    TCompositeSchedulerElement(ISchedulerStrategyHost* host, TFairShareStrategyConfigPtr strategyConfig);
    TCompositeSchedulerElement(const TCompositeSchedulerElement& other);

    virtual int EnumerateNodes(int startIndex) override;

    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override;
    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList) override;

    virtual double GetFairShareStarvationToleranceLimit() const;
    virtual TDuration GetMinSharePreemptionTimeoutLimit() const;
    virtual TDuration GetFairSharePreemptionTimeoutLimit() const;

    void UpdatePreemptionSettingsLimits();
    void UpdateChildPreemptionSettings(const ISchedulerElementPtr& child);

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList) override;

    virtual void BuildJobToOperationMapping(TFairShareContext& context) override;
    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) override;
    virtual bool ScheduleJob(TFairShareContext& context) override;

    virtual void IncreaseResourceUsage(const TJobResources& delta) override;

    virtual bool IsRoot() const;
    virtual bool AggressiveStarvationEnabled() const;

    void AddChild(const ISchedulerElementPtr& child, bool enabled = true);
    void EnableChild(const ISchedulerElementPtr& child);
    void RemoveChild(const ISchedulerElementPtr& child);

    bool IsEmpty() const;

    virtual int GetMaxOperationCount() const = 0;
    virtual int GetMaxRunningOperationCount() const = 0;

protected:
    yhash_set<ISchedulerElementPtr> Children;
    yhash_set<ISchedulerElementPtr> DisabledChildren;

    template <class TGetter, class TSetter>
    void ComputeByFitting(const TGetter& getter, const TSetter& setter, double sum);

    void UpdateFifo(TDynamicAttributesList& dynamicAttributesList);
    void UpdateFairShare(TDynamicAttributesList& dynamicAttributesList);

    ISchedulerElementPtr GetBestActiveChild(const TDynamicAttributesList& dynamicAttributesList) const;
    ISchedulerElementPtr GetBestActiveChildFifo(const TDynamicAttributesList& dynamicAttributesList) const;
    ISchedulerElementPtr GetBestActiveChildFairShare(const TDynamicAttributesList& dynamicAttributesList) const;

};

////////////////////////////////////////////////////////////////////

class TPoolFixedState
{
protected:
    explicit TPoolFixedState(const Stroka& id);

    const Stroka Id_;
    bool DefaultConfigured_ = true;

};

class TPool
    : public TCompositeSchedulerElement
    , public TPoolFixedState
{
public:
    TPool(ISchedulerStrategyHost* host, const Stroka& id, TFairShareStrategyConfigPtr strategyConfig);
    TPool(const TPool& other);

    bool IsDefaultConfigured() const;

    TPoolConfigPtr GetConfig();
    void SetConfig(TPoolConfigPtr config);
    void SetDefaultConfig();

    virtual bool AggressiveStarvationEnabled() const override;

    virtual Stroka GetId() const override;

    virtual double GetWeight() const override;
    virtual double GetMinShareRatio() const override;
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

    virtual TNullable<Stroka> GetNodeTag() const override;

    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override;

    virtual int GetMaxRunningOperationCount() const override;
    virtual int GetMaxOperationCount() const override;

    virtual ISchedulerElementPtr Clone() override;

    NProfiling::TTagId GetProfilingTag() const;

private:
    TPoolConfigPtr Config_;

    NProfiling::TTagId ProfilingTag_;

    void DoSetConfig(TPoolConfigPtr newConfig);

    TJobResources ComputeResourceLimits() const;

};

////////////////////////////////////////////////////////////////////

class TOperationElementFixedState
{
protected:
    explicit TOperationElementFixedState(TOperationPtr operation);

    const TOperationId OperationId_;
    TInstant StartTime_;
    bool IsSchedulable_;
    TOperation* const Operation_;

    DEFINE_BYVAL_RO_PROPERTY(IOperationControllerPtr, Controller);
};

class TOperationElementSharedState
    : public TIntrinsicRefCounted
{
public:
    TOperationElementSharedState();

    void IncreaseJobResourceUsage(const TJobId& jobId, const TJobResources& resourcesDelta);

    void UpdatePreemptableJobsList(
        double fairShareRatio,
        const TJobResources& totalResourceLimits,
        double preemptionSatisfactionThreshold,
        double aggressivePreemptionSatisfactionThreshold);

    bool IsJobExisting(const TJobId& jobId) const;

    bool IsJobPreemptable(const TJobId& jobId, bool aggressivePreemptionEnabled) const;

    int GetPreemptableJobCount() const;
    int GetAggressivelyPreemptableJobCount() const;

    void AddJob(const TJobId& jobId, const TJobResources resourceUsage);
    TJobResources RemoveJob(const TJobId& jobId);

    bool IsBlocked(
        TInstant now,
        int MaxConcurrentScheduleJobCalls,
        TDuration ScheduleJobFailBackoffTime) const;

    bool TryStartScheduleJob(
        TInstant now,
        int maxConcurrentScheduleJobCalls,
        TDuration scheduleJobFailBackoffTime);

    void FinishScheduleJob(
        bool success,
        bool enableBackoff,
        TDuration scheduleJobDuration,
        TInstant now);

    NJobTrackerClient::TStatistics GetControllerTimeStatistics();

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

        //! Determines whether job belongs to list of preemtable or aggressively preemtable jobs of operation.
        bool Preemptable;

        //! Determines whether job belongs to list of preemtable (but not aggressively preemptable) jobs of operation.
        bool AggressivelyPreemptable;

        //! Iterator in the per-operation list pointing to this particular job.
        TJobIdList::iterator JobIdListIterator;

        TJobResources ResourceUsage;

        static void SetPreemptable(TJobProperties* properties) {
            properties->Preemptable = true;
            properties->AggressivelyPreemptable = true;
        }

        static void SetAggressivelyPreemptable(TJobProperties* properties) {
            properties->Preemptable = false;
            properties->AggressivelyPreemptable = true;
        }

        static void SetNonPreemptable(TJobProperties* properties) {
            properties->Preemptable = false;
            properties->AggressivelyPreemptable = false;
        }
    };

    yhash_map<TJobId, TJobProperties> JobPropertiesMap_;
    NConcurrency::TReaderWriterSpinLock JobPropertiesMapLock_;

    int ConcurrentScheduleJobCalls_ = 0;
    TInstant LastScheduleJobFailTime_;
    bool BackingOff_ = false;
    NConcurrency::TReaderWriterSpinLock ConcurrentScheduleJobCallsLock_;

    DEFINE_BYREF_RW_PROPERTY(NJobTrackerClient::TStatistics, ControllerTimeStatistics);

    bool IsBlockedImpl(
        TInstant now,
        int MaxConcurrentScheduleJobCalls,
        TDuration ScheduleJobFailBackoffTime) const;

    void IncreaseJobResourceUsage(TJobProperties& properties, const TJobResources& resourcesDelta);
};

typedef TIntrusivePtr<TOperationElementSharedState> TOperationElementSharedStatePtr;

////////////////////////////////////////////////////////////////////

class TOperationElement
    : public TSchedulerElementBase
    , public TOperationElementFixedState
{
public:
    TOperationElement(
        TFairShareStrategyConfigPtr strategyConfig,
        TStrategyOperationSpecPtr spec,
        TOperationRuntimeParamsPtr runtimeParams,
        ISchedulerStrategyHost* host,
        TOperationPtr operation);

    TOperationElement(const TOperationElement& other);

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetMinSharePreemptionTimeout() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual void UpdateBottomUp(TDynamicAttributesList& dynamicAttributesList) override;
    virtual void UpdateTopDown(TDynamicAttributesList& dynamicAttributesList) override;

    virtual void UpdateDynamicAttributes(TDynamicAttributesList& dynamicAttributesList) override;

    virtual void BuildJobToOperationMapping(TFairShareContext& context) override;
    virtual void PrescheduleJob(TFairShareContext& context, bool starvingOnly) override;
    virtual bool ScheduleJob(TFairShareContext& context) override;

    virtual Stroka GetId() const override;

    virtual double GetWeight() const override;
    virtual double GetMinShareRatio() const override;
    virtual double GetMaxShareRatio() const override;

    virtual TNullable<Stroka> GetNodeTag() const override;

    virtual ESchedulableStatus GetStatus() const override;

    virtual void SetStarving(bool starving) override;
    virtual void CheckForStarvation(TInstant now) override;
    bool HasStarvingParent() const;

    virtual void IncreaseResourceUsage(const TJobResources& delta) override;

    void IncreaseJobResourceUsage(const TJobId& jobId, const TJobResources& resourcesDelta);

    bool IsJobExisting(const TJobId& jobId) const;

    bool IsJobPreemptable(const TJobId& jobId, bool aggressivePreemptionEnabled) const;

    int GetPreemptableJobCount() const;
    int GetAggressivelyPreemptableJobCount() const;

    void OnJobStarted(const TJobId& jobId, const TJobResources& resourceUsage);
    void OnJobFinished(const TJobId& jobId);

    NJobTrackerClient::TStatistics GetControllerTimeStatistics();

    virtual ISchedulerElementPtr Clone() override;

    DEFINE_BYVAL_RW_PROPERTY(TOperationRuntimeParamsPtr, RuntimeParams);

    DEFINE_BYVAL_RO_PROPERTY(TStrategyOperationSpecPtr, Spec);

private:
    TOperationElementSharedStatePtr SharedState_;

    TOperation* GetOperation() const;

    bool IsBlocked(TInstant now) const;

    TJobResources GetHierarchicalResourceLimits(const TFairShareContext& context) const;

    TScheduleJobResultPtr DoScheduleJob(TFairShareContext& context);

    TJobResources ComputeResourceDemand() const;
    TJobResources ComputeResourceLimits() const;
    TJobResources ComputeMaxPossibleResourceUsage() const;
    int ComputePendingJobCount() const;

};

////////////////////////////////////////////////////////////////////

class TRootElementFixedState
{
protected:
    DEFINE_BYVAL_RO_PROPERTY(int, TreeSize);

};

class TRootElement
    : public TCompositeSchedulerElement
    , public TRootElementFixedState
{
public:
    TRootElement(ISchedulerStrategyHost* host, TFairShareStrategyConfigPtr strategyConfig);

    virtual void Update(TDynamicAttributesList& dynamicAttributesList) override;

    virtual bool IsRoot() const override;

    virtual TNullable<Stroka> GetNodeTag() const override;

    virtual Stroka GetId() const override;

    virtual double GetWeight() const override;
    virtual double GetMinShareRatio() const override;
    virtual double GetMaxShareRatio() const override;

    virtual double GetFairShareStarvationTolerance() const override;
    virtual TDuration GetMinSharePreemptionTimeout() const override;
    virtual TDuration GetFairSharePreemptionTimeout() const override;

    virtual void CheckForStarvation(TInstant now) override;

    virtual int GetMaxRunningOperationCount() const override;
    virtual int GetMaxOperationCount() const override;

    virtual ISchedulerElementPtr Clone() override;

    TRootElementPtr CloneRoot();

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
