#pragma once

#include "public.h"
#include "job_metrics.h"
#include "fair_share_tree_element.h"

namespace NYT::NScheduler {


//! Thread affinity: any
struct IFairShareTreeSnapshot
    : public TIntrinsicRefCounted
{
    virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) = 0;
    virtual void ProcessUpdatedJob(const TOperationId& operationId, const TJobId& jobId, const TJobResources& delta) = 0;
    virtual void ProcessFinishedJob(const TOperationId& operationId, const TJobId& jobId) = 0;
    virtual bool HasOperation(const TOperationId& operationId) const = 0;
    virtual void ApplyJobMetricsDelta(const TOperationId& operationId, const TJobMetrics& jobMetricsDelta) = 0;
    virtual const TSchedulingTagFilter& GetNodesFilter() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IFairShareTreeSnapshot);

////////////////////////////////////////////////////////////////////////////////

class TFairShareStrategyOperationState
    : public TIntrinsicRefCounted
{
public:
    using TTreeIdToPoolNameMap = THashMap<TString, TPoolName>;

    DEFINE_BYVAL_RO_PROPERTY(IOperationStrategyHost*, Host);
    DEFINE_BYVAL_RO_PROPERTY(TFairShareStrategyOperationControllerPtr, Controller);
    DEFINE_BYVAL_RW_PROPERTY(bool, Active);
    DEFINE_BYREF_RW_PROPERTY(TTreeIdToPoolNameMap, TreeIdToPoolNameMap);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TString>, ErasedTrees);

public:
    TFairShareStrategyOperationState(IOperationStrategyHost* host)
        : Host_(host)
        , Controller_(New<TFairShareStrategyOperationController>(host))
    { }

    TPoolName GetPoolNameByTreeId(const TString& treeId) const
    {
        auto it = TreeIdToPoolNameMap_.find(treeId);
        YCHECK(it != TreeIdToPoolNameMap_.end());
        return it->second;
    }

    void EraseTree(const TString& treeId)
    {
        ErasedTrees_.push_back(treeId);
        YCHECK(TreeIdToPoolNameMap_.erase(treeId) == 1);
    }
};

using TFairShareStrategyOperationStatePtr = TIntrusivePtr<TFairShareStrategyOperationState>;

////////////////////////////////////////////////////////////////////////////////

struct TOperationUnregistrationResult
{
    std::vector<TOperationId> OperationsToActivate;
};

struct TPoolsUpdateResult
{
    TError Error;
    bool Updated;
};

NProfiling::TTagIdList GetFailReasonProfilingTags(NControllerAgent::EScheduleJobFailReason reason);

////////////////////////////////////////////////////////////////////////////////

class TFairShareTree
    : public TIntrinsicRefCounted
      , public IFairShareTreeHost
{
public:
    TFairShareTree(
        TFairShareStrategyTreeConfigPtr config,
        TFairShareStrategyOperationControllerConfigPtr controllerConfig,
        ISchedulerStrategyHost* host,
        const std::vector<IInvokerPtr>& feasibleInvokers,
        const TString& treeId);
    IFairShareTreeSnapshotPtr CreateSnapshot();

    TFuture<void> ValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName);

    void ValidatePoolLimits(const IOperationStrategyHost* operation, const TPoolName& poolName);

    void ValidatePoolLimitsOnPoolChange(const IOperationStrategyHost* operation, const TPoolName& newPoolName);
    void ValidateAllOperationsCountsOnPoolChange(const TOperationId& operationId, const TPoolName& newPoolName);
    bool RegisterOperation(
        const TFairShareStrategyOperationStatePtr& state,
        const TStrategyOperationSpecPtr& spec,
        const TOperationFairShareTreeRuntimeParametersPtr& runtimeParams);

    // Attaches operation to tree and returns if it can be activated (pools limits are satisfied)
    bool AttachOperation(
        const TFairShareStrategyOperationStatePtr& state,
        TOperationElementPtr& operationElement,
        const TPoolName& poolName);

    TOperationUnregistrationResult UnregisterOperation(
        const TFairShareStrategyOperationStatePtr& state);

    // Detaches operation element from tree but leaves it eligible to be attached in another place in the same tree.
    // Removes operation from waiting queue if operation wasn't active. Returns true if operation was active.
    bool DetachOperation(const TFairShareStrategyOperationStatePtr& state, const TOperationElementPtr& operationElement);

    void DisableOperation(const TFairShareStrategyOperationStatePtr& state);

    void EnableOperation(const TFairShareStrategyOperationStatePtr& state);

    TPoolsUpdateResult UpdatePools(const NYTree::INodePtr& poolsNode);

    bool ChangeOperationPool(
        const TOperationId& operationId,
        const TFairShareStrategyOperationStatePtr& state,
        const TPoolName& newPool);

    TError CheckOperationUnschedulable(
        const TOperationId& operationId,
        TDuration safeTimeout,
        int minScheduleJobCallAttempts,
        THashSet<EDeactivationReason> deactivationReasons);

    void UpdateOperationRuntimeParameters(
        const TOperationId& operationId,
        const TOperationFairShareTreeRuntimeParametersPtr& runtimeParams);

    void UpdateConfig(const TFairShareStrategyTreeConfigPtr& config);

    void UpdateControllerConfig(const TFairShareStrategyOperationControllerConfigPtr& config);

    void BuildOperationAttributes(const TOperationId& operationId, NYTree::TFluentMap fluent);

    void BuildOperationProgress(const TOperationId& operationId, NYTree::TFluentMap fluent);

    void BuildBriefOperationProgress(const TOperationId& operationId, NYTree::TFluentMap fluent);

    void BuildUserToEphemeralPools(NYTree::TFluentAny fluent);

    // NB: This function is public for testing purposes.
    TError OnFairShareUpdateAt(TInstant now);

    void ProfileFairShare() const;

    void ResetTreeIndexes();

    void LogOperationsInfo();

    void LogPoolsInfo();

    // NB: This function is public for testing purposes.
    void OnFairShareLoggingAt(TInstant now);

    // NB: This function is public for testing purposes.
    void OnFairShareEssentialLoggingAt(TInstant now);

    void RegisterJobs(const TOperationId& operationId, const std::vector<TJobPtr>& jobs);

    void BuildPoolsInformation(NYTree::TFluentMap fluent);

    void BuildStaticPoolsInformation(NYTree::TFluentAny fluent);

    void BuildOrchid(NYTree::TFluentMap fluent);

    void BuildFairShareInfo(NYTree::TFluentMap fluent);

    void BuildEssentialFairShareInfo(NYTree::TFluentMap fluent);

    void ResetState();

    const TSchedulingTagFilter& GetNodesFilter() const;

    TPoolName CreatePoolName(const std::optional<TString>& poolFromSpec, const TString& user);

    bool HasOperation(const TOperationId& operationId);

    virtual NProfiling::TAggregateGauge& GetProfilingCounter(const TString& name) override;

private:
    TFairShareStrategyTreeConfigPtr Config;
    TFairShareStrategyOperationControllerConfigPtr ControllerConfig;
    ISchedulerStrategyHost* const Host;

    std::vector<IInvokerPtr> FeasibleInvokers;

    NYTree::INodePtr LastPoolsNodeUpdate;
    TError LastPoolsNodeUpdateError;

    const TString TreeId;
    const NProfiling::TTagId TreeIdProfilingTag;

    const NLogging::TLogger Logger;

    using TPoolMap = THashMap<TString, TPoolPtr>;
    TPoolMap Pools;

    THashMap<TString, NProfiling::TTagId> PoolIdToProfilingTagId;

    THashMap<TString, THashSet<TString>> UserToEphemeralPools;

    THashMap<TString, THashSet<int>> PoolToSpareSlotIndices;
    THashMap<TString, int> PoolToMinUnusedSlotIndex;

    using TOperationElementPtrByIdMap = THashMap<TOperationId, TOperationElementPtr>;
    TOperationElementPtrByIdMap OperationIdToElement;

    THashMap<TOperationId, TInstant> OperationIdToActivationTime_;

    std::list<TOperationId> WaitingOperationQueue;

    NConcurrency::TReaderWriterSpinLock NodeIdToLastPreemptiveSchedulingTimeLock;
    THashMap<NNodeTrackerClient::TNodeId, NProfiling::TCpuInstant> NodeIdToLastPreemptiveSchedulingTime;

    std::vector<TSchedulingTagFilter> RegisteredSchedulingTagFilters;
    std::vector<int> FreeSchedulingTagFilterIndexes;
    struct TSchedulingTagFilterEntry
    {
        int Index;
        int Count;
    };
    THashMap<TSchedulingTagFilter, TSchedulingTagFilterEntry> SchedulingTagFilterToIndexAndCount;

    TRootElementPtr RootElement;

    struct TRootElementSnapshot
        : public TIntrinsicRefCounted
    {
        TRootElementPtr RootElement;
        TOperationElementByIdMap OperationIdToElement;
        TFairShareStrategyTreeConfigPtr Config;
        std::vector<TSchedulingTagFilter> RegisteredSchedulingTagFilters;

        TOperationElement* FindOperationElement(const TOperationId& operationId) const
        {
            auto it = OperationIdToElement.find(operationId);
            return it != OperationIdToElement.end() ? it->second : nullptr;
        }
    };

    typedef TIntrusivePtr<TRootElementSnapshot> TRootElementSnapshotPtr;
    TRootElementSnapshotPtr RootElementSnapshot;

    class TFairShareTreeSnapshot
        : public IFairShareTreeSnapshot
    {
    public:
        TFairShareTreeSnapshot(TFairShareTreePtr tree, TRootElementSnapshotPtr rootElementSnapshot, const NLogging::TLogger& logger)
            : Tree(std::move(tree))
            , RootElementSnapshot(std::move(rootElementSnapshot))
            , Logger(logger)
            , NodesFilter(Tree->GetNodesFilter())
        { }

        virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) override
        {
            return BIND(&TFairShareTree::DoScheduleJobs,
                Tree,
                schedulingContext,
                RootElementSnapshot)
                .AsyncVia(GetCurrentInvoker())
                .Run();
        }

        virtual void ProcessUpdatedJob(const TOperationId& operationId, const TJobId& jobId, const TJobResources& delta)
        {
            // NB: Should be filtered out on large clusters.
            YT_LOG_DEBUG("Processing updated job (OperationId: %v, JobId: %v)", operationId, jobId);
            auto* operationElement = RootElementSnapshot->FindOperationElement(operationId);
            if (operationElement) {
                operationElement->IncreaseJobResourceUsage(jobId, delta);
            }
        }

        virtual void ProcessFinishedJob(const TOperationId& operationId, const TJobId& jobId) override
        {
            // NB: Should be filtered out on large clusters.
            YT_LOG_DEBUG("Processing finished job (OperationId: %v, JobId: %v)", operationId, jobId);
            auto* operationElement = RootElementSnapshot->FindOperationElement(operationId);
            if (operationElement) {
                operationElement->OnJobFinished(jobId);
            }
        }

        virtual void ApplyJobMetricsDelta(const TOperationId& operationId, const TJobMetrics& jobMetricsDelta) override
        {
            auto* operationElement = RootElementSnapshot->FindOperationElement(operationId);
            if (operationElement) {
                operationElement->ApplyJobMetricsDelta(jobMetricsDelta);
            }
        }

        virtual bool HasOperation(const TOperationId& operationId) const override
        {
            auto* operationElement = RootElementSnapshot->FindOperationElement(operationId);
            return operationElement != nullptr;
        }

        virtual const TSchedulingTagFilter& GetNodesFilter() const override
        {
            return NodesFilter;
        }

    private:
        const TIntrusivePtr<TFairShareTree> Tree;
        const TRootElementSnapshotPtr RootElementSnapshot;
        const NLogging::TLogger Logger;
        const TSchedulingTagFilter NodesFilter;
    };

    TDynamicAttributesList GlobalDynamicAttributes_;

    struct TScheduleJobsProfilingCounters
    {
        TScheduleJobsProfilingCounters(const TString& prefix, const NProfiling::TTagId& treeIdProfilingTag)
            : PrescheduleJobTime(prefix + "/preschedule_job_time", {treeIdProfilingTag})
            , TotalControllerScheduleJobTime(prefix + "/controller_schedule_job_time/total", {treeIdProfilingTag})
            , ExecControllerScheduleJobTime(prefix + "/controller_schedule_job_time/exec", {treeIdProfilingTag})
            , StrategyScheduleJobTime(prefix + "/strategy_schedule_job_time", {treeIdProfilingTag})
            , ScheduleJobCount(prefix + "/schedule_job_count", {treeIdProfilingTag})
            , ScheduleJobFailureCount(prefix + "/schedule_job_failure_count", {treeIdProfilingTag})
        {
            for (auto reason : TEnumTraits<NControllerAgent::EScheduleJobFailReason>::GetDomainValues())
            {
                auto tags = GetFailReasonProfilingTags(reason);
                tags.push_back(treeIdProfilingTag);

                ControllerScheduleJobFail[reason] = NProfiling::TMonotonicCounter(
                    prefix + "/controller_schedule_job_fail",
                    tags);
            }
        }

        NProfiling::TAggregateGauge PrescheduleJobTime;
        NProfiling::TAggregateGauge TotalControllerScheduleJobTime;
        NProfiling::TAggregateGauge ExecControllerScheduleJobTime;
        NProfiling::TAggregateGauge StrategyScheduleJobTime;
        NProfiling::TMonotonicCounter ScheduleJobCount;
        NProfiling::TMonotonicCounter ScheduleJobFailureCount;
        TEnumIndexedVector<NProfiling::TMonotonicCounter, NControllerAgent::EScheduleJobFailReason> ControllerScheduleJobFail;
    };

    TScheduleJobsProfilingCounters NonPreemptiveProfilingCounters;
    TScheduleJobsProfilingCounters PreemptiveProfilingCounters;

    NProfiling::TAggregateGauge FairShareUpdateTimeCounter;
    NProfiling::TAggregateGauge FairShareLogTimeCounter;
    NProfiling::TAggregateGauge AnalyzePreemptableJobsTimeCounter;

    TSpinLock CustomProfilingCountersLock_;
    THashMap<TString, std::unique_ptr<NProfiling::TAggregateGauge>> CustomProfilingCounters_;

    NProfiling::TCpuInstant LastSchedulingInformationLoggedTime_ = 0;

    TDynamicAttributes GetGlobalDynamicAttributes(const TSchedulerElementPtr& element) const
    {
        int index = element->GetTreeIndex();
        if (index == UnassignedTreeIndex) {
            return TDynamicAttributes();
        } else {
            YCHECK(index < GlobalDynamicAttributes_.size());
            return GlobalDynamicAttributes_[index];
        }
    }

    void DoScheduleJobsWithoutPreemption(
        const TRootElementSnapshotPtr& rootElementSnapshot,
        TFairShareContext* context,
        NProfiling::TCpuInstant startTime,
        const std::function<void(TScheduleJobsProfilingCounters&, int, TDuration)> profileTimings,
        const std::function<void(TStringBuf)> logAndCleanSchedulingStatistics);

    void DoScheduleJobsWithPreemption(
        const TRootElementSnapshotPtr& rootElementSnapshot,
        TFairShareContext* context,
        NProfiling::TCpuInstant startTime,
        const std::function<void(TScheduleJobsProfilingCounters&, int, TDuration)>& profileTimings,
        const std::function<void(TStringBuf)>& logAndCleanSchedulingStatistics);

    void DoScheduleJobs(
        const ISchedulingContextPtr& schedulingContext,
        const TRootElementSnapshotPtr& rootElementSnapshot);

    void PreemptJob(
        const TJobPtr& job,
        const TOperationElementPtr& operationElement,
        TFairShareContext* context) const;

    TCompositeSchedulerElement* FindPoolViolatingMaxRunningOperationCount(TCompositeSchedulerElement* pool);

    TCompositeSchedulerElementPtr FindPoolWithViolatedOperationCountLimit(const TCompositeSchedulerElementPtr& element);

    void AddOperationToPool(const TOperationId& operationId);

    void DoRegisterPool(const TPoolPtr& pool);

    void RegisterPool(const TPoolPtr& pool);

    void RegisterPool(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent);

    void ReconfigurePool(const TPoolPtr& pool, const TPoolConfigPtr& config);

    void UnregisterPool(const TPoolPtr& pool);

    bool TryAllocatePoolSlotIndex(const TString& poolName, int slotIndex);

    void AllocateOperationSlotIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName);

    void ReleaseOperationSlotIndex(const TFairShareStrategyOperationStatePtr& state, const TString& poolName);

    void TryActivateOperationsFromQueue(std::vector<TOperationId>* operationsToActivate);

    void BuildEssentialOperationProgress(const TOperationId& operationId, NYTree::TFluentMap fluent);

    int RegisterSchedulingTagFilter(const TSchedulingTagFilter& filter);

    void UnregisterSchedulingTagFilter(int index);

    void UnregisterSchedulingTagFilter(const TSchedulingTagFilter& filter);

    void SetPoolParent(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent);

    void SetPoolDefaultParent(const TPoolPtr& pool);

    TPoolPtr FindPool(const TString& id);

    TPoolPtr GetPool(const TString& id);

    NProfiling::TTagId GetPoolProfilingTag(const TString& id);

    TOperationElementPtr FindOperationElement(const TOperationId& operationId);

    TOperationElementPtr GetOperationElement(const TOperationId& operationId);

    TRootElementSnapshotPtr CreateRootElementSnapshot();

    void BuildEssentialPoolsInformation(NYTree::TFluentMap fluent);

    void BuildElementYson(const TSchedulerElementPtr& element, NYTree::TFluentMap fluent);

    void BuildEssentialElementYson(const TSchedulerElementPtr& element, NYTree::TFluentMap fluent, bool shouldPrintResourceUsage);

    void BuildEssentialPoolElementYson(const TSchedulerElementPtr& element, NYTree::TFluentMap fluent);

    void BuildEssentialOperationElementYson(const TSchedulerElementPtr& element, NYTree::TFluentMap fluent);

    NYTree::TYPath GetPoolPath(const TCompositeSchedulerElementPtr& element);

    TCompositeSchedulerElementPtr GetDefaultParent();

    TCompositeSchedulerElementPtr GetPoolOrParent(const TPoolName& poolName);

    void ValidateOperationCountLimit(const IOperationStrategyHost* operation, const TPoolName& poolName);

    void ValidateEphemeralPoolLimit(const IOperationStrategyHost* operation, const TPoolName& poolName);

    void DoValidateOperationPoolsCanBeUsed(const IOperationStrategyHost* operation, const TPoolName& poolName);

    void ProfileOperationElement(TProfileCollector& collector, TOperationElementPtr element) const;

    void ProfileCompositeSchedulerElement(TProfileCollector& collector, TCompositeSchedulerElementPtr element) const;

    void ProfileSchedulerElement(TProfileCollector& collector, const TSchedulerElementPtr& element, const TString& profilingPrefix, const NProfiling::TTagIdList& tags) const;
};

DEFINE_REFCOUNTED_TYPE(TFairShareTree)

} // namespace NYT::NScheduler
