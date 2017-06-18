#include "fair_share_strategy.h"
#include "fair_share_tree.h"
#include "public.h"
#include "config.h"
#include "job_resources.h"
#include "scheduler_strategy.h"

#include <yt/core/concurrency/async_rw_lock.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/scoped_timer.h>

namespace NYT {
namespace NScheduler {

using namespace NConcurrency;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NYson;
using namespace NYTree;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;
static const auto& Profiler = SchedulerProfiler;

////////////////////////////////////////////////////////////////////

TTagIdList GetFailReasonProfilingTags(EScheduleJobFailReason reason)
{
    static std::unordered_map<Stroka, TTagId> tagId;

    auto reasonAsString = ToString(reason);
    auto it = tagId.find(reasonAsString);
    if (it == tagId.end()) {
        it = tagId.emplace(
            reasonAsString,
            TProfileManager::Get()->RegisterTag("reason", reasonAsString)
        ).first;
    }
    return {it->second};
};

////////////////////////////////////////////////////////////////////

class TFairShareStrategy
    : public ISchedulerStrategy
{
public:
    TFairShareStrategy(
        TFairShareStrategyConfigPtr config,
        ISchedulerStrategyHost* host)
        : Config(config)
        , Host(host)
        , NonPreemptiveProfilingCounters("/non_preemptive")
        , PreemptiveProfilingCounters("/preemptive")
        , LastProfilingTime_(TInstant::Zero())
    {
        RootElement = New<TRootElement>(Host, GetPoolProfilingTag(RootPoolName), config);

        FairShareUpdateExecutor_ = New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TFairShareStrategy::OnFairShareUpdate, MakeWeak(this)),
            Config->FairShareUpdatePeriod);

        FairShareLoggingExecutor_ = New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TFairShareStrategy::OnFairShareLogging, MakeWeak(this)),
            Config->FairShareLogPeriod);
    }

    virtual TFuture<void> ScheduleJobs(const ISchedulingContextPtr& schedulingContext) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto asyncGuard = TAsyncLockReaderGuard::Acquire(&ScheduleJobsLock);

        TRootElementSnapshotPtr rootElementSnapshot;
        {
            TReaderGuard guard(RootElementSnapshotLock);
            rootElementSnapshot = RootElementSnapshot;
        }

        auto jobScheduler =
            BIND(
                &TFairShareStrategy::DoScheduleJobs,
                MakeStrong(this),
                schedulingContext,
                rootElementSnapshot)
            .AsyncVia(GetCurrentInvoker());
        return asyncGuard.Apply(jobScheduler);
    }

    virtual void StartPeriodicActivity() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        FairShareLoggingExecutor_->Start();
        FairShareUpdateExecutor_->Start();
    }

    virtual void ResetState() override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        FairShareLoggingExecutor_->Stop();
        FairShareUpdateExecutor_->Stop();

        // Do fair share update in order to rebuild tree snapshot
        // to drop references to old nodes.
        OnFairShareUpdate();

        LastPoolsNodeUpdate.Reset();
    }

    virtual TFuture<void> ValidateOperationStart(const TOperationPtr& operation) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return BIND(&TFairShareStrategy::DoValidateOperationStart, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run(operation);
    }

    virtual void ValidateOperationCanBeRegistered(const TOperationPtr& operation) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ValidateOperationCountLimit(operation);
        ValidateEphemeralPoolLimit(operation);
    }

    void RegisterOperation(const TOperationPtr& operation) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto spec = ParseSpec(operation, operation->GetSpec());
        auto params = BuildInitialRuntimeParams(spec);
        auto operationElement = New<TOperationElement>(
            Config,
            spec,
            params,
            Host,
            operation);

        int index = RegisterSchedulingTagFilter(TSchedulingTagFilter(spec->SchedulingTagFilter));
        operationElement->SetSchedulingTagFilterIndex(index);

        YCHECK(OperationIdToElement.insert(std::make_pair(operation->GetId(), operationElement)).second);

        const auto& userName = operation->GetAuthenticatedUser();

        auto poolId = spec->Pool ? *spec->Pool : userName;
        auto pool = FindPool(poolId);
        if (!pool) {
            pool = New<TPool>(Host, poolId, GetPoolProfilingTag(poolId), Config);
            pool->SetUserName(userName);
            UserToEphemeralPools[userName].insert(poolId);
            RegisterPool(pool);
        }
        if (!pool->GetParent()) {
            SetPoolDefaultParent(pool);
        }

        IncreaseOperationCount(pool.Get(), 1);

        pool->AddChild(operationElement, false);
        pool->IncreaseResourceUsage(operationElement->GetResourceUsage());
        operationElement->SetParent(pool.Get());

        if (CanAddOperationToPool(pool.Get())) {
            ActivateOperation(operation->GetId());
        } else {
            OperationQueue.push_back(operation);
        }
    }

    void UnregisterOperation(const TOperationPtr& operation) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationElement = GetOperationElement(operation->GetId());
        auto* pool = static_cast<TPool*>(operationElement->GetParent());

        UnregisterSchedulingTagFilter(operationElement->GetSchedulingTagFilterIndex());

        auto finalResourceUsage = operationElement->Finalize();
        YCHECK(OperationIdToElement.erase(operation->GetId()) == 1);
        operationElement->SetAlive(false);
        pool->RemoveChild(operationElement);
        pool->IncreaseResourceUsage(-finalResourceUsage);
        IncreaseOperationCount(pool, -1);

        LOG_INFO("Operation removed from pool (OperationId: %v, Pool: %v)",
            operation->GetId(),
            pool->GetId());

        bool isPending = false;
        for (auto it = OperationQueue.begin(); it != OperationQueue.end(); ++it) {
            if (*it == operation) {
                isPending = true;
                OperationQueue.erase(it);
                break;
            }
        }

        if (!isPending) {
            IncreaseRunningOperationCount(pool, -1);

            // Try to run operations from queue.
            auto it = OperationQueue.begin();
            while (it != OperationQueue.end() && RootElement->RunningOperationCount() < Config->MaxRunningOperationCount) {
                const auto& operation = *it;
                auto* operationPool = GetOperationElement(operation->GetId())->GetParent();
                if (CanAddOperationToPool(operationPool)) {
                    ActivateOperation(operation->GetId());
                    auto toRemove = it++;
                    OperationQueue.erase(toRemove);
                } else {
                    ++it;
                }
            }
        }

        if (pool->IsEmpty() && pool->IsDefaultConfigured()) {
            UnregisterPool(pool);
        }
    }

    void ProcessUpdatedAndCompletedJobs(
        const std::vector<TUpdatedJob>& updatedJobs,
        const std::vector<TCompletedJob>& completedJobs)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TRootElementSnapshotPtr rootElementSnapshot;
        {
            TReaderGuard guard(RootElementSnapshotLock);
            rootElementSnapshot = RootElementSnapshot;
        }

        for (const auto& job : updatedJobs) {
            auto* operationElement = rootElementSnapshot->FindOperationElement(job.OperationId);
            if (operationElement) {
                operationElement->IncreaseJobResourceUsage(job.JobId, job.Delta);
            }
        }

        for (const auto& job : completedJobs) {
            auto* operationElement = rootElementSnapshot->FindOperationElement(job.OperationId);
            if (operationElement) {
                operationElement->OnJobFinished(job.JobId);
            }
        }
    }

    void UpdatePools(const INodePtr& poolsNode) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (LastPoolsNodeUpdate && AreNodesEqual(LastPoolsNodeUpdate, poolsNode)) {
            LOG_INFO("Pools are not changed, skipping update");
            return;
        }
        LastPoolsNodeUpdate = poolsNode;

        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&ScheduleJobsLock)).Value();

        std::vector<TError> errors;

        try {
            // Build the set of potential orphans.
            yhash_set<Stroka> orphanPoolIds;
            for (const auto& pair : Pools) {
                YCHECK(orphanPoolIds.insert(pair.first).second);
            }

            // Track ids appearing in various branches of the tree.
            yhash<Stroka, TYPath> poolIdToPath;

            // NB: std::function is needed by parseConfig to capture itself.
            std::function<void(INodePtr, TCompositeSchedulerElementPtr)> parseConfig =
                [&] (INodePtr configNode, TCompositeSchedulerElementPtr parent) {
                    auto configMap = configNode->AsMap();
                    for (const auto& pair : configMap->GetChildren()) {
                        const auto& childId = pair.first;
                        const auto& childNode = pair.second;
                        auto childPath = childNode->GetPath();
                        if (!poolIdToPath.insert(std::make_pair(childId, childPath)).second) {
                            errors.emplace_back(
                                "Pool %Qv is defined both at %v and %v; skipping second occurrence",
                                childId,
                                poolIdToPath[childId],
                                childPath);
                            continue;
                        }

                        // Parse config.
                        auto configNode = ConvertToNode(childNode->Attributes());
                        TPoolConfigPtr config;
                        try {
                            config = ConvertTo<TPoolConfigPtr>(configNode);
                        } catch (const std::exception& ex) {
                            errors.emplace_back(
                                TError(
                                    "Error parsing configuration of pool %Qv; using defaults",
                                    childPath)
                                << ex);
                            config = New<TPoolConfig>();
                        }

                        try {
                            config->Validate();
                        } catch (const std::exception& ex) {
                            errors.emplace_back(
                                TError(
                                    "Misconfiguration of pool %Qv found",
                                    childPath)
                                << ex);
                        }

                        auto pool = FindPool(childId);
                        if (pool) {
                            // Reconfigure existing pool.
                            pool->SetConfig(config);
                            YCHECK(orphanPoolIds.erase(childId) == 1);
                        } else {
                            // Create new pool.
                            pool = New<TPool>(Host, childId, GetPoolProfilingTag(childId), Config);
                            pool->SetConfig(config);
                            RegisterPool(pool, parent);
                        }
                        SetPoolParent(pool, parent);

                        if (parent->GetMode() == ESchedulingMode::Fifo) {
                            parent->SetMode(ESchedulingMode::FairShare);
                            errors.emplace_back(
                                TError(
                                    "Pool %Qv cannot have subpools since it is in %v mode",
                                    parent->GetId(),
                                    ESchedulingMode::Fifo));
                        }

                        // Parse children.
                        parseConfig(childNode, pool.Get());
                    }
                };

            // Run recursive descent parsing.
            parseConfig(poolsNode, RootElement);

            // Unregister orphan pools.
            for (const auto& id : orphanPoolIds) {
                auto pool = GetPool(id);
                if (pool->IsEmpty()) {
                    UnregisterPool(pool);
                } else {
                    pool->SetDefaultConfig();
                    SetPoolDefaultParent(pool);
                }
            }

            RootElement->Update(GlobalDynamicAttributes_);
            AssignRootElementSnapshot(CreateRootElementSnapshot());
        } catch (const std::exception& ex) {
            auto error = TError("Error updating pools")
                << ex;
            Host->RegisterAlert(EAlertType::UpdatePools, error);
            return;
        }

        if (!errors.empty()) {
            auto combinedError = TError("Found pool configuration issues");
            combinedError.InnerErrors() = std::move(errors);
            Host->RegisterAlert(EAlertType::UpdatePools, combinedError);
        } else {
            Host->UnregisterAlert(EAlertType::UpdatePools);
            LOG_INFO("Pools updated");
        }
    }

    void UpdateOperationRuntimeParams(
        const TOperationPtr& operation,
        const INodePtr& update) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = FindOperationElement(operation->GetId());
        if (!element)
            return;

        NLogging::TLogger Logger(SchedulerLogger);
        Logger.AddTag("OperationId: %v", operation->GetId());

        try {
            auto newRuntimeParams = CloneYsonSerializable(element->GetRuntimeParams());
            if (ReconfigureYsonSerializable(newRuntimeParams, update)) {
                element->SetRuntimeParams(newRuntimeParams);
                LOG_INFO("Operation runtime parameters updated");
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing operation runtime parameters");
        }
    }

    void UpdateConfig(const TFairShareStrategyConfigPtr& config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        Config = config;
        RootElement->UpdateStrategyConfig(Config);
    }

    virtual void BuildOperationAttributes(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = GetOperationElement(operationId);
        auto serializedParams = ConvertToAttributes(element->GetRuntimeParams());
        BuildYsonMapFluently(consumer)
            .Items(*serializedParams)
            .Item("pool").Value(element->GetParent()->GetId());
    }

    virtual void BuildOperationInfoForEventLog(
        const TOperationPtr& operation,
        NYson::IYsonConsumer* consumer) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildYsonMapFluently(consumer)
            .Item("pool").Value(GetOperationPoolName(operation));
    }

    virtual void BuildOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        auto* parent = element->GetParent();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(parent->GetId())
            .Item("start_time").Value(element->GetStartTime())
            .Item("preemptable_job_count").Value(element->GetPreemptableJobCount())
            .Item("aggressively_preemptable_job_count").Value(element->GetAggressivelyPreemptableJobCount())
            .Item("fifo_index").Value(element->Attributes().FifoIndex)
            .Do(BIND(&TFairShareStrategy::BuildElementYson, Unretained(this), element));
    }

    virtual void BuildBriefOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        auto* parent = element->GetParent();
        const auto& attributes = element->Attributes();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(parent->GetId())
            .Item("fair_share_ratio").Value(attributes.FairShareRatio);
    }

    virtual void BuildOrchid(IYsonConsumer* consumer) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        BuildPoolsInformation(consumer);
        BuildYsonMapFluently(consumer)
            .Item("fair_share_info").BeginMap()
                .Do(BIND(&TFairShareStrategy::BuildFairShareInfo, Unretained(this)))
            .EndMap()
            .Item("user_to_ephemeral_pools").Value(UserToEphemeralPools);
    }

    virtual Stroka GetOperationLoggingProgress(const TOperationId& operationId) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = GetOperationElement(operationId);
        const auto& attributes = element->Attributes();
        auto dynamicAttributes = GetGlobalDynamicAttributes(element);

        return Format(
            "Scheduling = {Status: %v, DominantResource: %v, Demand: %.4lf, "
            "Usage: %.4lf, FairShare: %.4lf, Satisfaction: %.4lg, AdjustedMinShare: %.4lf, "
            "GuaranteedResourcesRatio: %.4lf, "
            "MaxPossibleUsage: %.4lf,  BestAllocation: %.4lf, "
            "Starving: %v, Weight: %v, "
            "PreemptableRunningJobs: %v, "
            "AggressivelyPreemptableRunningJobs: %v}",
            element->GetStatus(),
            attributes.DominantResource,
            attributes.DemandRatio,
            element->GetResourceUsageRatio(),
            attributes.FairShareRatio,
            dynamicAttributes.SatisfactionRatio,
            attributes.AdjustedMinShareRatio,
            attributes.GuaranteedResourcesRatio,
            attributes.MaxPossibleUsageRatio,
            attributes.BestAllocationRatio,
            element->GetStarving(),
            element->GetWeight(),
            element->GetPreemptableJobCount(),
            element->GetAggressivelyPreemptableJobCount());
    }

    virtual void BuildBriefSpec(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = GetOperationElement(operationId);
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(element->GetParent()->GetId());
    }

    // NB: This function is public for testing purposes.
    virtual void OnFairShareUpdateAt(TInstant now) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Starting fair share update");

        // Run periodic update.
        PROFILE_TIMING ("/fair_share_update_time") {
            // The root element gets the whole cluster.
            RootElement->Update(GlobalDynamicAttributes_);

            // Collect alerts after update.
            std::vector<TError> alerts;
            for (const auto& pair : Pools) {
                const auto& poolAlerts = pair.second->UpdateFairShareAlerts();
                alerts.insert(alerts.end(), poolAlerts.begin(), poolAlerts.end());
            }
            const auto& rootElementAlerts = RootElement->UpdateFairShareAlerts();
            alerts.insert(alerts.end(), rootElementAlerts.begin(), rootElementAlerts.end());
            if (alerts.empty()) {
                Host->UnregisterAlert(EAlertType::UpdateFairShare);
            } else {
                auto error = TError("Found pool configuration issues during fair share update");
                error.InnerErrors() = std::move(alerts);
                Host->RegisterAlert(EAlertType::UpdateFairShare, error);
            }

            // Update starvation flags for all operations.
            for (const auto& pair : OperationIdToElement) {
                pair.second->CheckForStarvation(now);
            }

            // Update starvation flags for all pools.
            if (Config->EnablePoolStarvation) {
                for (const auto& pair : Pools) {
                    pair.second->CheckForStarvation(now);
                }
            }

            AssignRootElementSnapshot(CreateRootElementSnapshot());

            // Profiling.
            if (LastProfilingTime_ + Config->FairShareProfilingPeriod < now) {
                LastProfilingTime_ = now;
                for (const auto& pair : Pools) {
                    ProfileSchedulerElement(pair.second);
                }
                ProfileSchedulerElement(RootElement);
            }
        }

        LOG_INFO("Fair share successfully updated");
    }

    // NB: This function is public for testing purposes.
    virtual void OnFairShareLoggingAt(TInstant now) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        PROFILE_TIMING ("/fair_share_log_time") {
            // Log pools information.

            Host->LogEventFluently(ELogEventType::FairShareInfo, now)
                .Do(BIND(&TFairShareStrategy::BuildFairShareInfo, Unretained(this)));

            for (const auto& pair : OperationIdToElement) {
                const auto& operationId = pair.first;
                LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
                    GetOperationLoggingProgress(operationId),
                    operationId);
            }
        }
    }

private:
    TFairShareStrategyConfigPtr Config;
    ISchedulerStrategyHost* const Host;

    INodePtr LastPoolsNodeUpdate;

    using TPoolMap = yhash<Stroka, TPoolPtr>;
    TPoolMap Pools;

    yhash_map<Stroka, NProfiling::TTagId> PoolIdToProfilingTagId;

    yhash<Stroka, yhash_set<Stroka>> UserToEphemeralPools;

    typedef yhash<TOperationId, TOperationElementPtr> TOperationElementPtrByIdMap;
    TOperationElementPtrByIdMap OperationIdToElement;

    std::list<TOperationPtr> OperationQueue;

    std::vector<TSchedulingTagFilter> RegisteredSchedulingTagFilter;
    std::vector<int> FreeSchedulingTagFilterIndexes;
    struct TSchedulingTagFilterEntry
    {
        int Index;
        int Count;
    };
    yhash<TSchedulingTagFilter, TSchedulingTagFilterEntry> SchedulingTagFilterToIndexAndCount;

    TRootElementPtr RootElement;

    struct TRootElementSnapshot
        : public TIntrinsicRefCounted
    {
        TRootElementPtr RootElement;
        TOperationElementByIdMap OperationIdToElement;
        TFairShareStrategyConfigPtr Config;

        TOperationElement* FindOperationElement(const TOperationId& operationId) const
        {
            auto it = OperationIdToElement.find(operationId);
            return it != OperationIdToElement.end() ? it->second : nullptr;
        }
    };

    typedef TIntrusivePtr<TRootElementSnapshot> TRootElementSnapshotPtr;

    TReaderWriterSpinLock RootElementSnapshotLock;
    TRootElementSnapshotPtr RootElementSnapshot;

    TAsyncReaderWriterLock ScheduleJobsLock;

    TDynamicAttributesList GlobalDynamicAttributes_;

    struct TProfilingCounters
    {
        TProfilingCounters(const Stroka& prefix)
            : PrescheduleJobTimeCounter(prefix + "/preschedule_job_time")
            , TotalControllerScheduleJobTimeCounter(prefix + "/controller_schedule_job_time/total")
            , ExecControllerScheduleJobTimeCounter(prefix + "/controller_schedule_job_time/exec")
            , StrategyScheduleJobTimeCounter(prefix + "/strategy_schedule_job_time")
            , ScheduleJobCallCounter(prefix + "/schedule_job_count")
        {
            for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues())
            {
                ControllerScheduleJobFailCounter[reason] = TSimpleCounter(
                    prefix + "/controller_schedule_job_fail",
                    GetFailReasonProfilingTags(reason));
            }
        }

        TAggregateCounter PrescheduleJobTimeCounter;
        TAggregateCounter TotalControllerScheduleJobTimeCounter;
        TAggregateCounter ExecControllerScheduleJobTimeCounter;
        TAggregateCounter StrategyScheduleJobTimeCounter;
        TAggregateCounter ScheduleJobCallCounter;

        TEnumIndexedVector<TSimpleCounter, EScheduleJobFailReason> ControllerScheduleJobFailCounter;
    };

    TProfilingCounters NonPreemptiveProfilingCounters;
    TProfilingCounters PreemptiveProfilingCounters;

    TInstant LastProfilingTime_;

    TPeriodicExecutorPtr FairShareUpdateExecutor_;
    TPeriodicExecutorPtr FairShareLoggingExecutor_;

    TCpuInstant LastSchedulingInformationLoggedTime_ = 0;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    TDynamicAttributes GetGlobalDynamicAttributes(const TSchedulerElementPtr& element) const
    {
        int index = element->GetTreeIndex();
        if (index == UnassignedTreeIndex) {
            return TDynamicAttributes();
        } else {
            return GlobalDynamicAttributes_[index];
        }
    }

    void OnFairShareUpdate()
    {
        OnFairShareUpdateAt(TInstant::Now());
    }

    void OnFairShareLogging()
    {
        OnFairShareLoggingAt(TInstant::Now());
    }

    void DoScheduleJobs(
        const ISchedulingContextPtr& schedulingContext,
        const TRootElementSnapshotPtr& rootElementSnapshot,
        const TIntrusivePtr<TAsyncLockReaderGuard>& /*guard*/)
    {
        auto& rootElement = rootElementSnapshot->RootElement;
        auto& config = rootElementSnapshot->Config;
        auto context = TFairShareContext(schedulingContext, rootElement->GetTreeSize(), RegisteredSchedulingTagFilter);

        auto profileTimings = [&] (
            TProfilingCounters& counters,
            int scheduleJobCount,
            TDuration scheduleJobDurationWithoutControllers)
        {
            Profiler.Update(
                counters.StrategyScheduleJobTimeCounter,
                scheduleJobDurationWithoutControllers.MicroSeconds());

            Profiler.Update(
                counters.TotalControllerScheduleJobTimeCounter,
                context.TotalScheduleJobDuration.MicroSeconds());

            Profiler.Update(
                counters.ExecControllerScheduleJobTimeCounter,
                context.ExecScheduleJobDuration.MicroSeconds());

            Profiler.Update(counters.ScheduleJobCallCounter, scheduleJobCount);

            for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
                Profiler.Increment(
                    counters.ControllerScheduleJobFailCounter[reason],
                    context.FailedScheduleJob[reason]);
            }
        };

        bool enableSchedulingInfoLogging = false;
        auto now = GetCpuInstant();
        if (LastSchedulingInformationLoggedTime_ + DurationToCpuDuration(Config->HeartbeatTreeSchedulingInfoLogBackoff) < now) {
            enableSchedulingInfoLogging = true;
            LastSchedulingInformationLoggedTime_ = now;
        }

        auto logAndCleanSchedulingStatistics = [&] (const Stroka& stageName) {
            if (!enableSchedulingInfoLogging) {
                return;
            }
            LOG_DEBUG("%v scheduling statistics (ActiveTreeSize: %v, ActiveOperationCount: %v, DeactivationReasons: %v, CanStartMoreJobs: %v)",
                stageName,
                context.ActiveTreeSize,
                context.ActiveOperationCount,
                context.DeactivationReasons,
                schedulingContext->CanStartMoreJobs());
            context.ActiveTreeSize = 0;
            context.ActiveOperationCount = 0;
            for (auto& item : context.DeactivationReasons) {
                item = 0;
            }
        };

        // First-chance scheduling.
        int nonPreemptiveScheduleJobCount = 0;
        {
            LOG_TRACE("Scheduling new jobs");
            PROFILE_AGGREGATED_TIMING(NonPreemptiveProfilingCounters.PrescheduleJobTimeCounter) {
                rootElement->PrescheduleJob(context, /*starvingOnly*/ false, /*aggressiveStarvationEnabled*/ false);
            }

            TScopedTimer timer;
            while (schedulingContext->CanStartMoreJobs()) {
                ++nonPreemptiveScheduleJobCount;
                if (!rootElement->ScheduleJob(context)) {
                    break;
                }
            }
            profileTimings(
                NonPreemptiveProfilingCounters,
                nonPreemptiveScheduleJobCount,
                timer.GetElapsed() - context.TotalScheduleJobDuration);

            if (nonPreemptiveScheduleJobCount > 0) {
                logAndCleanSchedulingStatistics("Non preemtive");
            }
        }

        // Compute discount to node usage.
        LOG_TRACE("Looking for preemptable jobs");
        yhash_set<TCompositeSchedulerElementPtr> discountedPools;
        std::vector<TJobPtr> preemptableJobs;
        PROFILE_TIMING ("/analyze_preemptable_jobs_time") {
            for (const auto& job : schedulingContext->RunningJobs()) {
                auto* operationElement = rootElementSnapshot->FindOperationElement(job->GetOperationId());
                if (!operationElement || !operationElement->IsJobExisting(job->GetId())) {
                    LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v)",
                        job->GetId(),
                        job->GetOperationId());
                    continue;
                }

                if (operationElement->HasStarvingParent()) {
                    continue;
                }

                if (IsJobPreemptable(job, operationElement, context.HasAggressivelyStarvingNodes, config)) {
                    auto* parent = operationElement->GetParent();
                    while (parent) {
                        discountedPools.insert(parent);
                        context.DynamicAttributes(parent).ResourceUsageDiscount += job->ResourceUsage();
                        parent = parent->GetParent();
                    }
                    schedulingContext->ResourceUsageDiscount() += job->ResourceUsage();
                    preemptableJobs.push_back(job);
                }
            }
        }

        auto resourceDiscount = schedulingContext->ResourceUsageDiscount();
        int startedBeforePreemption = schedulingContext->StartedJobs().size();

        // Second-chance scheduling.
        // NB: Schedule at most one job.
        TJobPtr jobStartedUsingPreemption;
        int preemptiveScheduleJobCount = 0;
        {
            LOG_TRACE("Scheduling new jobs with preemption");
            PROFILE_AGGREGATED_TIMING(PreemptiveProfilingCounters.PrescheduleJobTimeCounter) {
                rootElement->PrescheduleJob(context, /*starvingOnly*/ true, /*aggressiveStarvationEnabled*/ false);
            }

            // Clean data from previous profiling.
            context.TotalScheduleJobDuration = TDuration::Zero();
            context.ExecScheduleJobDuration = TDuration::Zero();
            std::fill(context.FailedScheduleJob.begin(), context.FailedScheduleJob.end(), 0);

            TScopedTimer timer;
            while (schedulingContext->CanStartMoreJobs()) {
                ++preemptiveScheduleJobCount;
                if (!rootElement->ScheduleJob(context)) {
                    break;
                }
                if (schedulingContext->StartedJobs().size() > startedBeforePreemption) {
                    jobStartedUsingPreemption = schedulingContext->StartedJobs().back();
                    break;
                }
            }
            profileTimings(
                PreemptiveProfilingCounters,
                preemptiveScheduleJobCount,
                timer.GetElapsed() - context.TotalScheduleJobDuration);
            if (preemptiveScheduleJobCount > 0) {
                logAndCleanSchedulingStatistics("Preemtive");
            }
        }

        int startedAfterPreemption = schedulingContext->StartedJobs().size();
        int scheduledDuringPreemption = startedAfterPreemption - startedBeforePreemption;

        // Reset discounts.
        schedulingContext->ResourceUsageDiscount() = ZeroJobResources();
        for (const auto& pool : discountedPools) {
            context.DynamicAttributes(pool.Get()).ResourceUsageDiscount = ZeroJobResources();
        }

        // Preempt jobs if needed.
        std::sort(
            preemptableJobs.begin(),
            preemptableJobs.end(),
            [] (const TJobPtr& lhs, const TJobPtr& rhs) {
                return lhs->GetStartTime() > rhs->GetStartTime();
            });

        auto findPoolWithViolatedLimitsForJob = [&] (const TJobPtr& job) -> TCompositeSchedulerElement* {
            auto* operationElement = rootElementSnapshot->FindOperationElement(job->GetOperationId());
            if (!operationElement) {
                return nullptr;
            }

            auto* parent = operationElement->GetParent();
            while (parent) {
                if (!Dominates(parent->ResourceLimits(), parent->GetResourceUsage())) {
                    return parent;
                }
                parent = parent->GetParent();
            }
            return nullptr;
        };

        auto findPoolWithViolatedLimits = [&] () -> TCompositeSchedulerElement* {
            for (const auto& job : schedulingContext->StartedJobs()) {
                auto violatedPool = findPoolWithViolatedLimitsForJob(job);
                if (violatedPool) {
                    return violatedPool;
                }
            }
            return nullptr;
        };

        bool nodeLimitsViolated = true;
        bool poolsLimitsViolated = true;

        for (const auto& job : preemptableJobs) {
            auto* operationElement = rootElementSnapshot->FindOperationElement(job->GetOperationId());
            if (!operationElement || !operationElement->IsJobExisting(job->GetId())) {
                LOG_DEBUG("Dangling preemptable job found (JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());
                continue;
            }

            // Update flags only if violation is not resolved yet to avoid costly computations.
            if (nodeLimitsViolated) {
                nodeLimitsViolated = !Dominates(schedulingContext->ResourceLimits(), schedulingContext->ResourceUsage());
            }
            if (!nodeLimitsViolated && poolsLimitsViolated) {
                poolsLimitsViolated = findPoolWithViolatedLimits() == nullptr;
            }

            if (!nodeLimitsViolated && !poolsLimitsViolated) {
                break;
            }

            if (nodeLimitsViolated) {
                if (jobStartedUsingPreemption) {
                    job->SetPreemptionReason(Format("Preempted to start job %v of operation %v",
                        jobStartedUsingPreemption->GetId(),
                        jobStartedUsingPreemption->GetOperationId()));
                } else {
                    job->SetPreemptionReason(Format("Node resource limits violated"));
                }
                PreemptJob(job, operationElement, context);
            }
            if (poolsLimitsViolated) {
                auto violatedPool = findPoolWithViolatedLimitsForJob(job);
                if (violatedPool) {
                    job->SetPreemptionReason(Format("Preempted due to violation of limits on pool %v",
                        violatedPool->GetId()));
                    PreemptJob(job, operationElement, context);
                }
            }
        }

        LOG_DEBUG("Heartbeat info (StartedJobs: %v, PreemptedJobs: %v, "
            "JobsScheduledDuringPreemption: %v, PreemptableJobs: %v, PreemptableResources: %v, "
            "NonPreemptiveScheduleJobCount: %v, PreemptiveScheduleJobCount: %v, HasAggressivelyStarvingNodes: %v, Address: %v)",
            schedulingContext->StartedJobs().size(),
            schedulingContext->PreemptedJobs().size(),
            scheduledDuringPreemption,
            preemptableJobs.size(),
            FormatResources(resourceDiscount),
            nonPreemptiveScheduleJobCount,
            preemptiveScheduleJobCount,
            context.HasAggressivelyStarvingNodes,
            schedulingContext->GetNodeDescriptor().Address);
    }

    bool IsJobPreemptable(
        const TJobPtr& job,
        const TOperationElementPtr& element,
        bool aggressivePreemptionEnabled,
        const TFairShareStrategyConfigPtr& config) const
    {
        int jobCount = element->GetRunningJobCount();
        if (jobCount <= config->MaxUnpreemptableRunningJobCount) {
            return false;
        }

        double usageRatio = element->GetResourceUsageRatio();
        const auto& attributes = element->Attributes();
        auto threshold = aggressivePreemptionEnabled
            ? config->AggressivePreemptionSatisfactionThreshold
            : config->PreemptionSatisfactionThreshold;
        if (usageRatio < attributes.FairShareRatio * threshold) {
            return false;
        }

        if (!element->IsJobPreemptable(job->GetId(), aggressivePreemptionEnabled)) {
            return false;
        }

        return true;
    }

    void PreemptJob(
        const TJobPtr& job,
        const TOperationElementPtr& operationElement,
        TFairShareContext& context) const
    {
        context.SchedulingContext->ResourceUsage() -= job->ResourceUsage();
        operationElement->IncreaseJobResourceUsage(job->GetId(), -job->ResourceUsage());
        job->ResourceUsage() = ZeroJobResources();

        context.SchedulingContext->PreemptJob(job);
    }


    TStrategyOperationSpecPtr ParseSpec(const TOperationPtr& operation, INodePtr specNode)
    {
        try {
            return ConvertTo<TStrategyOperationSpecPtr>(specNode);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error parsing strategy spec of operation %v, defaults will be used",
                operation->GetId());
            return New<TStrategyOperationSpec>();
        }
    }

    TOperationRuntimeParamsPtr BuildInitialRuntimeParams(const TStrategyOperationSpecPtr& spec)
    {
        auto params = New<TOperationRuntimeParams>();
        params->Weight = spec->Weight;
        return params;
    }

    bool CanAddOperationToPool(TCompositeSchedulerElement* pool)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        while (pool) {
            if (pool->RunningOperationCount() >= pool->GetMaxRunningOperationCount()) {
                return false;
            }
            pool = pool->GetParent();
        }
        return true;
    }

    TCompositeSchedulerElementPtr FindPoolWithViolatedOperationCountLimit(const TCompositeSchedulerElementPtr& element)
    {
        auto current = element;
        while (current) {
            if (current->OperationCount() >= current->GetMaxOperationCount()) {
                return current;
            }
            current = current->GetParent();
        }
        return nullptr;
    }

    void IncreaseOperationCount(TCompositeSchedulerElement* element, int delta)
    {
        while (element) {
            element->OperationCount() += delta;
            element = element->GetParent();
        }
    }

    void IncreaseRunningOperationCount(TCompositeSchedulerElement* element, int delta)
    {
        while (element) {
            element->RunningOperationCount() += delta;
            element = element->GetParent();
        }
    }

    void ActivateOperation(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& operationElement = GetOperationElement(operationId);
        auto* parent = operationElement->GetParent();
        parent->EnableChild(operationElement);
        IncreaseRunningOperationCount(parent, 1);

        Host->ActivateOperation(operationId);

        LOG_INFO("Operation added to pool (OperationId: %v, Pool: %v)",
            operationId,
            parent->GetId());
    }

    void RegisterPool(const TPoolPtr& pool)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        int index = RegisterSchedulingTagFilter(TSchedulingTagFilter(pool->GetConfig()->SchedulingTagFilter));
        pool->SetSchedulingTagFilterIndex(index);
        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        LOG_INFO("Pool registered (Pool: %v)", pool->GetId());
    }

    void RegisterPool(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YCHECK(Pools.insert(std::make_pair(pool->GetId(), pool)).second);
        pool->SetParent(parent.Get());
        parent->AddChild(pool);

        LOG_INFO("Pool registered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    void UnregisterPool(const TPoolPtr& pool)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto userName = pool->GetUserName();
        if (userName) {
            YCHECK(UserToEphemeralPools[*userName].erase(pool->GetId()) == 1);
        }

        UnregisterSchedulingTagFilter(pool->GetSchedulingTagFilterIndex());

        YCHECK(Pools.erase(pool->GetId()) == 1);
        pool->SetAlive(false);
        auto parent = pool->GetParent();
        SetPoolParent(pool, nullptr);

        LOG_INFO("Pool unregistered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    int RegisterSchedulingTagFilter(const TSchedulingTagFilter& filter)
    {
        if (filter.IsEmpty()) {
            return EmptySchedulingTagFilterIndex;
        }
        auto it = SchedulingTagFilterToIndexAndCount.find(filter);
        if (it == SchedulingTagFilterToIndexAndCount.end()) {
            int index;
            if (FreeSchedulingTagFilterIndexes.empty()) {
                index = RegisteredSchedulingTagFilter.size();
                RegisteredSchedulingTagFilter.push_back(filter);
            } else {
                index = FreeSchedulingTagFilterIndexes.back();
                RegisteredSchedulingTagFilter[index] = filter;
                FreeSchedulingTagFilterIndexes.pop_back();
            }
            SchedulingTagFilterToIndexAndCount.emplace(filter, TSchedulingTagFilterEntry({index, 1}));
            return index;
        } else {
            ++it->second.Count;
            return it->second.Index;
        }
    }

    void UnregisterSchedulingTagFilter(int index)
    {
        if (index == EmptySchedulingTagFilterIndex) {
            return;
        }
        UnregisterSchedulingTagFilter(RegisteredSchedulingTagFilter[index]);
    }

    void UnregisterSchedulingTagFilter(const TSchedulingTagFilter& filter)
    {
        if (filter.IsEmpty()) {
            return;
        }
        auto it = SchedulingTagFilterToIndexAndCount.find(filter);
        YCHECK(it != SchedulingTagFilterToIndexAndCount.end());
        --it->second.Count;
        if (it->second.Count == 0) {
            RegisteredSchedulingTagFilter[it->second.Index] = EmptySchedulingTagFilter;
            FreeSchedulingTagFilterIndexes.push_back(it->second.Index);
            SchedulingTagFilterToIndexAndCount.erase(it);
        }
    }

    void SetPoolParent(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (pool->GetParent() == parent)
            return;

        auto* oldParent = pool->GetParent();
        if (oldParent) {
            oldParent->IncreaseResourceUsage(-pool->GetResourceUsage());
            IncreaseOperationCount(oldParent, -pool->OperationCount());
            IncreaseRunningOperationCount(oldParent, -pool->RunningOperationCount());
            oldParent->RemoveChild(pool);
        }

        pool->SetParent(parent.Get());
        if (parent) {
            parent->AddChild(pool);
            parent->IncreaseResourceUsage(pool->GetResourceUsage());
            IncreaseOperationCount(parent.Get(), pool->OperationCount());
            IncreaseRunningOperationCount(parent.Get(), pool->RunningOperationCount());

            LOG_INFO("Parent pool set (Pool: %v, Parent: %v)",
                pool->GetId(),
                parent->GetId());
        }
    }

    void SetPoolDefaultParent(const TPoolPtr& pool)
    {
        auto defaultParentPool = FindPool(Config->DefaultParentPool);
        if (!defaultParentPool || defaultParentPool == pool) {
            // NB: root element is not a pool, so we should suppress warning in this special case.
            if (Config->DefaultParentPool != RootPoolName) {
                auto error = TError("Default parent pool %Qv is not registered", Config->DefaultParentPool);
                Host->RegisterAlert(EAlertType::UpdatePools, error);
            }
            SetPoolParent(pool, RootElement);
        } else {
            SetPoolParent(pool, defaultParentPool);
        }
    }

    TPoolPtr FindPool(const Stroka& id)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto it = Pools.find(id);
        return it == Pools.end() ? nullptr : it->second;
    }

    TPoolPtr GetPool(const Stroka& id)
    {
        auto pool = FindPool(id);
        YCHECK(pool);
        return pool;
    }

    NProfiling::TTagId GetPoolProfilingTag(const Stroka& id)
    {
        auto it = PoolIdToProfilingTagId.find(id);
        if (it == PoolIdToProfilingTagId.end()) {
            it = PoolIdToProfilingTagId.emplace(
                id,
                NProfiling::TProfileManager::Get()->RegisterTag("pool", id)
            ).first;
        }
        return it->second;
    }


    TOperationElementPtr FindOperationElement(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto it = OperationIdToElement.find(operationId);
        return it == OperationIdToElement.end() ? nullptr : it->second;
    }

    TOperationElementPtr GetOperationElement(const TOperationId& operationId)
    {
        auto element = FindOperationElement(operationId);
        YCHECK(element);
        return element;
    }

    TRootElementSnapshotPtr CreateRootElementSnapshot()
    {
        auto snapshot = New<TRootElementSnapshot>();
        snapshot->RootElement = RootElement->Clone();
        snapshot->RootElement->BuildOperationToElementMapping(&snapshot->OperationIdToElement);
        snapshot->Config = Config;
        return snapshot;
    }

    void AssignRootElementSnapshot(TRootElementSnapshotPtr rootElementSnapshot)
    {
        // NB: Avoid destroying the cloned tree inside critical section.
        TWriterGuard guard(RootElementSnapshotLock);
        std::swap(RootElementSnapshot, rootElementSnapshot);
    }

    void BuildPoolsInformation(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("pools").DoMapFor(Pools, [&] (TFluentMap fluent, const TPoolMap::value_type& pair) {
                const auto& id = pair.first;
                const auto& pool = pair.second;
                const auto& config = pool->GetConfig();
                fluent
                    .Item(id).BeginMap()
                        .Item("mode").Value(config->Mode)
                        .Item("running_operation_count").Value(pool->RunningOperationCount())
                        .Item("operation_count").Value(pool->OperationCount())
                        .Item("max_running_operation_count").Value(pool->GetMaxRunningOperationCount())
                        .Item("max_operation_count").Value(pool->GetMaxOperationCount())
                        .Item("aggressive_starvation_enabled").Value(pool->IsAggressiveStarvationEnabled())
                        .DoIf(config->Mode == ESchedulingMode::Fifo, [&] (TFluentMap fluent) {
                            fluent
                                .Item("fifo_sort_parameters").Value(config->FifoSortParameters);
                        })
                        .DoIf(pool->GetParent(), [&] (TFluentMap fluent) {
                            fluent
                                .Item("parent").Value(pool->GetParent()->GetId());
                        })
                        .Do(BIND(&TFairShareStrategy::BuildElementYson, Unretained(this), pool))
                    .EndMap();
            });
    }

    void BuildFairShareInfo(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Do(BIND(&TFairShareStrategy::BuildPoolsInformation, Unretained(this)))
            .Item("operations").DoMapFor(
                OperationIdToElement,
                [=] (TFluentMap fluent, const TOperationElementPtrByIdMap::value_type& pair) {
                    const auto& operationId = pair.first;
                    BuildYsonMapFluently(fluent)
                        .Item(ToString(operationId)).BeginMap()
                            .Do(BIND(&TFairShareStrategy::BuildOperationProgress, Unretained(this), operationId))
                        .EndMap();
                });
    }

    void BuildElementYson(const TSchedulerElementPtr& element, IYsonConsumer* consumer)
    {
        const auto& attributes = element->Attributes();
        auto dynamicAttributes = GetGlobalDynamicAttributes(element);

        auto guaranteedResources = Host->GetMainNodesResourceLimits() * attributes.GuaranteedResourcesRatio;

        BuildYsonMapFluently(consumer)
            .Item("scheduling_status").Value(element->GetStatus())
            .Item("starving").Value(element->GetStarving())
            .Item("fair_share_starvation_tolerance").Value(element->GetFairShareStarvationTolerance())
            .Item("min_share_preemption_timeout").Value(element->GetMinSharePreemptionTimeout())
            .Item("fair_share_preemption_timeout").Value(element->GetFairSharePreemptionTimeout())
            .Item("adjusted_fair_share_starvation_tolerance").Value(attributes.AdjustedFairShareStarvationTolerance)
            .Item("adjusted_min_share_preemption_timeout").Value(attributes.AdjustedMinSharePreemptionTimeout)
            .Item("adjusted_fair_share_preemption_timeout").Value(attributes.AdjustedFairSharePreemptionTimeout)
            .Item("resource_demand").Value(element->ResourceDemand())
            .Item("resource_usage").Value(element->GetResourceUsage())
            .Item("resource_limits").Value(element->ResourceLimits())
            .Item("dominant_resource").Value(attributes.DominantResource)
            .Item("weight").Value(element->GetWeight())
            .Item("min_share_ratio").Value(element->GetMinShareRatio())
            .Item("max_share_ratio").Value(element->GetMaxShareRatio())
            .Item("adjusted_min_share_ratio").Value(attributes.AdjustedMinShareRatio)
            .Item("guaranteed_resources_ratio").Value(attributes.GuaranteedResourcesRatio)
            .Item("guaranteed_resources").Value(guaranteedResources)
            .Item("max_possible_usage_ratio").Value(attributes.MaxPossibleUsageRatio)
            .Item("usage_ratio").Value(element->GetResourceUsageRatio())
            .Item("demand_ratio").Value(attributes.DemandRatio)
            .Item("fair_share_ratio").Value(attributes.FairShareRatio)
            .Item("satisfaction_ratio").Value(dynamicAttributes.SatisfactionRatio)
            .Item("best_allocation_ratio").Value(attributes.BestAllocationRatio);
    }

    TYPath GetPoolPath(const TCompositeSchedulerElementPtr& element)
    {
        std::vector<Stroka> tokens;
        auto current = element;
        while (!current->IsRoot()) {
            if (current->IsExplicit()) {
                tokens.push_back(current->GetId());
            }
            current = current->GetParent();
        }

        std::reverse(tokens.begin(), tokens.end());

        TYPath path;
        for (const auto& token : tokens) {
            path.append('/');
            path.append(NYPath::ToYPathLiteral(token));
        }
        return path;
    }

    Stroka GetOperationPoolName(const TOperationPtr& operation)
    {
        auto spec = ParseSpec(operation, operation->GetSpec());
        return spec->Pool ? *spec->Pool : operation->GetAuthenticatedUser();
    }

    TCompositeSchedulerElementPtr GetDefaultParent()
    {
        auto defaultPool = FindPool(Config->DefaultParentPool);
        if (defaultPool) {
            return defaultPool;
        } else {
            return RootElement;
        }
    }

    TCompositeSchedulerElementPtr GetParentElement(const TOperationPtr& operation)
    {
        auto parentPool = FindPool(GetOperationPoolName(operation));
        return parentPool ? parentPool : GetDefaultParent();
    }

    void ValidateOperationCountLimit(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto parentElement = GetParentElement(operation);

        auto poolWithViolatedLimit = FindPoolWithViolatedOperationCountLimit(parentElement);
        if (poolWithViolatedLimit) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::TooManyOperations,
                "Limit for the number of concurrent operations %v for pool %Qv has been reached",
                poolWithViolatedLimit->GetMaxOperationCount(),
                poolWithViolatedLimit->GetId());
        }
    }

    void ValidateEphemeralPoolLimit(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto pool = FindPool(GetOperationPoolName(operation));
        if (pool) {
            return;
        }

        const auto& userName = operation->GetAuthenticatedUser();

        auto it = UserToEphemeralPools.find(userName);
        if (it == UserToEphemeralPools.end()) {
            return;
        }

        if (it->second.size() + 1 > Config->MaxEphemeralPoolsPerUser) {
            THROW_ERROR_EXCEPTION("Limit for number of ephemeral pools %v for user %v has been reached",
                Config->MaxEphemeralPoolsPerUser,
                userName);
        }
    }

    void DoValidateOperationStart(const TOperationPtr& operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ValidateOperationCountLimit(operation);
        ValidateEphemeralPoolLimit(operation);

        auto immediateParentPool = FindPool(GetOperationPoolName(operation));
        // NB: Check is not performed if operation is started in default or unknown pool.
        if (immediateParentPool && immediateParentPool->AreImmediateOperationsFobidden()) {
            THROW_ERROR_EXCEPTION(
                "Starting operations immediately in pool %Qv is forbidden",
                immediateParentPool->GetId());
        }

        auto parentElement = GetParentElement(operation);
        auto poolPath = GetPoolPath(parentElement);
        const auto& user = operation->GetAuthenticatedUser();

        Host->ValidatePoolPermission(poolPath, user, EPermission::Use);
    }

    void ProfileSchedulerElement(TCompositeSchedulerElementPtr element)
    {
        auto tag = element->GetProfilingTag();
        Profiler.Enqueue(
            "/pools/fair_share_ratio_x100000",
            static_cast<i64>(element->Attributes().FairShareRatio * 1e5),
            EMetricType::Gauge,
            {tag});
        Profiler.Enqueue(
            "/pools/usage_ratio_x100000",
            static_cast<i64>(element->GetResourceUsageRatio() * 1e5),
            EMetricType::Gauge,
            {tag});
        Profiler.Enqueue(
            "/pools/demand_ratio_x100000",
            static_cast<i64>(element->Attributes().DemandRatio * 1e5),
            EMetricType::Gauge,
            {tag});
        Profiler.Enqueue(
            "/pools/guaranteed_resource_ratio_x100000",
            static_cast<i64>(element->Attributes().GuaranteedResourcesRatio * 1e5),
            EMetricType::Gauge,
            {tag});

        ProfileResources(
            Profiler,
            element->GetResourceUsage(),
            "/pools/resource_usage",
            {tag});
        ProfileResources(
            Profiler,
            element->ResourceLimits(),
            "/pools/resource_limits",
            {tag});
        ProfileResources(
            Profiler,
            element->ResourceDemand(),
            "/pools/resource_demand",
            {tag});

        Profiler.Enqueue(
            "/running_operation_count",
            element->RunningOperationCount(),
            EMetricType::Gauge,
            {tag});
        Profiler.Enqueue(
            "/total_operation_count",
            element->OperationCount(),
            EMetricType::Gauge,
            {tag});
    }
};

ISchedulerStrategyPtr CreateFairShareStrategy(
    TFairShareStrategyConfigPtr config,
    ISchedulerStrategyHost* host)
{
    return New<TFairShareStrategy>(config, host);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

