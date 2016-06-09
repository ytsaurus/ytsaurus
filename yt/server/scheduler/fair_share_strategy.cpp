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

////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;
static const auto& Profiler = SchedulerProfiler;

////////////////////////////////////////////////////////////////////

NProfiling::TTagIdList GetFailReasonProfilingTags(EScheduleJobFailReason reason)
{
    static std::unordered_map<Stroka, NProfiling::TTagId> tagId;

    auto reasonAsString = ToString(reason);
    auto it = tagId.find(reasonAsString);
    if (it == tagId.end()) {
        it = tagId.emplace(
            reasonAsString,
            NProfiling::TProfileManager::Get()->RegisterTag("reason", reasonAsString)
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
        , ScheduleJobsThreadPool_(New<TThreadPool>(Config->StrategyScheduleJobsThreadCount, "ScheduleJob"))
    {
        Host->SubscribeOperationRegistered(BIND(&TFairShareStrategy::OnOperationRegistered, this));
        Host->SubscribeOperationUnregistered(BIND(&TFairShareStrategy::OnOperationUnregistered, this));

        Host->SubscribeJobFinished(BIND(&TFairShareStrategy::OnJobFinished, this));
        Host->SubscribeJobUpdated(BIND(&TFairShareStrategy::OnJobUpdated, this));
        Host->SubscribePoolsUpdated(BIND(&TFairShareStrategy::OnPoolsUpdated, this));

        Host->SubscribeOperationRuntimeParamsUpdated(
            BIND(&TFairShareStrategy::OnOperationRuntimeParamsUpdated, this));

        RootElement = New<TRootElement>(Host, config);

        FairShareUpdateExecutor_ = New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TFairShareStrategy::OnFairShareUpdate, this),
            Config->FairShareUpdatePeriod);

        FairShareLoggingExecutor_ = New<TPeriodicExecutor>(
            GetCurrentInvoker(),
            BIND(&TFairShareStrategy::OnFairShareLogging, this),
            Config->FairShareLogPeriod);
    }

    virtual void ScheduleJobs(const ISchedulingContextPtr& schedulingContext) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&ScheduleJobsLock))
            .Value();

        auto asyncResult = BIND(&TFairShareStrategy::DoScheduleJobs, this)
            .AsyncVia(ScheduleJobsThreadPool_->GetInvoker())
            .Run(schedulingContext, RootElementSnapshot);

        WaitFor(asyncResult)
            .ThrowOnError();
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

        LastPoolsNodeUpdate.Reset();
    }

    virtual TError CanAddOperation(TOperationPtr operation) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto spec = ParseSpec(operation, operation->GetSpec());
        auto poolName = spec->Pool ? *spec->Pool : operation->GetAuthenticatedUser();
        auto pool = FindPool(poolName);
        TCompositeSchedulerElement* poolElement;
        if (!pool) {
            auto defaultPool = FindPool(Config->DefaultParentPool);
            if (!defaultPool) {
                poolElement = RootElement.Get();
            } else {
                poolElement = defaultPool.Get();
            }
        } else {
            poolElement = pool.Get();
        }

        const auto& poolWithViolatedLimit = FindPoolWithViolatedOperationCountLimit(poolElement);
        if (poolWithViolatedLimit) {
            return TError(
                EErrorCode::TooManyOperations,
                "Limit for the number of concurrent operations %v for pool %Qv has been reached",
                poolWithViolatedLimit->GetMaxOperationCount(),
                poolWithViolatedLimit->GetId());
        }
        return TError();
    }

    virtual TStatistics GetOperationTimeStatistics(const TOperationId& operationId) override
    {
        auto element = GetOperationElement(operationId);
        return element->GetControllerTimeStatistics();
    }

    virtual void BuildOperationAttributes(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = GetOperationElement(operationId);
        auto serializedParams = ConvertToAttributes(element->GetRuntimeParams());
        BuildYsonMapFluently(consumer)
            .Items(*serializedParams);
    }

    virtual void BuildOperationProgress(const TOperationId& operationId, IYsonConsumer* consumer) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = FindOperationElement(operationId);
        if (!element) {
            return;
        }

        auto dynamicAttributes = GetGlobalDynamicAttributes(element);

        auto* parent = element->GetParent();
        BuildYsonMapFluently(consumer)
            .Item("pool").Value(parent->GetId())
            .Item("start_time").Value(dynamicAttributes.MinSubtreeStartTime)
            .Item("preemptable_job_count").Value(element->GetPreemptableJobCount())
            .Item("aggressively_preemptable_job_count").Value(element->GetAggressivelyPreemptableJobCount())
            .Do(BIND(&TFairShareStrategy::BuildElementYson, this, element));
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

        // Run periodic update.
        PROFILE_TIMING ("/fair_share_update_time") {
            // The root element gets the whole cluster.
            RootElement->Update(GlobalDynamicAttributes_);
        }

        // Update starvation flags for all operations.
        for (const auto& pair : OperationToElement) {
            pair.second->CheckForStarvation(now);
        }

        // Update starvation flags for all pools.
        if (Config->EnablePoolStarvation) {
            for (const auto& pair : Pools) {
                pair.second->CheckForStarvation(now);
            }
        }

        RootElementSnapshot = RootElement->CloneRoot();
    }

    // NB: This function is public for testing purposes.
    virtual void OnFairShareLoggingAt(TInstant now) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Log pools information.
        Host->LogEventFluently(ELogEventType::FairShareInfo, now)
            .Do(BIND(&TFairShareStrategy::BuildPoolsInformation, this))
            .Item("operations").DoMapFor(OperationToElement, [=] (TFluentMap fluent, const TOperationMap::value_type& pair) {
                const auto& operationId = pair.first;
                BuildYsonMapFluently(fluent)
                    .Item(ToString(operationId))
                    .BeginMap()
                        .Do(BIND(&TFairShareStrategy::BuildOperationProgress, this, operationId))
                    .EndMap();
            });

        for (auto& pair : OperationToElement) {
            const auto& operationId = pair.first;
            LOG_DEBUG("FairShareInfo: %v (OperationId: %v)",
                GetOperationLoggingProgress(operationId),
                operationId);
        }
    }

private:
    const TFairShareStrategyConfigPtr Config;
    ISchedulerStrategyHost* const Host;

    INodePtr LastPoolsNodeUpdate;
    typedef yhash_map<Stroka, TPoolPtr> TPoolMap;
    TPoolMap Pools;

    typedef yhash_map<TOperationId, TOperationElementPtr> TOperationMap;
    TOperationMap OperationToElement;

    std::list<TOperationPtr> OperationQueue;

    TRootElementPtr RootElement;
    TRootElementPtr RootElementSnapshot;

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
                ControllerScheduleJobFailCounter[reason] = NProfiling::TSimpleCounter(
                    prefix + "/controller_schedule_job_fail",
                    GetFailReasonProfilingTags(reason));
            }
        }

        NProfiling::TAggregateCounter PrescheduleJobTimeCounter;
        NProfiling::TAggregateCounter TotalControllerScheduleJobTimeCounter;
        NProfiling::TAggregateCounter ExecControllerScheduleJobTimeCounter;
        NProfiling::TAggregateCounter StrategyScheduleJobTimeCounter;
        NProfiling::TAggregateCounter ScheduleJobCallCounter;

        TEnumIndexedVector<NProfiling::TSimpleCounter, EScheduleJobFailReason> ControllerScheduleJobFailCounter;
    };

    TProfilingCounters NonPreemptiveProfilingCounters;
    TProfilingCounters PreemptiveProfilingCounters;

    TPeriodicExecutorPtr FairShareUpdateExecutor_;
    TPeriodicExecutorPtr FairShareLoggingExecutor_;

    TThreadPoolPtr ScheduleJobsThreadPool_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    TDynamicAttributes GetGlobalDynamicAttributes(const ISchedulerElementPtr& element) const
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

    void DoScheduleJobs(const ISchedulingContextPtr schedulingContext, const TRootElementPtr rootElement)
    {
        auto context = TFairShareContext(schedulingContext, rootElement->GetTreeSize());
        rootElement->BuildJobToOperationMapping(context);

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
                Profiler.Update(
                    counters.ControllerScheduleJobFailCounter[reason],
                    context.FailedScheduleJob[reason]);
            }
        };

        // First-chance scheduling.
        int nonPreemptiveScheduleJobCount = 0;
        {
            LOG_DEBUG("Scheduling new jobs");
            PROFILE_AGGREGATED_TIMING(NonPreemptiveProfilingCounters.PrescheduleJobTimeCounter) {
                rootElement->PrescheduleJob(context, /*starvingOnly*/ false);
            }

            NProfiling::TScopedTimer timer;
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
        }

        // Compute discount to node usage.
        LOG_DEBUG("Looking for preemptable jobs");
        // TODO(acid): Put raw pointers here.
        yhash_set<TCompositeSchedulerElementPtr> discountedPools;
        std::vector<TJobPtr> preemptableJobs;
        PROFILE_TIMING ("/analyze_preemptable_jobs_time") {
            for (const auto& job : schedulingContext->RunningJobs()) {
                const auto& operationElement = context.JobToOperationElement.at(job);
                if (!operationElement || !operationElement->IsJobExisting(job->GetId())) {
                    LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v)",
                        job->GetId(),
                        job->GetOperationId());
                    continue;
                }

                if (IsJobPreemptable(job, operationElement, context.HasAggressivelyStarvingNodes) && !operationElement->HasStarvingParent()) {
                    auto* parent = operationElement->GetParent();
                    while (parent) {
                        discountedPools.insert(parent);
                        context.DynamicAttributes(parent).ResourceUsageDiscount += job->ResourceUsage();
                        parent = parent->GetParent();
                    }
                    schedulingContext->ResourceUsageDiscount() += job->ResourceUsage();
                    preemptableJobs.push_back(job);
                    LOG_DEBUG("Job is preemptable (JobId: %v)",
                        job->GetId());
                }
            }
        }

        auto resourceDiscount = schedulingContext->ResourceUsageDiscount();
        int startedBeforePreemption = schedulingContext->StartedJobs().size();

        // Second-chance scheduling.
        // NB: Schedule at most one job.
        int preemptiveScheduleJobCount = 0;
        {
            LOG_DEBUG("Scheduling new jobs with preemption");
            PROFILE_AGGREGATED_TIMING(PreemptiveProfilingCounters.PrescheduleJobTimeCounter) {
                rootElement->PrescheduleJob(context, /*starvingOnly*/ true);
            }

            // Clean data from previous profiling.
            context.TotalScheduleJobDuration = TDuration::Zero();
            context.ExecScheduleJobDuration = TDuration::Zero();
            std::fill(context.FailedScheduleJob.begin(), context.FailedScheduleJob.end(), 0);

            NProfiling::TScopedTimer timer;
            while (schedulingContext->CanStartMoreJobs()) {
                ++preemptiveScheduleJobCount;
                if (!rootElement->ScheduleJob(context)) {
                    break;
                }
                if (schedulingContext->StartedJobs().size() > startedBeforePreemption) {
                    break;
                }
            }
            profileTimings(
                PreemptiveProfilingCounters,
                preemptiveScheduleJobCount,
                timer.GetElapsed() - context.TotalScheduleJobDuration);
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

        auto poolLimitsViolated = [&] (const TJobPtr& job) -> bool {
            const auto& operationElement = context.JobToOperationElement.at(job);
            if (!operationElement) {
                return false;
            }

            auto* parent = operationElement->GetParent();
            while (parent) {
                if (!Dominates(parent->ResourceLimits(), parent->GetResourceUsage())) {
                    return true;
                }
                parent = parent->GetParent();
            }
            return false;
        };

        auto anyPoolLimitsViolated = [&] () -> bool {
            for (const auto& job : schedulingContext->StartedJobs()) {
                if (poolLimitsViolated(job)) {
                    return true;
                }
            }
            return false;
        };

        bool nodeLimitsViolated = true;
        bool poolsLimitsViolated = true;

        for (const auto& job : preemptableJobs) {
            const auto& operationElement = context.JobToOperationElement.at(job);
            if (!operationElement || !operationElement->IsJobExisting(job->GetId())) {
                LOG_INFO("Dangling preemptable job found (JobId: %v, OperationId: %v)",
                    job->GetId(),
                    job->GetOperationId());
                continue;
            }

            // Update flags only if violation is not resolved yet to avoid costly computations.
            if (nodeLimitsViolated) {
                nodeLimitsViolated = !Dominates(schedulingContext->ResourceLimits(), schedulingContext->ResourceUsage());
            }
            if (!nodeLimitsViolated && poolsLimitsViolated) {
                poolsLimitsViolated = anyPoolLimitsViolated();
            }

            if (!nodeLimitsViolated && !poolsLimitsViolated) {
                break;
            }

            if (nodeLimitsViolated || (poolsLimitsViolated && poolLimitsViolated(job))) {
                PreemptJob(job, operationElement, context);
            }
        }

        LOG_DEBUG("Heartbeat info (StartedJobs: %v, PreemptedJobs: %v, "
            "JobsScheduledDuringPreemption: %v, PreemptableJobs: %v, PreemptableResources: %v, "
            "NonPreemptiveScheduleJobCount: %v, PreemptiveScheduleJobCount: %v, HasAggressivelyStarvingNodes: %v)",
            schedulingContext->StartedJobs().size(),
            schedulingContext->PreemptedJobs().size(),
            scheduledDuringPreemption,
            preemptableJobs.size(),
            FormatResources(resourceDiscount),
            nonPreemptiveScheduleJobCount,
            preemptiveScheduleJobCount,
            context.HasAggressivelyStarvingNodes);
    }

    bool IsJobPreemptable(
        const TJobPtr& job,
        const TOperationElementPtr& element,
        bool aggressivePreemptionEnabled) const
    {
        double usageRatio = element->GetResourceUsageRatio();
        if (usageRatio < Config->MinPreemptableRatio) {
            return false;
        }

        const auto& attributes = element->Attributes();
        auto threshold = aggressivePreemptionEnabled
            ? Config->AggressivePreemptionSatisfactionThreshold
            : Config->PreemptionSatisfactionThreshold;
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
            LOG_ERROR(ex, "Error parsing spec of pooled operation %v, defaults will be used",
                operation->GetId());
            return New<TStrategyOperationSpec>();
        }
    }

    TOperationRuntimeParamsPtr BuildInitialRuntimeParams(TStrategyOperationSpecPtr spec)
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

    void OnOperationRegistered(TOperationPtr operation)
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
        YCHECK(OperationToElement.insert(std::make_pair(operation->GetId(), operationElement)).second);

        auto poolName = spec->Pool ? *spec->Pool : operation->GetAuthenticatedUser();
        auto pool = FindPool(poolName);
        if (!pool) {
            pool = New<TPool>(Host, poolName, Config);
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

    TCompositeSchedulerElementPtr FindPoolWithViolatedOperationCountLimit(TCompositeSchedulerElement* element)
    {
        while (element) {
            if (element->OperationCount() >= element->GetMaxOperationCount()) {
                return element;
            }
            element = element->GetParent();
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

    void OnOperationUnregistered(TOperationPtr operation)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto operationElement = GetOperationElement(operation->GetId());
        auto* pool = static_cast<TPool*>(operationElement->GetParent());

        YCHECK(OperationToElement.erase(operation->GetId()) == 1);
        operationElement->SetAlive(false);
        pool->RemoveChild(operationElement);
        pool->IncreaseResourceUsage(-operationElement->GetResourceUsage());
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

    void OnOperationRuntimeParamsUpdated(
        TOperationPtr operation,
        INodePtr update)
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


    void OnJobFinished(const TJobPtr& job)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = GetOperationElement(job->GetOperationId());
        element->OnJobFinished(job->GetId());
    }

    void OnJobUpdated(const TJobPtr& job, const TJobResources& resourcesDelta)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& element = GetOperationElement(job->GetOperationId());
        element->IncreaseJobResourceUsage(job->GetId(), resourcesDelta);
    }

    void RegisterPool(const TPoolPtr& pool)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

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

        YCHECK(Pools.erase(pool->GetId()) == 1);
        pool->SetAlive(false);
        auto parent = pool->GetParent();
        SetPoolParent(pool, nullptr);

        LOG_INFO("Pool unregistered (Pool: %v, Parent: %v)",
            pool->GetId(),
            parent->GetId());
    }

    void SetPoolParent(const TPoolPtr& pool, const TCompositeSchedulerElementPtr& parent)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (pool->GetParent() == parent)
            return;

        auto* oldParent = pool->GetParent();
        if (oldParent) {
            oldParent->IncreaseResourceUsage(-pool->GetResourceUsage());
            IncreaseRunningOperationCount(oldParent, -pool->RunningOperationCount());
            oldParent->RemoveChild(pool);
        }

        pool->SetParent(parent.Get());
        if (parent) {
            parent->AddChild(pool);
            parent->IncreaseResourceUsage(pool->GetResourceUsage());
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
            // NB: root element is not a pool, so we should supress warning in this special case.
            if (Config->DefaultParentPool != RootPoolName) {
                LOG_WARNING("Default parent pool %Qv is not registered", Config->DefaultParentPool);
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


    TOperationElementPtr FindOperationElement(const TOperationId& operationId)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto it = OperationToElement.find(operationId);
        return it == OperationToElement.end() ? nullptr : it->second;
    }

    TOperationElementPtr GetOperationElement(const TOperationId& operationId)
    {
        auto element = FindOperationElement(operationId);
        YCHECK(element);
        return element;
    }

    void OnPoolsUpdated(INodePtr poolsNode)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (LastPoolsNodeUpdate && AreNodesEqual(LastPoolsNodeUpdate, poolsNode)) {
            LOG_INFO("Pools are not changed, skipping update");
            return;
        }
        LastPoolsNodeUpdate = poolsNode;

        auto guard = WaitFor(TAsyncLockWriterGuard::Acquire(&ScheduleJobsLock)).Value();

        try {
            // Build the set of potential orphans.
            yhash_set<Stroka> orphanPoolIds;
            for (const auto& pair : Pools) {
                YCHECK(orphanPoolIds.insert(pair.first).second);
            }

            // Track ids appearing in various branches of the tree.
            yhash_map<Stroka, TYPath> poolIdToPath;

            // NB: std::function is needed by parseConfig to capture itself.
            std::function<void(INodePtr, TCompositeSchedulerElementPtr)> parseConfig =
                [&] (INodePtr configNode, TCompositeSchedulerElementPtr parent) {
                    auto configMap = configNode->AsMap();
                    for (const auto& pair : configMap->GetChildren()) {
                        const auto& childId = pair.first;
                        const auto& childNode = pair.second;
                        auto childPath = childNode->GetPath();
                        if (!poolIdToPath.insert(std::make_pair(childId, childPath)).second) {
                            LOG_ERROR("Pool %Qv is defined both at %v and %v; skipping second occurrence",
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
                            LOG_ERROR(ex, "Error parsing configuration of pool %Qv; using defaults",
                                childPath);
                            config = New<TPoolConfig>();
                        }

                        auto pool = FindPool(childId);
                        if (pool) {
                            // Reconfigure existing pool.
                            pool->SetConfig(config);
                            YCHECK(orphanPoolIds.erase(childId) == 1);
                        } else {
                            // Create new pool.
                            pool = New<TPool>(Host, childId, Config);
                            pool->SetConfig(config);
                            RegisterPool(pool, parent);
                        }
                        SetPoolParent(pool, parent);

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
            RootElementSnapshot = RootElement->CloneRoot();

            LOG_INFO("Pools updated");
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error updating pools");
        }
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
                        .DoIf(config->Mode == ESchedulingMode::Fifo, [&] (TFluentMap fluent) {
                            fluent
                                .Item("fifo_sort_parameters").Value(config->FifoSortParameters);
                        })
                        .DoIf(pool->GetParent(), [&] (TFluentMap fluent) {
                            fluent
                                .Item("parent").Value(pool->GetParent()->GetId());
                        })
                        .Do(BIND(&TFairShareStrategy::BuildElementYson, this, pool))
                    .EndMap();
            });
    }

    void BuildElementYson(const ISchedulerElementPtr& element, IYsonConsumer* consumer)
    {
        const auto& attributes = element->Attributes();
        auto dynamicAttributes = GetGlobalDynamicAttributes(element);

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
            .Item("max_possible_usage_ratio").Value(attributes.MaxPossibleUsageRatio)
            .Item("usage_ratio").Value(element->GetResourceUsageRatio())
            .Item("demand_ratio").Value(attributes.DemandRatio)
            .Item("fair_share_ratio").Value(attributes.FairShareRatio)
            .Item("satisfaction_ratio").Value(dynamicAttributes.SatisfactionRatio)
            .Item("best_allocation_ratio").Value(attributes.BestAllocationRatio);
    }

};

std::unique_ptr<ISchedulerStrategy> CreateFairShareStrategy(
    TFairShareStrategyConfigPtr config,
    ISchedulerStrategyHost* host)
{
    return std::unique_ptr<ISchedulerStrategy>(new TFairShareStrategy(config, host));
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

