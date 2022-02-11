#include "fair_share_tree_job_scheduler.h"
#include "fair_share_tree_profiling.h"
#include "fair_share_tree_snapshot.h"
#include "scheduling_context.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeJobScheduler::TFairShareTreeJobScheduler(NLogging::TLogger logger, TFairShareTreeProfileManagerPtr treeProfiler)
    : Logger(std::move(logger))
    , TreeProfiler_(std::move(treeProfiler))
    , CumulativeScheduleJobsTime_(TreeProfiler_->GetProfiler().TimeCounter("/cumulative_schedule_jobs_time"))
    , ScheduleJobsDeadlineReachedCounter_(TreeProfiler_->GetProfiler().Counter("/schedule_jobs_deadline_reached"))
{
    InitSchedulingStages();
}

void TFairShareTreeJobScheduler::ScheduleJobs(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    VERIFY_THREAD_AFFINITY_ANY();

    NProfiling::TWallTimer scheduleJobsTimer;

    bool enableSchedulingInfoLogging = false;
    auto now = schedulingContext->GetNow();
    const auto& config = treeSnapshot->TreeConfig();
    if (LastSchedulingInformationLoggedTime_ + DurationToCpuDuration(config->HeartbeatTreeSchedulingInfoLogBackoff) < now) {
        enableSchedulingInfoLogging = true;
        LastSchedulingInformationLoggedTime_ = now;
    }

    // TODO(eshcherbin): Use TAtomicObject.
    std::vector<TSchedulingTagFilter> registeredSchedulingTagFilters;
    {
        auto guard = ReaderGuard(RegisteredSchedulingTagFiltersLock_);
        registeredSchedulingTagFilters = RegisteredSchedulingTagFilters_;
    }

    TScheduleJobsContext context(
        schedulingContext,
        std::move(registeredSchedulingTagFilters),
        enableSchedulingInfoLogging,
        Logger);

    context.SchedulingStatistics().ResourceUsage = schedulingContext->ResourceUsage();
    context.SchedulingStatistics().ResourceLimits = schedulingContext->ResourceLimits();

    if (config->EnableResourceUsageSnapshot) {
        if (auto snapshot = treeSnapshot->GetDynamicAttributesListSnapshot()) {
            YT_LOG_DEBUG_IF(enableSchedulingInfoLogging, "Using dynamic attributes snapshot for job scheduling");
            context.DynamicAttributesListSnapshot() = std::move(snapshot);
        }
    }

    bool needPackingFallback;
    {
        context.StartStage(&SchedulingStages_[EJobSchedulingStage::NonPreemptive]);
        ScheduleJobsWithoutPreemption(treeSnapshot, &context, now);
        needPackingFallback = schedulingContext->StartedJobs().empty() && !context.BadPackingOperations().empty();
        ReactivateBadPackingOperations(&context);
        context.SchedulingStatistics().MaxNonPreemptiveSchedulingIndex = context.StageState()->MaxSchedulingIndex;
        context.FinishStage();
    }

    auto nodeId = schedulingContext->GetNodeDescriptor().Id;

    bool scheduleJobsWithPreemption = false;
    {
        bool nodeIsMissing = false;
        {
            auto guard = ReaderGuard(NodeIdToLastPreemptiveSchedulingTimeLock_);
            auto it = NodeIdToLastPreemptiveSchedulingTime_.find(nodeId);
            if (it == NodeIdToLastPreemptiveSchedulingTime_.end()) {
                nodeIsMissing = true;
                scheduleJobsWithPreemption = true;
            } else if (it->second + DurationToCpuDuration(config->PreemptiveSchedulingBackoff) <= now) {
                scheduleJobsWithPreemption = true;
                it->second = now;
            }
        }
        if (nodeIsMissing) {
            auto guard = WriterGuard(NodeIdToLastPreemptiveSchedulingTimeLock_);
            NodeIdToLastPreemptiveSchedulingTime_[nodeId] = now;
        }
    }

    context.SchedulingStatistics().ScheduleWithPreemption = scheduleJobsWithPreemption;
    if (scheduleJobsWithPreemption) {
        auto ssdPriorityPreemptionConfig = treeSnapshot->TreeConfig()->SsdPriorityPreemption;
        bool shouldAttemptSsdPriorityPreemption = ssdPriorityPreemptionConfig->Enable &&
            schedulingContext->CanSchedule(ssdPriorityPreemptionConfig->NodeTagFilter);
        context.SetSsdPriorityPreemptionEnabled(shouldAttemptSsdPriorityPreemption);
        context.SsdPriorityPreemptionMedia() = treeSnapshot->SsdPriorityPreemptionMedia();
        context.SchedulingStatistics().SsdPriorityPreemptionEnabled = context.GetSsdPriorityPreemptionEnabled();
        context.SchedulingStatistics().SsdPriorityPreemptionMedia = context.SsdPriorityPreemptionMedia();

        context.CountOperationsByPreemptionPriority(treeSnapshot->RootElement());

        for (const auto& preemptiveStage : BuildPreemptiveSchedulingStageList(&context)) {
            // We allow to schedule at most one job using preemption.
            if (context.SchedulingStatistics().ScheduledDuringPreemption > 0) {
                break;
            }

            context.StartStage(preemptiveStage.Stage);
            ScheduleJobsWithPreemption(
                treeSnapshot,
                &context,
                now,
                preemptiveStage.TargetOperationPreemptionPriority,
                preemptiveStage.MinJobPreemptionLevel,
                preemptiveStage.ForcePreemptionAttempt);
            context.FinishStage();
        }
    } else {
        YT_LOG_DEBUG("Skip preemptive scheduling");
    }

    if (needPackingFallback) {
        context.StartStage(&SchedulingStages_[EJobSchedulingStage::PackingFallback]);
        ScheduleJobsPackingFallback(treeSnapshot, &context, now);
        context.FinishStage();
    }

    // Interrupt some jobs if usage is greater that limit.
    if (schedulingContext->ShouldAbortJobsSinceResourcesOvercommit()) {
        AbortJobsSinceResourcesOvercommit(schedulingContext, treeSnapshot);
    }

    schedulingContext->SetSchedulingStatistics(context.SchedulingStatistics());

    CumulativeScheduleJobsTime_.Add(scheduleJobsTimer.GetElapsedTime());
}

int TFairShareTreeJobScheduler::RegisterSchedulingTagFilter(const TSchedulingTagFilter& filter)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (filter.IsEmpty()) {
        return EmptySchedulingTagFilterIndex;
    }
    auto it = SchedulingTagFilterToIndexAndCount_.find(filter);
    if (it == SchedulingTagFilterToIndexAndCount_.end()) {
        int index;
        if (FreeSchedulingTagFilterIndexes_.empty()) {
            auto guard = WriterGuard(RegisteredSchedulingTagFiltersLock_);

            index = RegisteredSchedulingTagFilters_.size();
            RegisteredSchedulingTagFilters_.push_back(filter);
        } else {
            index = FreeSchedulingTagFilterIndexes_.back();
            FreeSchedulingTagFilterIndexes_.pop_back();

            {
                auto guard = WriterGuard(RegisteredSchedulingTagFiltersLock_);
                RegisteredSchedulingTagFilters_[index] = filter;
            }
        }
        SchedulingTagFilterToIndexAndCount_.emplace(filter, TSchedulingTagFilterEntry({index, 1}));
        return index;
    } else {
        ++it->second.Count;
        return it->second.Index;
    }
}

void TFairShareTreeJobScheduler::UnregisterSchedulingTagFilter(int index)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (index == EmptySchedulingTagFilterIndex) {
        return;
    }

    TSchedulingTagFilter filter;
    {
        auto guard = ReaderGuard(RegisteredSchedulingTagFiltersLock_);
        filter = RegisteredSchedulingTagFilters_[index];
    }

    UnregisterSchedulingTagFilter(filter);
}

void TFairShareTreeJobScheduler::UnregisterSchedulingTagFilter(const TSchedulingTagFilter& filter)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (filter.IsEmpty()) {
        return;
    }
    auto it = SchedulingTagFilterToIndexAndCount_.find(filter);
    YT_VERIFY(it != SchedulingTagFilterToIndexAndCount_.end());
    --it->second.Count;
    if (it->second.Count == 0) {
        {
            auto guard = WriterGuard(RegisteredSchedulingTagFiltersLock_);
            RegisteredSchedulingTagFilters_[it->second.Index] = EmptySchedulingTagFilter;
        }

        FreeSchedulingTagFilterIndexes_.push_back(it->second.Index);
        SchedulingTagFilterToIndexAndCount_.erase(it);
    }
}

void TFairShareTreeJobScheduler::InitSchedulingStages()
{
    for (auto stage : TEnumTraits<EJobSchedulingStage>::GetDomainValues()) {
        SchedulingStages_[stage] = TScheduleJobsStage{
            .Type = stage,
            .ProfilingCounters = TScheduleJobsProfilingCounters(
                TreeProfiler_->GetProfiler().WithTag("scheduling_stage", FormatEnum(stage))),
        };
    }
}

TPreemptiveScheduleJobsStageList TFairShareTreeJobScheduler::BuildPreemptiveSchedulingStageList(TScheduleJobsContext* context)
{
    TPreemptiveScheduleJobsStageList preemptiveStages;

    if (context->GetSsdPriorityPreemptionEnabled()) {
        preemptiveStages.push_back(TPreemptiveScheduleJobsStage{
            .Stage = &SchedulingStages_[EJobSchedulingStage::SsdAggressivelyPreemptive],
            .TargetOperationPreemptionPriority = EOperationPreemptionPriority::SsdAggressive,
            .MinJobPreemptionLevel = EJobPreemptionLevel::SsdAggressivelyPreemptable,
        });
        preemptiveStages.push_back(TPreemptiveScheduleJobsStage{
            .Stage = &SchedulingStages_[EJobSchedulingStage::SsdPreemptive],
            .TargetOperationPreemptionPriority = EOperationPreemptionPriority::SsdRegular,
            .MinJobPreemptionLevel = EJobPreemptionLevel::NonPreemptable,
        });
    }

    preemptiveStages.push_back(TPreemptiveScheduleJobsStage{
        .Stage = &SchedulingStages_[EJobSchedulingStage::AggressivelyPreemptive],
        .TargetOperationPreemptionPriority = EOperationPreemptionPriority::Aggressive,
        .MinJobPreemptionLevel = EJobPreemptionLevel::AggressivelyPreemptable,
    });
    preemptiveStages.push_back(TPreemptiveScheduleJobsStage{
        .Stage = &SchedulingStages_[EJobSchedulingStage::Preemptive],
        .TargetOperationPreemptionPriority = EOperationPreemptionPriority::Regular,
        .MinJobPreemptionLevel = EJobPreemptionLevel::Preemptable,
        .ForcePreemptionAttempt = true,
    });

    return preemptiveStages;
}

void TFairShareTreeJobScheduler::ScheduleJobsWithoutPreemption(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    TScheduleJobsContext* context,
    TCpuInstant startTime)
{
    YT_LOG_TRACE("Scheduling new jobs");

    DoScheduleJobsWithoutPreemption(
        treeSnapshot,
        context,
        startTime,
        /* ignorePacking */ false,
        /* oneJobOnly */ false);
}

void TFairShareTreeJobScheduler::ScheduleJobsPackingFallback(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    TScheduleJobsContext* context,
    TCpuInstant startTime)
{
    YT_LOG_TRACE("Scheduling jobs with packing ignored");

    // Schedule at most one job with packing ignored in case all operations have rejected the heartbeat.
    DoScheduleJobsWithoutPreemption(
        treeSnapshot,
        context,
        startTime,
        /* ignorePacking */ true,
        /* oneJobOnly */ true);
}

void TFairShareTreeJobScheduler::DoScheduleJobsWithoutPreemption(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    TScheduleJobsContext* context,
    TCpuInstant startTime,
    bool ignorePacking,
    bool oneJobOnly)
{
    const auto& rootElement = treeSnapshot->RootElement();
    const auto& controllerConfig = treeSnapshot->ControllerConfig();

    {
        TCpuInstant schedulingDeadline = startTime + DurationToCpuDuration(controllerConfig->ScheduleJobsTimeout);

        while (context->SchedulingContext()->CanStartMoreJobs() && context->SchedulingContext()->GetNow() < schedulingDeadline)
        {
            if (!context->StageState()->PrescheduleExecuted) {

                context->PrepareForScheduling(rootElement);
                context->PrescheduleJob(rootElement);
            }
            ++context->StageState()->ScheduleJobAttemptCount;
            auto scheduleJobResult = rootElement->ScheduleJob(context, ignorePacking);
            if (scheduleJobResult.Scheduled) {
                ReactivateBadPackingOperations(context);
            }
            if (scheduleJobResult.Finished || (oneJobOnly && scheduleJobResult.Scheduled)) {
                break;
            }
        }

        if (context->SchedulingContext()->GetNow() >= schedulingDeadline) {
            ScheduleJobsDeadlineReachedCounter_.Increment();
        }
    }
}

void TFairShareTreeJobScheduler::ReactivateBadPackingOperations(TScheduleJobsContext* context)
{
    for (const auto& operation : context->BadPackingOperations()) {
        // TODO(antonkikh): multiple activations can be implemented more efficiently.
        operation->ActivateOperation(context);
    }
    context->BadPackingOperations().clear();
}

// TODO(eshcherbin): Maybe receive a set of preemptable job levels instead of max level.
void TFairShareTreeJobScheduler::ScheduleJobsWithPreemption(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    TScheduleJobsContext* context,
    TCpuInstant startTime,
    EOperationPreemptionPriority targetOperationPreemptionPriority,
    EJobPreemptionLevel minJobPreemptionLevel,
    bool forcePreemptionAttempt)
{
    YT_VERIFY(targetOperationPreemptionPriority != EOperationPreemptionPriority::None);

    // NB(eshcherbin): We might want to analyze jobs and attempt preemption even if there are no candidate operations of target priority.
    // For example, we preempt jobs in pools or operations which exceed their specified resource limits.
    bool shouldAttemptScheduling = context->OperationCountByPreemptionPriority()[targetOperationPreemptionPriority] > 0;
    bool shouldAttemptPreemption = forcePreemptionAttempt || shouldAttemptScheduling;
    if (!shouldAttemptPreemption) {
        return;
    }

    // NB: This method achieves 2 goals relevant for scheduling with preemption:
    // 1. Reset |Active| attribute after scheduling without preemption (this is necessary for PrescheduleJob correctness).
    // 2. Initialize dynamic attributes and calculate local resource usages if scheduling without preemption was skipped.
    context->PrepareForScheduling(treeSnapshot->RootElement());

    std::vector<TJobWithPreemptionInfo> unconditionallyPreemptableJobs;
    TNonOwningJobSet forcefullyPreemptableJobs;
    AnalyzePreemptableJobs(
        treeSnapshot,
        context,
        targetOperationPreemptionPriority,
        minJobPreemptionLevel,
        &unconditionallyPreemptableJobs,
        &forcefullyPreemptableJobs);

    int startedBeforePreemption = context->SchedulingContext()->StartedJobs().size();

    // NB: Schedule at most one job with preemption.
    TJobPtr jobStartedUsingPreemption;
    if (shouldAttemptScheduling) {
        YT_LOG_TRACE(
            "Scheduling new jobs with preemption "
            "(UnconditionallyPreemptableJobs: %v, UnconditionalResourceUsageDiscount: %v, TargetOperationPreemptionPriority: %v)",
            unconditionallyPreemptableJobs,
            FormatResources(context->SchedulingContext()->UnconditionalResourceUsageDiscount()),
            targetOperationPreemptionPriority);

        auto& rootElement = treeSnapshot->RootElement();
        const auto& controllerConfig = treeSnapshot->ControllerConfig();
        TCpuInstant schedulingDeadline = startTime + DurationToCpuDuration(controllerConfig->ScheduleJobsTimeout);

        while (context->SchedulingContext()->CanStartMoreJobs() && context->SchedulingContext()->GetNow() < schedulingDeadline)
        {
            if (!context->StageState()->PrescheduleExecuted) {
                context->PrescheduleJob(rootElement, targetOperationPreemptionPriority);
            }

            ++context->StageState()->ScheduleJobAttemptCount;
            auto scheduleJobResult = rootElement->ScheduleJob(context, /* ignorePacking */ true);
            if (scheduleJobResult.Scheduled) {
                jobStartedUsingPreemption = context->SchedulingContext()->StartedJobs().back();
                break;
            }
            if (scheduleJobResult.Finished) {
                break;
            }
        }

        if (context->SchedulingContext()->GetNow() >= schedulingDeadline) {
            ScheduleJobsDeadlineReachedCounter_.Increment();
        }
    }

    int startedAfterPreemption = context->SchedulingContext()->StartedJobs().size();
    context->SchedulingStatistics().ScheduledDuringPreemption = startedAfterPreemption - startedBeforePreemption;

    PreemptJobsAfterScheduling(
        treeSnapshot,
        context,
        targetOperationPreemptionPriority,
        std::move(unconditionallyPreemptableJobs),
        forcefullyPreemptableJobs,
        jobStartedUsingPreemption);
}

void TFairShareTreeJobScheduler::AnalyzePreemptableJobs(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    TScheduleJobsContext* context,
    EOperationPreemptionPriority targetOperationPreemptionPriority,
    EJobPreemptionLevel minJobPreemptionLevel,
    std::vector<TJobWithPreemptionInfo>* unconditionallyPreemptableJobs,
    TNonOwningJobSet* forcefullyPreemptableJobs)
{
    const auto& treeConfig = treeSnapshot->TreeConfig();

    YT_LOG_TRACE("Looking for preemptable jobs (MinJobPreemptionLevel: %v)", minJobPreemptionLevel);

    int totalConditionallyPreemptableJobCount = 0;
    int maxConditionallyPreemptableJobCountInPool = 0;

    NProfiling::TWallTimer timer;

    const auto& nodeModule = TNodeSchedulingSegmentManager::GetNodeModule(
        context->SchedulingContext()->GetNodeDescriptor(),
        treeConfig->SchedulingSegments->ModuleType);
    auto jobInfos = CollectJobsWithPreemptionInfo(context->SchedulingContext(), treeSnapshot);
    for (const auto& jobInfo : jobInfos) {
        const auto& [job, _, operationElement] = jobInfo;

        bool isJobForcefullyPreemptable = !operationElement->IsSchedulingSegmentCompatibleWithNode(
            context->SchedulingContext()->GetSchedulingSegment(),
            nodeModule);
        if (isJobForcefullyPreemptable) {
            YT_ELEMENT_LOG_DETAILED(operationElement,
                "Job is forcefully preemptable because it is running on a node in a different scheduling segment or module "
                "(JobId: %v, OperationId: %v, OperationSegment: %v, NodeSegment: %v, Address: %v, Module: %v)",
                job->GetId(),
                operationElement->GetId(),
                operationElement->SchedulingSegment(),
                context->SchedulingContext()->GetSchedulingSegment(),
                context->SchedulingContext()->GetNodeDescriptor().Address,
                context->SchedulingContext()->GetNodeDescriptor().DataCenter);

            forcefullyPreemptableJobs->insert(job.Get());
        }

        bool isJobPreemptable = isJobForcefullyPreemptable || (context->GetJobPreemptionLevel(jobInfo) >= minJobPreemptionLevel);
        if (!isJobPreemptable) {
            continue;
        }

        auto preemptionBlockingAncestor = operationElement->FindPreemptionBlockingAncestor(
            targetOperationPreemptionPriority,
            context->DynamicAttributesList(),
            treeConfig);
        bool isUnconditionalPreemptionAllowed = isJobForcefullyPreemptable ||
            preemptionBlockingAncestor == nullptr;
        bool isConditionalPreemptionAllowed = treeSnapshot->TreeConfig()->EnableConditionalPreemption &&
            !isUnconditionalPreemptionAllowed &&
            preemptionBlockingAncestor != operationElement;

        if (isUnconditionalPreemptionAllowed) {
            const auto* parent = operationElement->GetParent();
            while (parent) {
                context->LocalUnconditionalUsageDiscountMap()[parent->GetTreeIndex()] += job->ResourceUsage();
                parent = parent->GetParent();
            }
            context->SchedulingContext()->UnconditionalResourceUsageDiscount() += job->ResourceUsage();
            unconditionallyPreemptableJobs->push_back(jobInfo);
        } else if (isConditionalPreemptionAllowed) {
            context->ConditionallyPreemptableJobSetMap()[preemptionBlockingAncestor->GetTreeIndex()].insert(jobInfo);
            ++totalConditionallyPreemptableJobCount;
        }
    }

    context->PrepareConditionalUsageDiscounts(treeSnapshot->RootElement(), targetOperationPreemptionPriority);
    for (const auto& [_, jobSet] : context->ConditionallyPreemptableJobSetMap()) {
        maxConditionallyPreemptableJobCountInPool = std::max(
            maxConditionallyPreemptableJobCountInPool,
            static_cast<int>(jobSet.size()));
    }

    context->StageState()->AnalyzeJobsDuration += timer.GetElapsedTime();

    context->SchedulingStatistics().UnconditionallyPreemptableJobCount = unconditionallyPreemptableJobs->size();
    context->SchedulingStatistics().UnconditionalResourceUsageDiscount = context->SchedulingContext()->UnconditionalResourceUsageDiscount();
    context->SchedulingStatistics().MaxConditionalResourceUsageDiscount = context->SchedulingContext()->GetMaxConditionalUsageDiscount();
    context->SchedulingStatistics().TotalConditionallyPreemptableJobCount = totalConditionallyPreemptableJobCount;
    context->SchedulingStatistics().MaxConditionallyPreemptableJobCountInPool = maxConditionallyPreemptableJobCountInPool;
}

void TFairShareTreeJobScheduler::PreemptJobsAfterScheduling(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    TScheduleJobsContext* context,
    EOperationPreemptionPriority targetOperationPreemptionPriority,
    std::vector<TJobWithPreemptionInfo> preemptableJobs,
    const TNonOwningJobSet& forcefullyPreemptableJobs,
    const TJobPtr& jobStartedUsingPreemption)
{
    // Collect conditionally preemptable jobs.
    TJobWithPreemptionInfoSet conditionallyPreemptableJobs;
    if (jobStartedUsingPreemption) {
        auto* operationElement = treeSnapshot->FindEnabledOperationElement(jobStartedUsingPreemption->GetOperationId());
        YT_VERIFY(operationElement);

        auto* parent = operationElement->GetParent();
        while (parent) {
            const auto& parentConditionallyPreemptableJobs = context->GetConditionallyPreemptableJobsInPool(parent);
            conditionallyPreemptableJobs.insert(
                parentConditionallyPreemptableJobs.begin(),
                parentConditionallyPreemptableJobs.end());

            parent = parent->GetParent();
        }
    }

    preemptableJobs.insert(preemptableJobs.end(), conditionallyPreemptableJobs.begin(), conditionallyPreemptableJobs.end());
    SortJobsWithPreemptionInfo(&preemptableJobs);
    std::reverse(preemptableJobs.begin(), preemptableJobs.end());

    // Reset discounts.
    context->SchedulingContext()->ResetUsageDiscounts();
    context->LocalUnconditionalUsageDiscountMap().clear();
    context->ConditionallyPreemptableJobSetMap().clear();

    auto findPoolWithViolatedLimitsForJob = [&] (const TJobPtr& job) -> const TSchedulerCompositeElement* {
        auto* operationElement = treeSnapshot->FindEnabledOperationElement(job->GetOperationId());
        if (!operationElement) {
            return nullptr;
        }

        auto* parent = operationElement->GetParent();
        while (parent) {
            if (parent->AreResourceLimitsViolated()) {
                return parent;
            }
            parent = parent->GetParent();
        }
        return nullptr;
    };

    // TODO(eshcherbin): Use a separate tag for specifying preemptive scheduling stage.
    // Bloating |EJobPreemptionReason| is unwise.
    auto preemptionReason = [&] {
        switch (targetOperationPreemptionPriority) {
            case EOperationPreemptionPriority::Regular:
                return EJobPreemptionReason::Preemption;
            case EOperationPreemptionPriority::SsdRegular:
                return EJobPreemptionReason::SsdPreemption;
            case EOperationPreemptionPriority::Aggressive:
                return EJobPreemptionReason::AggressivePreemption;
            case EOperationPreemptionPriority::SsdAggressive:
                return EJobPreemptionReason::SsdAggressivePreemption;
            default:
                YT_ABORT();
        }
    }();

    int currentJobIndex = 0;
    for (; currentJobIndex < std::ssize(preemptableJobs); ++currentJobIndex) {
        if (Dominates(context->SchedulingContext()->ResourceLimits(), context->SchedulingContext()->ResourceUsage())) {
            break;
        }

        const auto& jobInfo = preemptableJobs[currentJobIndex];
        const auto& [job, _, operationElement] = jobInfo;

        if (jobStartedUsingPreemption) {
            // TODO(eshcherbin): Rethink preemption reason format to allow more variable attributes easily.
            job->SetPreemptionReason(Format(
                "Preempted to start job %v of operation %v during preemptive stage with priority %Qlv, "
                "job was %v and %v preemptable",
                jobStartedUsingPreemption->GetId(),
                jobStartedUsingPreemption->GetOperationId(),
                targetOperationPreemptionPriority,
                forcefullyPreemptableJobs.contains(job.Get()) ? "forcefully" : "nonforcefully",
                conditionallyPreemptableJobs.contains(jobInfo) ? "conditionally" : "unconditionally"));

            job->SetPreemptedFor(TPreemptedFor{
                .JobId = jobStartedUsingPreemption->GetId(),
                .OperationId = jobStartedUsingPreemption->GetOperationId(),
            });
        } else {
            job->SetPreemptionReason(Format("Node resource limits violated"));
        }
        PreemptJob(job, operationElement, treeSnapshot, context->SchedulingContext(), preemptionReason);
    }

    // NB(eshcherbin): Specified resource limits can be violated in two cases:
    // 1. A job has just been scheduled with preemption over the limit.
    // 2. The limit has been reduced in the config.
    // Note that in the second case any job, which is considered preemptable at least in some stage,
    // may be preempted (e.g. an aggressively preemptable job can be preempted without scheduling any new jobs).
    // This is one of the reasons why we advise against specified resource limits.
    for (; currentJobIndex < std::ssize(preemptableJobs); ++currentJobIndex) {
        const auto& jobInfo = preemptableJobs[currentJobIndex];
        if (conditionallyPreemptableJobs.contains(jobInfo)) {
            // Only unconditionally preemptable jobs can be preempted to recover violated resource limits.
            continue;
        }

        const auto& [job, _, operationElement] = jobInfo;
        if (!Dominates(operationElement->GetResourceLimits(), operationElement->GetInstantResourceUsage())) {
            job->SetPreemptionReason(Format("Preempted due to violation of resource limits of operation %v",
                operationElement->GetId()));
            PreemptJob(job, operationElement, treeSnapshot, context->SchedulingContext(), EJobPreemptionReason::ResourceLimitsViolated);
            continue;
        }

        if (auto violatedPool = findPoolWithViolatedLimitsForJob(job)) {
            job->SetPreemptionReason(Format("Preempted due to violation of limits on pool %v",
                violatedPool->GetId()));
            PreemptJob(job, operationElement, treeSnapshot, context->SchedulingContext(), EJobPreemptionReason::ResourceLimitsViolated);
        }
    }

    if (!Dominates(context->SchedulingContext()->ResourceLimits(), context->SchedulingContext()->ResourceUsage())) {
        YT_LOG_INFO("Resource usage exceeds node resource limits even after preemption (ResourceLimits: %v, ResourceUsage: %v, NodeId: %v, Address: %v)",
            FormatResources(context->SchedulingContext()->ResourceLimits()),
            FormatResources(context->SchedulingContext()->ResourceUsage()),
            context->SchedulingContext()->GetNodeDescriptor().Id,
            context->SchedulingContext()->GetNodeDescriptor().Address);
    }
}

void TFairShareTreeJobScheduler::AbortJobsSinceResourcesOvercommit(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    YT_LOG_DEBUG("Interrupting jobs on node since resources are overcommitted (NodeId: %v, Address: %v)",
        schedulingContext->GetNodeDescriptor().Id,
        schedulingContext->GetNodeDescriptor().Address);

    auto jobInfos = CollectJobsWithPreemptionInfo(schedulingContext, treeSnapshot);
    SortJobsWithPreemptionInfo(&jobInfos);

    TJobResources currentResources;
    for (const auto& jobInfo : jobInfos) {
        if (!Dominates(schedulingContext->ResourceLimits(), currentResources + jobInfo.Job->ResourceUsage())) {
            YT_LOG_DEBUG("Interrupt job since node resources are overcommitted (JobId: %v, OperationId: %v)",
                jobInfo.Job->GetId(),
                jobInfo.OperationElement->GetId());
            PreemptJob(jobInfo.Job, jobInfo.OperationElement, treeSnapshot, schedulingContext, EJobPreemptionReason::ResourceOvercommit);
        } else {
            currentResources += jobInfo.Job->ResourceUsage();
        }
    }
}

void TFairShareTreeJobScheduler::PreemptJobsGracefully(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& treeConfig = treeSnapshot->TreeConfig();

    YT_LOG_TRACE("Looking for gracefully preemptable jobs");
    const auto jobInfos = CollectJobsWithPreemptionInfo(schedulingContext, treeSnapshot);
    for (const auto& [job, preemptionStatus, operationElement] : jobInfos) {
        bool shouldPreemptJobGracefully = (job->GetPreemptionMode() == EPreemptionMode::Graceful) &&
            !job->GetPreempted() &&
            (preemptionStatus == EJobPreemptionStatus::Preemptable);
        if (shouldPreemptJobGracefully) {
            schedulingContext->PreemptJob(job, treeConfig->JobGracefulInterruptTimeout, EJobPreemptionReason::GracefulPreemption);
        }
    }
}

std::vector<TJobWithPreemptionInfo> TFairShareTreeJobScheduler::CollectJobsWithPreemptionInfo(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot) const
{
    std::vector<TJobWithPreemptionInfo> jobInfos;
    for (const auto& job : schedulingContext->RunningJobs()) {
        auto* operationElement = treeSnapshot->FindEnabledOperationElement(job->GetOperationId());
        if (!operationElement || !operationElement->IsJobKnown(job->GetId())) {
            YT_LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());
            continue;
        }
        jobInfos.push_back(TJobWithPreemptionInfo{
            .Job = job,
            .PreemptionStatus = operationElement->GetJobPreemptionStatus(job->GetId()),
            .OperationElement = operationElement,
        });
    }

    return jobInfos;
}

void TFairShareTreeJobScheduler::SortJobsWithPreemptionInfo(std::vector<TJobWithPreemptionInfo>* jobInfos) const
{
    std::sort(
        jobInfos->begin(),
        jobInfos->end(),
        [&] (const TJobWithPreemptionInfo& lhs, const TJobWithPreemptionInfo& rhs) {
            if (lhs.PreemptionStatus != rhs.PreemptionStatus) {
                return lhs.PreemptionStatus < rhs.PreemptionStatus;
            }

            if (lhs.PreemptionStatus != EJobPreemptionStatus::Preemptable) {
                auto hasCpuGap = [] (const TJobWithPreemptionInfo& jobWithPreemptionInfo) {
                    return jobWithPreemptionInfo.Job->ResourceUsage().GetCpu() < jobWithPreemptionInfo.Job->ResourceLimits().GetCpu();
                };

                // Save jobs without cpu gap.
                bool lhsHasCpuGap = hasCpuGap(lhs);
                bool rhsHasCpuGap = hasCpuGap(rhs);
                if (lhsHasCpuGap != rhsHasCpuGap) {
                    return lhsHasCpuGap < rhsHasCpuGap;
                }
            }

            return lhs.Job->GetStartTime() < rhs.Job->GetStartTime();
        }
    );
}

void TFairShareTreeJobScheduler::PreemptJob(
    const TJobPtr& job,
    const TSchedulerOperationElementPtr& operationElement,
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const ISchedulingContextPtr& schedulingContext,
    EJobPreemptionReason preemptionReason) const
{
    const auto& treeConfig = treeSnapshot->TreeConfig();

    schedulingContext->ResourceUsage() -= job->ResourceUsage();
    operationElement->SetJobResourceUsage(job->GetId(), TJobResources());
    job->ResourceUsage() = {};

    schedulingContext->PreemptJob(job, treeConfig->JobInterruptTimeout, preemptionReason);
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NScheduler
