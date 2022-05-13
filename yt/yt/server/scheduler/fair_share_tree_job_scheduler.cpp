#include "fair_share_tree_job_scheduler.h"
#include "fair_share_tree_job_scheduler_operation_shared_state.h"
#include "scheduling_context.h"

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/core/misc/heap.h>
#include <yt/yt/core/misc/string_builder.h>

namespace NYT::NScheduler {

using namespace NControllerAgent;
using namespace NProfiling;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const TJobWithPreemptionInfoSet EmptyJobWithPreemptionInfoSet;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

int SchedulingIndexToProfilingRangeIndex(int schedulingIndex)
{
    return std::min(
        static_cast<int>((schedulingIndex == 0) ? 0 : (MostSignificantBit(schedulingIndex) + 1)),
        SchedulingIndexProfilingRangeCount);
}

TString FormatProfilingRangeIndex(int rangeIndex)
{
    switch (rangeIndex) {
        case 0:
        case 1:
            return ToString(rangeIndex);
        case SchedulingIndexProfilingRangeCount:
            return Format("%v-inf", 1 << (SchedulingIndexProfilingRangeCount - 1));
        default:
            return Format("%v-%v", 1 << (rangeIndex - 1), (1 << rangeIndex) - 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TJobWithPreemptionInfo> CollectJobsWithPreemptionInfo(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot)
{
    std::vector<TJobWithPreemptionInfo> jobInfos;
    for (const auto& job : schedulingContext->RunningJobs()) {
        auto* operationElement = treeSnapshot->FindEnabledOperationElement(job->GetOperationId());
        const auto& operationSharedState = operationElement
            ? treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(operationElement)
            : nullptr;
        if (!operationElement || !operationSharedState->IsJobKnown(job->GetId())) {
            const auto& Logger = StrategyLogger;

            YT_LOG_DEBUG("Dangling running job found (JobId: %v, OperationId: %v, TreeId: %v)",
                job->GetId(),
                job->GetOperationId(),
                treeSnapshot->RootElement()->GetTreeId());
            continue;
        }
        jobInfos.push_back(TJobWithPreemptionInfo{
            .Job = job,
            .PreemptionStatus = operationSharedState->GetJobPreemptionStatus(job->GetId()),
            .OperationElement = operationElement,
        });
    }

    return jobInfos;
}

void SortJobsWithPreemptionInfo(std::vector<TJobWithPreemptionInfo>* jobInfos)
{
    std::sort(
        jobInfos->begin(),
        jobInfos->end(),
        [&] (const TJobWithPreemptionInfo& lhs, const TJobWithPreemptionInfo& rhs) {
            if (lhs.PreemptionStatus != rhs.PreemptionStatus) {
                return lhs.PreemptionStatus < rhs.PreemptionStatus;
            }

            if (lhs.PreemptionStatus != EJobPreemptionStatus::Preemptable) {
                auto hasCpuGap = [](const TJobWithPreemptionInfo& jobWithPreemptionInfo) {
                    return jobWithPreemptionInfo.Job->ResourceUsage().GetCpu() <
                        jobWithPreemptionInfo.Job->ResourceLimits().GetCpu();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStaticAttributes& TStaticAttributesList::AttributesOf(const TSchedulerElement* element)
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(*this));
    return (*this)[index];
}

const TStaticAttributes& TStaticAttributesList::AttributesOf(const TSchedulerElement* element) const
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(*this));
    return (*this)[index];
}

////////////////////////////////////////////////////////////////////////////////

TDynamicAttributesList::TDynamicAttributesList(int size)
    : Value_(static_cast<size_t>(size))
{ }

TDynamicAttributes& TDynamicAttributesList::AttributesOf(const TSchedulerElement* element)
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(Value_));
    return Value_[index];
}

const TDynamicAttributes& TDynamicAttributesList::AttributesOf(const TSchedulerElement* element) const
{
    int index = element->GetTreeIndex();
    YT_ASSERT(index != UnassignedTreeIndex && index < std::ssize(Value_));
    return Value_[index];
}

void TDynamicAttributesList::UpdateAttributes(
    TSchedulerElement* element,
    const TFairShareTreeSchedulingSnapshotPtr& schedulingSnapshot,
    bool checkLiveness,
    TChildHeapMap* childHeapMap)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            UpdateAttributesAtCompositeElement(static_cast<TSchedulerCompositeElement*>(element), schedulingSnapshot, checkLiveness, childHeapMap);
            break;
        case ESchedulerElementType::Operation:
            UpdateAttributesAtOperation(static_cast<TSchedulerOperationElement*>(element), schedulingSnapshot, checkLiveness, childHeapMap);
            break;
        default:
            YT_ABORT();
    }
}

void TDynamicAttributesList::DeactivateAll()
{
    for (auto& attributes : Value_) {
        attributes.Active = false;
    }
}

void TDynamicAttributesList::InitializeResourceUsage(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TResourceUsageSnapshotPtr& resourceUsageSnapshot,
    TCpuInstant now)
{
    auto* rootElement = treeSnapshot->RootElement().Get();
    Value_.resize(rootElement->SchedulableElementCount());
    FillResourceUsage(rootElement, treeSnapshot, resourceUsageSnapshot);

    for (auto& attributes : Value_) {
        attributes.ResourceUsageUpdateTime = now;
    }
}

void TDynamicAttributesList::ActualizeResourceUsageOfOperation(
    TSchedulerOperationElement* element,
    const TFairShareTreeJobSchedulerOperationSharedStatePtr& operationSharedState)
{
    AttributesOf(element).ResourceUsage = (element->IsAlive() && operationSharedState->IsEnabled())
        ? element->GetInstantResourceUsage()
        : TJobResources();
}

void TDynamicAttributesList::UpdateAttributesAtCompositeElement(
    TSchedulerCompositeElement* element,
    const TFairShareTreeSchedulingSnapshotPtr& schedulingSnapshot,
    bool checkLiveness,
    TChildHeapMap* childHeapMap)
{
    auto& attributes = AttributesOf(element);

    if (checkLiveness && !element->IsAlive()) {
        attributes.Active = false;
        return;
    }

    // Satisfaction ratio of a composite element is the minimum of its children's satisfaction ratios.
    // NB(eshcherbin): We initialize with local satisfaction ratio in case all children have no pending jobs
    // and thus are not in the |SchedulableChildren_| list.
    if (element->GetMutable()) {
        attributes.SatisfactionRatio = element->ComputeLocalSatisfactionRatio(element->ResourceUsageAtUpdate());
    } else {
        attributes.SatisfactionRatio = element->ComputeLocalSatisfactionRatio(attributes.ResourceUsage);
    }

    // Declare the element passive if all children are passive.
    attributes.Active = false;
    attributes.BestLeafDescendant = nullptr;

    while (auto bestChild = GetBestActiveChild(element, childHeapMap)) {
        const auto& bestChildAttributes = AttributesOf(bestChild);
        auto childBestLeafDescendant = bestChildAttributes.BestLeafDescendant;
        if (checkLiveness && !childBestLeafDescendant->IsAlive()) {
            UpdateAttributes(bestChild, schedulingSnapshot, checkLiveness, childHeapMap);
            if (!bestChildAttributes.Active) {
                continue;
            }
            childBestLeafDescendant = bestChildAttributes.BestLeafDescendant;
        }

        attributes.SatisfactionRatio = std::min(bestChildAttributes.SatisfactionRatio, attributes.SatisfactionRatio);
        attributes.BestLeafDescendant = childBestLeafDescendant;
        attributes.Active = true;
        break;
    }
}

void TDynamicAttributesList::UpdateAttributesAtOperation(
    TSchedulerOperationElement* element,
    const TFairShareTreeSchedulingSnapshotPtr& schedulingSnapshot,
    bool checkLiveness,
    TChildHeapMap* /*childHeapMap*/)
{
    auto& attributes = AttributesOf(element);
    attributes.BestLeafDescendant = element;

    // NB: We treat unset Active attribute as unknown here.
    if (!attributes.Active) {
        if (checkLiveness) {
            YT_ASSERT(schedulingSnapshot);

            attributes.Active = element->IsAlive() && schedulingSnapshot->GetEnabledOperationSharedState(element)->IsEnabled();
        } else {
            attributes.Active = true;
        }
    }

    if (element->GetMutable()) {
        attributes.SatisfactionRatio = element->ComputeLocalSatisfactionRatio(element->ResourceUsageAtUpdate());
    } else {
        attributes.SatisfactionRatio = element->ComputeLocalSatisfactionRatio(attributes.ResourceUsage);
    }
}

TSchedulerElement* TDynamicAttributesList::GetBestActiveChild(TSchedulerCompositeElement* element, TChildHeapMap* childHeapMap) const
{
    const auto& childHeapIt = childHeapMap->find(element->GetTreeIndex());
    if (childHeapIt != childHeapMap->end()) {
        const auto& childHeap = childHeapIt->second;
        auto* topChild = childHeap.GetTop();
        return AttributesOf(topChild).Active
            ? topChild
            : nullptr;
    }

    switch (element->GetMode()) {
        case ESchedulingMode::Fifo:
            return GetBestActiveChildFifo(element);
        case ESchedulingMode::FairShare:
            return GetBestActiveChildFairShare(element);
        default:
            YT_ABORT();
    }
}

TSchedulerElement* TDynamicAttributesList::GetBestActiveChildFifo(TSchedulerCompositeElement* element) const
{
    TSchedulerElement* bestChild = nullptr;
    for (const auto& child : element->SchedulableChildren()) {
        if (!AttributesOf(child.Get()).Active) {
            continue;
        }

        if (!bestChild || element->HasHigherPriorityInFifoMode(child.Get(), bestChild)) {
            bestChild = child.Get();
        }
    }
    return bestChild;
}

TSchedulerElement* TDynamicAttributesList::GetBestActiveChildFairShare(TSchedulerCompositeElement* element) const
{
    TSchedulerElement* bestChild = nullptr;
    double bestChildSatisfactionRatio = InfiniteSatisfactionRatio;
    for (const auto& child : element->SchedulableChildren()) {
        if (!AttributesOf(child.Get()).Active) {
            continue;
        }

        double childSatisfactionRatio = AttributesOf(child.Get()).SatisfactionRatio;
        if (!bestChild || childSatisfactionRatio < bestChildSatisfactionRatio) {
            bestChild = child.Get();
            bestChildSatisfactionRatio = childSatisfactionRatio;
        }
    }
    return bestChild;
}

TJobResources TDynamicAttributesList::FillResourceUsage(
    TSchedulerElement* element,
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TResourceUsageSnapshotPtr& resourceUsageSnapshot)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            return FillResourceUsageAtCompositeElement(static_cast<TSchedulerCompositeElement*>(element), treeSnapshot, resourceUsageSnapshot);
        case ESchedulerElementType::Operation:
            return FillResourceUsageAtOperation(static_cast<TSchedulerOperationElement*>(element), treeSnapshot, resourceUsageSnapshot);
        default:
            YT_ABORT();
    }
}

TJobResources TDynamicAttributesList::FillResourceUsageAtCompositeElement(
    TSchedulerCompositeElement* element,
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TResourceUsageSnapshotPtr& resourceUsageSnapshot)
{
    auto& attributes = AttributesOf(element);

    attributes.ResourceUsage = element->Attributes().UnschedulableOperationsResourceUsage;
    for (const auto& child : element->SchedulableChildren()) {
        attributes.ResourceUsage += FillResourceUsage(child.Get(), treeSnapshot, resourceUsageSnapshot);
    }

    return attributes.ResourceUsage;
}

TJobResources TDynamicAttributesList::FillResourceUsageAtOperation(
    TSchedulerOperationElement* element,
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TResourceUsageSnapshotPtr& resourceUsageSnapshot)
{
    auto& attributes = AttributesOf(element);

    if (resourceUsageSnapshot != nullptr) {
        auto operationId = element->GetOperationId();
        auto it = resourceUsageSnapshot->OperationIdToResourceUsage.find(operationId);
        attributes.ResourceUsage = it != resourceUsageSnapshot->OperationIdToResourceUsage.end()
            ? it->second
            : TJobResources();
        attributes.IsNotAlive = !resourceUsageSnapshot->AliveOperationIds.contains(operationId);
    } else {
        ActualizeResourceUsageOfOperation(element, treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element));
    }

    return attributes.ResourceUsage;
}

////////////////////////////////////////////////////////////////////////////////

TChildHeap::TChildHeap(
    const TSchedulerCompositeElement* owningElement,
    TDynamicAttributesList* dynamicAttributesList)
    : OwningElement_(owningElement)
    , DynamicAttributesList_(dynamicAttributesList)
    , Mode_(OwningElement_->GetMode())
{
    ChildHeap_.reserve(OwningElement_->SchedulableChildren().size());
    for (const auto& child : OwningElement_->SchedulableChildren()) {
        ChildHeap_.push_back(child.Get());
    }
    MakeHeap(
        ChildHeap_.begin(),
        ChildHeap_.end(),
        [&] (const TSchedulerElement* lhs, const TSchedulerElement* rhs) {
            return Comparator(lhs, rhs);
        });

    for (size_t index = 0; index < ChildHeap_.size(); ++index) {
        DynamicAttributesList_->AttributesOf(ChildHeap_[index]).HeapIndex = index;
    }
}

TSchedulerElement* TChildHeap::GetTop() const
{
    YT_VERIFY(!ChildHeap_.empty());
    return ChildHeap_.front();
}

void TChildHeap::Update(TSchedulerElement* child)
{
    int heapIndex = DynamicAttributesList_->AttributesOf(child).HeapIndex;
    YT_VERIFY(heapIndex != InvalidChildHeapIndex);
    AdjustHeapItem(
        ChildHeap_.begin(),
        ChildHeap_.end(),
        ChildHeap_.begin() + heapIndex,
        [&] (const TSchedulerElement* lhs, const TSchedulerElement* rhs) {
            return Comparator(lhs, rhs);
        },
        [&] (size_t offset) {
            DynamicAttributesList_->AttributesOf(ChildHeap_[offset]).HeapIndex = offset;
        });
}

const std::vector<TSchedulerElement*>& TChildHeap::GetHeap() const
{
    return ChildHeap_;
}

bool TChildHeap::Comparator(const TSchedulerElement* lhs, const TSchedulerElement* rhs) const
{
    const auto& lhsAttributes = DynamicAttributesList_->AttributesOf(lhs);
    const auto& rhsAttributes = DynamicAttributesList_->AttributesOf(rhs);

    if (lhsAttributes.Active != rhsAttributes.Active) {
        return rhsAttributes.Active < lhsAttributes.Active;
    }

    switch (Mode_) {
        case ESchedulingMode::Fifo:
            return OwningElement_->HasHigherPriorityInFifoMode(lhs, rhs);
        case ESchedulingMode::FairShare:
            return lhsAttributes.SatisfactionRatio < rhsAttributes.SatisfactionRatio;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TChildHeapMap::UpdateChild(TSchedulerCompositeElement* parent, TSchedulerElement* child)
{
    auto it = find(parent->GetTreeIndex());
    if (it != end()) {
        auto& childHeap = it->second;
        childHeap.Update(child);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeSchedulingSnapshot::TFairShareTreeSchedulingSnapshot(
    TStaticAttributesList staticAttributesList,
    THashSet<int> ssdPriorityPreemptionMedia,
    TCachedJobPreemptionStatuses cachedJobPreemptionStatuses,
    TTreeSchedulingSegmentsState schedulingSegmentsState,
    std::vector<TSchedulingTagFilter> knownSchedulingTagFilters,
    TOperationIdToJobSchedulerSharedState operationIdToSharedState)
    : StaticAttributesList_(std::move(staticAttributesList))
    , SsdPriorityPreemptionMedia_(std::move(ssdPriorityPreemptionMedia))
    , CachedJobPreemptionStatuses_(std::move(cachedJobPreemptionStatuses))
    , SchedulingSegmentsState_(std::move(schedulingSegmentsState))
    , KnownSchedulingTagFilters_(std::move(knownSchedulingTagFilters))
    , OperationIdToSharedState_(std::move(operationIdToSharedState))
{ }

const TFairShareTreeJobSchedulerOperationSharedStatePtr& TFairShareTreeSchedulingSnapshot::GetOperationSharedState(const TSchedulerOperationElement* element) const
{
    return GetOrCrash(OperationIdToSharedState_, element->GetOperationId());
}

const TFairShareTreeJobSchedulerOperationSharedStatePtr& TFairShareTreeSchedulingSnapshot::GetEnabledOperationSharedState(const TSchedulerOperationElement* element) const
{
    const auto& operationSharedState = StaticAttributesList_.AttributesOf(element).OperationSharedState;
    YT_ASSERT(operationSharedState);
    return operationSharedState;
}

TDynamicAttributesListSnapshotPtr TFairShareTreeSchedulingSnapshot::GetDynamicAttributesListSnapshot() const
{
    return DynamicAttributesListSnapshot_.Acquire();
}

void TFairShareTreeSchedulingSnapshot::UpdateDynamicAttributesSnapshot(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TResourceUsageSnapshotPtr& resourceUsageSnapshot)
{
    if (!resourceUsageSnapshot) {
        DynamicAttributesListSnapshot_.Release();
        return;
    }

    auto attributesSnapshot = New<TDynamicAttributesListSnapshot>();
    attributesSnapshot->Value.InitializeResourceUsage(
        treeSnapshot,
        resourceUsageSnapshot,
        NProfiling::GetCpuInstant());
    DynamicAttributesListSnapshot_.Store(std::move(attributesSnapshot));
}

////////////////////////////////////////////////////////////////////////////////

TScheduleJobsProfilingCounters::TScheduleJobsProfilingCounters(
    const NProfiling::TProfiler& profiler)
    : PrescheduleJobCount(profiler.Counter("/preschedule_job_count"))
    , UselessPrescheduleJobCount(profiler.Counter("/useless_preschedule_job_count"))
    , PrescheduleJobTime(profiler.Timer("/preschedule_job_time"))
    , TotalControllerScheduleJobTime(profiler.Timer("/controller_schedule_job_time/total"))
    , ExecControllerScheduleJobTime(profiler.Timer("/controller_schedule_job_time/exec"))
    , StrategyScheduleJobTime(profiler.Timer("/strategy_schedule_job_time"))
    , PackingRecordHeartbeatTime(profiler.Timer("/packing_record_heartbeat_time"))
    , PackingCheckTime(profiler.Timer("/packing_check_time"))
    , AnalyzeJobsTime(profiler.Timer("/analyze_jobs_time"))
    , CumulativePrescheduleJobTime(profiler.TimeCounter("/cumulative_preschedule_job_time"))
    , CumulativeTotalControllerScheduleJobTime(profiler.TimeCounter("/cumulative_controller_schedule_job_time/total"))
    , CumulativeExecControllerScheduleJobTime(profiler.TimeCounter("/cumulative_controller_schedule_job_time/exec"))
    , CumulativeStrategyScheduleJobTime(profiler.TimeCounter("/cumulative_strategy_schedule_job_time"))
    , CumulativeAnalyzeJobsTime(profiler.TimeCounter("/cumulative_analyze_jobs_time"))
    , ScheduleJobAttemptCount(profiler.Counter("/schedule_job_attempt_count"))
    , ScheduleJobFailureCount(profiler.Counter("/schedule_job_failure_count"))
    , ControllerScheduleJobCount(profiler.Counter("/controller_schedule_job_count"))
    , ControllerScheduleJobTimedOutCount(profiler.Counter("/controller_schedule_job_timed_out_count"))
{
    for (auto reason : TEnumTraits<NControllerAgent::EScheduleJobFailReason>::GetDomainValues()) {
        ControllerScheduleJobFail[reason] = profiler
            .WithTag("reason", FormatEnum(reason))
            .Counter("/controller_schedule_job_fail");
    }
    for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
        DeactivationCount[reason] = profiler
            .WithTag("reason", FormatEnum(reason))
            .Counter("/deactivation_count");
    }
    for (int rangeIndex = 0; rangeIndex <= SchedulingIndexProfilingRangeCount; ++rangeIndex) {
        SchedulingIndexCounters[rangeIndex] = profiler
            .WithTag("scheduling_index", FormatProfilingRangeIndex(rangeIndex))
            .Counter("/operation_scheduling_index_attempt_count");
        MaxSchedulingIndexCounters[rangeIndex] = profiler
            .WithTag("scheduling_index", FormatProfilingRangeIndex(rangeIndex))
            .Counter("/max_operation_scheduling_index");
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TJobWithPreemptionInfo& jobInfo, TStringBuf /*format*/)
{
    builder->AppendFormat("{JobId: %v, PreemptionStatus: %v, OperationId: %v}",
        jobInfo.Job->GetId(),
        jobInfo.PreemptionStatus,
        jobInfo.OperationElement->GetId());
}

TString ToString(const TJobWithPreemptionInfo& jobInfo)
{
    return ToStringViaBuilder(jobInfo);
}

////////////////////////////////////////////////////////////////////////////////

TScheduleJobsContext::TScheduleJobsContext(
    ISchedulingContextPtr schedulingContext,
    TFairShareTreeSnapshotPtr treeSnapshot,
    std::vector<TSchedulingTagFilter> knownSchedulingTagFilters,
    bool enableSchedulingInfoLogging,
    ISchedulerStrategyHost* strategyHost,
    const NLogging::TLogger& logger)
    : SchedulingContext_(std::move(schedulingContext))
    , TreeSnapshot_(std::move(treeSnapshot))
    , KnownSchedulingTagFilters_(std::move(knownSchedulingTagFilters))
    , EnableSchedulingInfoLogging_(enableSchedulingInfoLogging)
    , StrategyHost_(strategyHost)
    , Logger(logger)
{ }

void TScheduleJobsContext::PrepareForScheduling()
{
    YT_VERIFY(StageState_);
    YT_VERIFY(!StageState_->PrescheduleExecuted);

    if (!Initialized_) {
        Initialized_ = true;

        CanSchedule_.reserve(KnownSchedulingTagFilters_.size());
        for (const auto& filter : KnownSchedulingTagFilters_) {
            CanSchedule_.push_back(SchedulingContext_->CanSchedule(filter));
        }

        if (DynamicAttributesListSnapshot_) {
            DynamicAttributesList_ = DynamicAttributesListSnapshot_->Value;
        } else {
            DynamicAttributesList_.InitializeResourceUsage(
                TreeSnapshot_,
                /*resourceUsageSnapshot*/ nullptr,
                SchedulingContext_->GetNow());
        }
    } else {
        DynamicAttributesList_.DeactivateAll();
        ChildHeapMap_.clear();
    }
}

void TScheduleJobsContext::PrescheduleJob(EOperationPreemptionPriority targetOperationPreemptionPriority)
{
    TWallTimer prescheduleTimer;

    PrescheduleJobAtCompositeElement(TreeSnapshot_->RootElement().Get(), targetOperationPreemptionPriority);

    StageState_->PrescheduleDuration = prescheduleTimer.GetElapsedTime();
    StageState_->PrescheduleExecuted = true;
}

TFairShareScheduleJobResult TScheduleJobsContext::ScheduleJob(bool ignorePacking)
{
    ++StageState_->ScheduleJobAttemptCount;

    return ScheduleJobAtCompositeElement(TreeSnapshot_->RootElement().Get(), ignorePacking);
}

TFairShareScheduleJobResult TScheduleJobsContext::ScheduleJob(TSchedulerElement* element, bool ignorePacking)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            return ScheduleJobAtCompositeElement(static_cast<TSchedulerCompositeElement*>(element), ignorePacking);
        case ESchedulerElementType::Operation:
            return ScheduleJobAtOperation(static_cast<TSchedulerOperationElement*>(element), ignorePacking);
        default:
            YT_ABORT();
    }
}

void TScheduleJobsContext::CountOperationsByPreemptionPriority()
{
    OperationCountByPreemptionPriority_ = {};
    for (const auto& [operationId, operationElement] : TreeSnapshot_->EnabledOperationMap()) {
        if (!operationElement->IsSchedulable() || operationElement->GetStarvationStatus() == EStarvationStatus::NonStarving) {
            continue;
        }
        ++OperationCountByPreemptionPriority_[GetOperationPreemptionPriority(operationElement)];
    }

    SchedulingStatistics_.OperationCountByPreemptionPriority = OperationCountByPreemptionPriority_;
}

int TScheduleJobsContext::GetOperationWithPreemptionPriorityCount(EOperationPreemptionPriority priority) const
{
    return OperationCountByPreemptionPriority_[priority];
}

void TScheduleJobsContext::AnalyzePreemptableJobs(
    EOperationPreemptionPriority targetOperationPreemptionPriority,
    EJobPreemptionLevel minJobPreemptionLevel,
    std::vector<TJobWithPreemptionInfo>* unconditionallyPreemptableJobs,
    TNonOwningJobSet* forcefullyPreemptableJobs)
{
    const auto& treeConfig = TreeSnapshot_->TreeConfig();

    YT_LOG_TRACE("Looking for preemptable jobs (MinJobPreemptionLevel: %v)", minJobPreemptionLevel);

    int totalConditionallyPreemptableJobCount = 0;
    int maxConditionallyPreemptableJobCountInPool = 0;

    NProfiling::TWallTimer timer;

    auto jobInfos = CollectJobsWithPreemptionInfo(SchedulingContext_, TreeSnapshot_);
    for (const auto& jobInfo : jobInfos) {
        const auto& [job, _, operationElement] = jobInfo;

        bool isJobForcefullyPreemptable = !IsSchedulingSegmentCompatibleWithNode(operationElement);
        if (isJobForcefullyPreemptable) {
            YT_ELEMENT_LOG_DETAILED(operationElement,
                "Job is forcefully preemptable because it is running on a node in a different scheduling segment or module "
                "(JobId: %v, OperationId: %v, OperationSegment: %v, NodeSegment: %v, Address: %v, Module: %v)",
                job->GetId(),
                operationElement->GetId(),
                operationElement->SchedulingSegment(),
                SchedulingContext_->GetSchedulingSegment(),
                SchedulingContext_->GetNodeDescriptor().Address,
                SchedulingContext_->GetNodeDescriptor().DataCenter);

            forcefullyPreemptableJobs->insert(job.Get());
        }

        bool isJobPreemptable = isJobForcefullyPreemptable || (GetJobPreemptionLevel(jobInfo) >= minJobPreemptionLevel);
        if (!isJobPreemptable) {
            continue;
        }

        auto preemptionBlockingAncestor = FindPreemptionBlockingAncestor(operationElement, targetOperationPreemptionPriority);
        bool isUnconditionalPreemptionAllowed = isJobForcefullyPreemptable || preemptionBlockingAncestor == nullptr;
        bool isConditionalPreemptionAllowed = treeConfig->EnableConditionalPreemption &&
            !isUnconditionalPreemptionAllowed &&
            preemptionBlockingAncestor != operationElement;

        if (isUnconditionalPreemptionAllowed) {
            const auto* parent = operationElement->GetParent();
            while (parent) {
                LocalUnconditionalUsageDiscountMap_[parent->GetTreeIndex()] += job->ResourceUsage();
                parent = parent->GetParent();
            }
            SchedulingContext_->UnconditionalResourceUsageDiscount() += job->ResourceUsage();
            unconditionallyPreemptableJobs->push_back(jobInfo);
        } else if (isConditionalPreemptionAllowed) {
            ConditionallyPreemptableJobSetMap_[preemptionBlockingAncestor->GetTreeIndex()].insert(jobInfo);
            ++totalConditionallyPreemptableJobCount;
        }
    }

    TPrepareConditionalUsageDiscountsContext context{.TargetOperationPreemptionPriority = targetOperationPreemptionPriority};
    PrepareConditionalUsageDiscountsAtCompositeElement(TreeSnapshot_->RootElement().Get(), &context);
    for (const auto& [_, jobSet] : ConditionallyPreemptableJobSetMap_) {
        maxConditionallyPreemptableJobCountInPool = std::max(
            maxConditionallyPreemptableJobCountInPool,
            static_cast<int>(jobSet.size()));
    }

    StageState_->AnalyzeJobsDuration += timer.GetElapsedTime();

    SchedulingStatistics_.UnconditionallyPreemptableJobCount = unconditionallyPreemptableJobs->size();
    SchedulingStatistics_.UnconditionalResourceUsageDiscount = SchedulingContext_->UnconditionalResourceUsageDiscount();
    SchedulingStatistics_.MaxConditionalResourceUsageDiscount = SchedulingContext_->GetMaxConditionalUsageDiscount();
    SchedulingStatistics_.TotalConditionallyPreemptableJobCount = totalConditionallyPreemptableJobCount;
    SchedulingStatistics_.MaxConditionallyPreemptableJobCountInPool = maxConditionallyPreemptableJobCountInPool;
}

void TScheduleJobsContext::PreemptJobsAfterScheduling(
    EOperationPreemptionPriority targetOperationPreemptionPriority,
    std::vector<TJobWithPreemptionInfo> preemptableJobs,
    const TNonOwningJobSet& forcefullyPreemptableJobs,
    const TJobPtr& jobStartedUsingPreemption)
{
    // Collect conditionally preemptable jobs.
    TJobWithPreemptionInfoSet conditionallyPreemptableJobs;
    if (jobStartedUsingPreemption) {
        auto* operationElement = TreeSnapshot_->FindEnabledOperationElement(jobStartedUsingPreemption->GetOperationId());
        YT_VERIFY(operationElement);

        auto* parent = operationElement->GetParent();
        while (parent) {
            const auto& parentConditionallyPreemptableJobs = GetConditionallyPreemptableJobsInPool(parent);
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
    SchedulingContext_->ResetUsageDiscounts();
    LocalUnconditionalUsageDiscountMap_.clear();
    ConditionallyPreemptableJobSetMap_.clear();

    auto findPoolWithViolatedLimitsForJob = [&] (const TJobPtr& job) -> const TSchedulerCompositeElement* {
        auto* operationElement = TreeSnapshot_->FindEnabledOperationElement(job->GetOperationId());
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
        if (Dominates(SchedulingContext_->ResourceLimits(), SchedulingContext_->ResourceUsage())) {
            break;
        }

        const auto& jobInfo = preemptableJobs[currentJobIndex];
        const auto& [job, _, operationElement] = jobInfo;

        if (!IsJobKnown(operationElement, job->GetId())) {
            // Job may have been terminated concurrently with scheduling, e.g. operation aborted by user request. See: YT-16429.
            YT_LOG_DEBUG("Job preemption skipped, since the job is already terminated (JobId: %v, OperationId: %v)",
                job->GetId(),
                job->GetOperationId());

            continue;
        }

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
        PreemptJob(job, operationElement, preemptionReason);
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
            PreemptJob(job, operationElement, EJobPreemptionReason::ResourceLimitsViolated);
            continue;
        }

        if (auto violatedPool = findPoolWithViolatedLimitsForJob(job)) {
            job->SetPreemptionReason(Format("Preempted due to violation of limits on pool %v",
                violatedPool->GetId()));
            PreemptJob(job, operationElement, EJobPreemptionReason::ResourceLimitsViolated);
        }
    }

    if (!Dominates(SchedulingContext_->ResourceLimits(), SchedulingContext_->ResourceUsage())) {
        YT_LOG_INFO("Resource usage exceeds node resource limits even after preemption (ResourceLimits: %v, ResourceUsage: %v, NodeId: %v, Address: %v)",
            FormatResources(SchedulingContext_->ResourceLimits()),
            FormatResources(SchedulingContext_->ResourceUsage()),
            SchedulingContext_->GetNodeDescriptor().Id,
            SchedulingContext_->GetNodeDescriptor().Address);
    }
}

void TScheduleJobsContext::AbortJobsSinceResourcesOvercommit() const
{
    YT_LOG_DEBUG("Interrupting jobs on node since resources are overcommitted (NodeId: %v, Address: %v)",
        SchedulingContext_->GetNodeDescriptor().Id,
        SchedulingContext_->GetNodeDescriptor().Address);

    auto jobInfos = CollectJobsWithPreemptionInfo(SchedulingContext_, TreeSnapshot_);
    SortJobsWithPreemptionInfo(&jobInfos);

    TJobResources currentResources;
    for (const auto& jobInfo : jobInfos) {
        if (!Dominates(SchedulingContext_->ResourceLimits(), currentResources + jobInfo.Job->ResourceUsage())) {
            YT_LOG_DEBUG("Interrupt job since node resources are overcommitted (JobId: %v, OperationId: %v)",
                jobInfo.Job->GetId(),
                jobInfo.OperationElement->GetId());

            jobInfo.Job->SetPreemptionReason("Preempted due to node resource ovecommit");
            PreemptJob(jobInfo.Job, jobInfo.OperationElement, EJobPreemptionReason::ResourceOvercommit);
        } else {
            currentResources += jobInfo.Job->ResourceUsage();
        }
    }
}

void TScheduleJobsContext::PreemptJob(
    const TJobPtr& job,
    TSchedulerOperationElement* element,
    EJobPreemptionReason preemptionReason) const
{
    const auto& treeConfig = TreeSnapshot_->TreeConfig();

    SchedulingContext_->ResourceUsage() -= job->ResourceUsage();
    job->ResourceUsage() = TJobResources();

    const auto& operationSharedState = TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element);
    auto delta = operationSharedState->SetJobResourceUsage(job->GetId(), TJobResources());
    element->IncreaseHierarchicalResourceUsage(delta);
    operationSharedState->UpdatePreemptableJobsList(element);

    SchedulingContext_->PreemptJob(job, treeConfig->JobInterruptTimeout, preemptionReason);
}

void TScheduleJobsContext::ReactivateBadPackingOperations()
{
    for (const auto& operation : BadPackingOperations_) {
        // TODO(antonkikh): multiple activations can be implemented more efficiently.
        ActivateOperation(operation.Get());
    }
    BadPackingOperations_.clear();
}

bool TScheduleJobsContext::HasBadPackingOperations() const
{
    return !BadPackingOperations_.empty();
}

void TScheduleJobsContext::StartStage(TScheduleJobsStage* schedulingStage)
{
    YT_VERIFY(!StageState_);

    StageState_.emplace(TStageState{
        .SchedulingStage = schedulingStage,
        .Timer = TWallTimer(),
    });
}

void TScheduleJobsContext::FinishStage()
{
    YT_VERIFY(StageState_);

    SchedulingStatistics_.ScheduleJobAttemptCountPerStage[GetStageType()] = StageState_->ScheduleJobAttemptCount;
    ProfileAndLogStatisticsOfStage();

    StageState_.reset();
}

int TScheduleJobsContext::GetStageMaxSchedulingIndex() const
{
    return StageState_->MaxSchedulingIndex;
}

bool TScheduleJobsContext::GetStagePrescheduleExecuted() const
{
    return StageState_->PrescheduleExecuted;
}

void TScheduleJobsContext::SetDynamicAttributesListSnapshot(TDynamicAttributesListSnapshotPtr snapshot)
{
    DynamicAttributesListSnapshot_ = std::move(snapshot);
}

const TSchedulerElement* TScheduleJobsContext::FindPreemptionBlockingAncestor(
    const TSchedulerOperationElement* element,
    EOperationPreemptionPriority targetOperationPreemptionPriority) const
{
    const auto& treeConfig = TreeSnapshot_->TreeConfig();
    const auto& spec = element->Spec();

    if (spec->PreemptionMode == EPreemptionMode::Graceful) {
        return element;
    }

    int maxUnpreemptableJobCount = treeConfig->MaxUnpreemptableRunningJobCount;
    if (spec->MaxUnpreemptableRunningJobCount) {
        maxUnpreemptableJobCount = std::min(maxUnpreemptableJobCount, *spec->MaxUnpreemptableRunningJobCount);
    }

    int jobCount = GetOperationRunningJobCount(element);
    if (jobCount <= maxUnpreemptableJobCount) {
        UpdateOperationPreemptionStatusStatistics(element, EOperationPreemptionStatus::ForbiddenSinceLowJobCount);
        return element;
    }

    const TSchedulerElement* current = element;
    while (current && !current->IsRoot()) {
        // NB(eshcherbin): A bit strange that we check for starvation here and then for satisfaction later.
        // Maybe just satisfaction is enough?
        if (treeConfig->PreemptionCheckStarvation && current->GetStarvationStatus() != EStarvationStatus::NonStarving) {
            UpdateOperationPreemptionStatusStatistics(
                element,
                current == element
                    ? EOperationPreemptionStatus::ForbiddenSinceStarving
                    : EOperationPreemptionStatus::AllowedConditionally);
            return current;
        }

        bool useAggressiveThreshold = StaticAttributesOf(current).EffectiveAggressivePreemptionAllowed &&
            targetOperationPreemptionPriority >= EOperationPreemptionPriority::Aggressive;
        auto threshold = useAggressiveThreshold
            ? treeConfig->AggressivePreemptionSatisfactionThreshold
            : treeConfig->PreemptionSatisfactionThreshold;

        // NB: We want to use *local* satisfaction ratio here.
        double localSatisfactionRatio = current->ComputeLocalSatisfactionRatio(GetCurrentResourceUsage(current));
        if (treeConfig->PreemptionCheckSatisfaction && localSatisfactionRatio < threshold + NVectorHdrf::RatioComparisonPrecision) {
            UpdateOperationPreemptionStatusStatistics(
                element,
                current == element
                    ? EOperationPreemptionStatus::ForbiddenSinceUnsatisfied
                    : EOperationPreemptionStatus::AllowedConditionally);
            return element;
        }

        current = current->GetParent();
    }

    UpdateOperationPreemptionStatusStatistics(element, EOperationPreemptionStatus::AllowedUnconditionally);
    return {};
}

void TScheduleJobsContext::PrepareConditionalUsageDiscounts(const TSchedulerElement* element, TPrepareConditionalUsageDiscountsContext* context)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            PrepareConditionalUsageDiscountsAtCompositeElement(static_cast<const TSchedulerCompositeElement*>(element), context);
            break;
        case ESchedulerElementType::Operation:
            PrepareConditionalUsageDiscountsAtOperation(static_cast<const TSchedulerOperationElement*>(element), context);
            break;
        default:
            YT_ABORT();
    }
}

const TJobWithPreemptionInfoSet& TScheduleJobsContext::GetConditionallyPreemptableJobsInPool(const TSchedulerCompositeElement* element) const
{
    auto it = ConditionallyPreemptableJobSetMap_.find(element->GetTreeIndex());
    return it != ConditionallyPreemptableJobSetMap_.end() ? it->second : EmptyJobWithPreemptionInfoSet;
}

TDynamicAttributes& TScheduleJobsContext::DynamicAttributesOf(const TSchedulerElement* element)
{
    YT_ASSERT(Initialized_);

    return DynamicAttributesList_.AttributesOf(element);
}

const TDynamicAttributes& TScheduleJobsContext::DynamicAttributesOf(const TSchedulerElement* element) const
{
    YT_ASSERT(Initialized_);

    return DynamicAttributesList_.AttributesOf(element);
}

const TStaticAttributes& TScheduleJobsContext::StaticAttributesOf(const TSchedulerElement* element) const
{
    return TreeSnapshot_->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element);
}

bool TScheduleJobsContext::IsActive(const TSchedulerElement* element) const
{
    return DynamicAttributesList_.AttributesOf(element).Active;
}

TJobResources TScheduleJobsContext::GetCurrentResourceUsage(const TSchedulerElement* element) const
{
    if (element->IsSchedulable()) {
        return DynamicAttributesOf(element).ResourceUsage;
    } else {
        return element->Attributes().UnschedulableOperationsResourceUsage;
    }
}

void TScheduleJobsContext::UpdateDynamicAttributes(TSchedulerElement* element, bool checkLiveness)
{
    DynamicAttributesList_.UpdateAttributes(element, TreeSnapshot_->SchedulingSnapshot(), checkLiveness, &ChildHeapMap_);
}

TJobResources TScheduleJobsContext::GetHierarchicalAvailableResources(const TSchedulerElement* element) const
{
    auto availableResources = TJobResources::Infinite();
    while (element) {
        availableResources = Min(availableResources, GetLocalAvailableResourceLimits(element));
        element = element->GetParent();
    }

    return availableResources;
}

TJobResources TScheduleJobsContext::GetLocalAvailableResourceLimits(const TSchedulerElement* element) const
{
    if (element->GetHasSpecifiedResourceLimits()) {
        return ComputeAvailableResources(
            element->ResourceLimits(),
            element->GetResourceUsageWithPrecommit(),
            GetLocalUnconditionalUsageDiscount(element));
    }
    return TJobResources::Infinite();
}

TJobResources TScheduleJobsContext::GetLocalUnconditionalUsageDiscount(const TSchedulerElement* element) const
{
    int index = element->GetTreeIndex();
    YT_VERIFY(index != UnassignedTreeIndex);

    auto it = LocalUnconditionalUsageDiscountMap_.find(index);
    return it != LocalUnconditionalUsageDiscountMap_.end() ? it->second : TJobResources{};
}

void TScheduleJobsContext::PrescheduleJob(
    TSchedulerElement* element,
    EOperationPreemptionPriority targetOperationPreemptionPriority)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            PrescheduleJobAtCompositeElement(static_cast<TSchedulerCompositeElement*>(element), targetOperationPreemptionPriority);
            break;
        case ESchedulerElementType::Operation:
            PrescheduleJobAtOperation(static_cast<TSchedulerOperationElement*>(element), targetOperationPreemptionPriority);
            break;
        default:
            YT_ABORT();
    }
}

void TScheduleJobsContext::PrescheduleJobAtCompositeElement(
    TSchedulerCompositeElement* element,
    EOperationPreemptionPriority targetOperationPreemptionPriority)
{
    auto& attributes = DynamicAttributesOf(element);

    auto onDeactivated = [&] (EDeactivationReason deactivationReason) {
        ++StageState_->DeactivationReasons[deactivationReason];
        YT_VERIFY(!attributes.Active);
    };

    if (!element->IsAlive()) {
        onDeactivated(EDeactivationReason::IsNotAlive);
        return;
    }

    if (TreeSnapshot_->TreeConfig()->EnableSchedulingTags && !CanSchedule(StaticAttributesOf(element).SchedulingTagFilterIndex)) {
        onDeactivated(EDeactivationReason::UnmatchedSchedulingTag);
        return;
    }

    for (const auto& child : element->SchedulableChildren()) {
        PrescheduleJob(child.Get(), targetOperationPreemptionPriority);
    }

    UpdateDynamicAttributes(element, /*checkLiveness*/ true);
    InitializeChildHeap(element);

    if (attributes.Active) {
        ++StageState_->ActiveTreeSize;
    }
}

void TScheduleJobsContext::PrescheduleJobAtOperation(
    TSchedulerOperationElement* element,
    EOperationPreemptionPriority targetOperationPreemptionPriority)
{
    auto& attributes = DynamicAttributesOf(element);

    CheckForDeactivation(element, targetOperationPreemptionPriority);
    if (!attributes.Active) {
        return;
    }

    UpdateDynamicAttributes(element, /*checkLiveness*/ true);

    ++StageState_->ActiveTreeSize;
    ++StageState_->ActiveOperationCount;
}

TFairShareScheduleJobResult TScheduleJobsContext::ScheduleJobAtCompositeElement(TSchedulerCompositeElement* element, bool ignorePacking)
{
    auto& attributes = DynamicAttributesOf(element);

    TSchedulerOperationElement* bestLeafDescendant = nullptr;
    TSchedulerOperationElement* lastConsideredBestLeafDescendant = nullptr;
    while (!bestLeafDescendant) {
        if (!attributes.Active) {
            return TFairShareScheduleJobResult{
                .Finished = true,
                .Scheduled = false,
            };
        }

        bestLeafDescendant = attributes.BestLeafDescendant;
        if (!bestLeafDescendant->IsAlive()) {
            DynamicAttributesOf(bestLeafDescendant).Active = false;
            UpdateDynamicAttributes(element, /*checkLiveness*/ true);
            bestLeafDescendant = nullptr;
            continue;
        }
        if (lastConsideredBestLeafDescendant != bestLeafDescendant && IsUsageOutdated(bestLeafDescendant)) {
            UpdateCurrentResourceUsage(bestLeafDescendant);
            lastConsideredBestLeafDescendant = bestLeafDescendant;
            bestLeafDescendant = nullptr;
            continue;
        }
    }

    auto childResult = ScheduleJob(bestLeafDescendant, ignorePacking);
    return TFairShareScheduleJobResult{
        .Finished = false,
        .Scheduled = childResult.Scheduled,
    };
}

TFairShareScheduleJobResult TScheduleJobsContext::ScheduleJobAtOperation(TSchedulerOperationElement* element, bool ignorePacking)
{
    YT_VERIFY(IsActive(element));

    YT_ELEMENT_LOG_DETAILED(element,
        "Trying to schedule job "
        "(SatisfactionRatio: %v, NodeId: %v, NodeResourceUsage: %v, "
        "UsageDiscount: {Total: %v, Unconditional: %v, Conditional: %v}, StageType: %v)",
        DynamicAttributesOf(element).SatisfactionRatio,
        SchedulingContext_->GetNodeDescriptor().Id,
        FormatResourceUsage(SchedulingContext_->ResourceUsage(), SchedulingContext_->ResourceLimits()),
        FormatResources(SchedulingContext_->UnconditionalResourceUsageDiscount() +
            SchedulingContext_->GetConditionalDiscountForOperation(element->GetOperationId())),
        FormatResources(SchedulingContext_->UnconditionalResourceUsageDiscount()),
        FormatResources(SchedulingContext_->GetConditionalDiscountForOperation(element->GetOperationId())),
        GetStageType());

    auto deactivateOperationElement = [&] (EDeactivationReason reason) {
        YT_ELEMENT_LOG_DETAILED(element,
            "Failed to schedule job, operation deactivated "
            "(DeactivationReason: %v, NodeResourceUsage: %v)",
            FormatEnum(reason),
            FormatResourceUsage(SchedulingContext_->ResourceUsage(), SchedulingContext_->ResourceLimits()));

        DeactivateOperation(element, reason);
    };

    auto recordPackingHeartbeatWithTimer = [&] (const auto& heartbeatSnapshot) {
        NProfiling::TWallTimer timer;
        RecordPackingHeartbeat(element, heartbeatSnapshot);
        StageState_->PackingRecordHeartbeatDuration += timer.GetElapsedTime();
    };

    auto decreaseHierarchicalResourceUsagePrecommit = [&] (const TJobResources& precommittedResources, int scheduleJobEpoch) {
        if (IsOperationEnabled(element) && scheduleJobEpoch == element->GetControllerEpoch()) {
            element->DecreaseHierarchicalResourceUsagePrecommit(precommittedResources);
        }
    };

    int schedulingIndex = StaticAttributesOf(element).SchedulingIndex;
    YT_VERIFY(schedulingIndex != UndefinedSchedulingIndex);
    ++StageState_->SchedulingIndexToScheduleJobAttemptCount[schedulingIndex];
    StageState_->MaxSchedulingIndex = std::max(StageState_->MaxSchedulingIndex, schedulingIndex);

    if (auto blockedReason = CheckBlocked(element)) {
        deactivateOperationElement(*blockedReason);
        return TFairShareScheduleJobResult{
            .Finished = true,
            .Scheduled = false,
        };
    }

    if (!IsOperationEnabled(element)) {
        deactivateOperationElement(EDeactivationReason::IsNotAlive);
        return TFairShareScheduleJobResult{
            .Finished = true,
            .Scheduled = false,
        };
    }

    if (!HasJobsSatisfyingResourceLimits(element)) {
        YT_ELEMENT_LOG_DETAILED(element,
            "No pending jobs can satisfy available resources on node ("
            "FreeResources: %v, DiscountResources: {Total: %v, Unconditional: %v, Conditional: %v}, "
            "MinNeededResources: %v, DetailedMinNeededResources: %v, "
            "Address: %v)",
            FormatResources(SchedulingContext_->GetNodeFreeResourcesWithoutDiscount()),
            FormatResources(SchedulingContext_->UnconditionalResourceUsageDiscount() +
                SchedulingContext_->GetConditionalDiscountForOperation(element->GetOperationId())),
            FormatResources(SchedulingContext_->UnconditionalResourceUsageDiscount()),
            FormatResources(SchedulingContext_->GetConditionalDiscountForOperation(element->GetOperationId())),
            FormatResources(element->AggregatedMinNeededJobResources()),
            MakeFormattableView(
                element->DetailedMinNeededJobResources(),
                [&] (TStringBuilderBase* builder, const TJobResourcesWithQuota& resources) {
                    builder->AppendFormat("%v", StrategyHost_->FormatResources(resources));
                }),
            SchedulingContext_->GetNodeDescriptor().Address);

        OnMinNeededResourcesUnsatisfied(
            element,
            SchedulingContext_->GetNodeFreeResourcesWithDiscountForOperation(element->GetOperationId()),
            element->AggregatedMinNeededJobResources());
        deactivateOperationElement(EDeactivationReason::MinNeededResourcesUnsatisfied);
        return TFairShareScheduleJobResult{
            .Finished = true,
            .Scheduled = false,
        };
    }

    TJobResources precommittedResources;
    TJobResources availableResources;

    int scheduleJobEpoch = element->GetControllerEpoch();

    auto deactivationReason = TryStartScheduleJob(
        element,
        &precommittedResources,
        &availableResources);
    if (deactivationReason) {
        deactivateOperationElement(*deactivationReason);
        return TFairShareScheduleJobResult{
            .Finished = true,
            .Scheduled = false,
        };
    }

    std::optional<TPackingHeartbeatSnapshot> heartbeatSnapshot;
    if (GetPackingConfig()->Enable && !ignorePacking) {
        heartbeatSnapshot = CreateHeartbeatSnapshot(SchedulingContext_);

        bool acceptPacking;
        {
            NProfiling::TWallTimer timer;
            acceptPacking = CheckPacking(element, *heartbeatSnapshot);
            StageState_->PackingCheckDuration += timer.GetElapsedTime();
        }

        if (!acceptPacking) {
            recordPackingHeartbeatWithTimer(*heartbeatSnapshot);
            decreaseHierarchicalResourceUsagePrecommit(precommittedResources, scheduleJobEpoch);
            deactivateOperationElement(EDeactivationReason::BadPacking);
            BadPackingOperations_.push_back(element);
            FinishScheduleJob(element);
            return TFairShareScheduleJobResult{
                .Finished = true,
                .Scheduled = false,
            };
        }
    }

    TControllerScheduleJobResultPtr scheduleJobResult;
    {
        NProfiling::TWallTimer timer;

        scheduleJobResult = DoScheduleJob(element, availableResources, &precommittedResources);

        auto scheduleJobDuration = timer.GetElapsedTime();
        StageState_->TotalScheduleJobDuration += scheduleJobDuration;
        StageState_->ExecScheduleJobDuration += scheduleJobResult->Duration;
    }

    if (!scheduleJobResult->StartDescriptor) {
        for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
            StageState_->FailedScheduleJob[reason] += scheduleJobResult->Failed[reason];
        }

        ++StageState_->ScheduleJobFailureCount;
        deactivateOperationElement(EDeactivationReason::ScheduleJobFailed);

        element->OnScheduleJobFailed(
            SchedulingContext_->GetNow(),
            element->GetTreeId(),
            scheduleJobResult);

        decreaseHierarchicalResourceUsagePrecommit(precommittedResources, scheduleJobEpoch);
        FinishScheduleJob(element);

        return TFairShareScheduleJobResult{
            .Finished = true,
            .Scheduled = false,
        };
    }

    const auto& startDescriptor = *scheduleJobResult->StartDescriptor;

    const auto& operationSharedState = TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element);
    bool onJobStartedSuccess = operationSharedState->OnJobStarted(
        element,
        startDescriptor.Id,
        startDescriptor.ResourceLimits,
        precommittedResources,
        scheduleJobEpoch);
    if (!onJobStartedSuccess) {
        element->AbortJob(
            startDescriptor.Id,
            EAbortReason::SchedulingOperationDisabled,
            element->GetTreeId(),
            scheduleJobResult->ControllerEpoch);
        deactivateOperationElement(EDeactivationReason::OperationDisabled);
        decreaseHierarchicalResourceUsagePrecommit(precommittedResources, scheduleJobEpoch);
        FinishScheduleJob(element);
        return TFairShareScheduleJobResult{
            .Finished = true,
            .Scheduled = false,
        };
    }

    SchedulingContext_->StartJob(
        element->GetTreeId(),
        element->GetOperationId(),
        scheduleJobResult->IncarnationId,
        scheduleJobResult->ControllerEpoch,
        startDescriptor,
        element->Spec()->PreemptionMode,
        schedulingIndex,
        GetStageType());

    UpdateCurrentResourceUsage(element);

    if (heartbeatSnapshot) {
        recordPackingHeartbeatWithTimer(*heartbeatSnapshot);
    }

    FinishScheduleJob(element);

    YT_ELEMENT_LOG_DETAILED(element,
        "Scheduled a job (SatisfactionRatio: %v, NodeId: %v, JobId: %v, JobResourceLimits: %v)",
        DynamicAttributesOf(element).SatisfactionRatio,
        SchedulingContext_->GetNodeDescriptor().Id,
        startDescriptor.Id,
        StrategyHost_->FormatResources(startDescriptor.ResourceLimits));
    return TFairShareScheduleJobResult{
        .Finished = true,
        .Scheduled = true,
    };
}

void TScheduleJobsContext::PrepareConditionalUsageDiscountsAtCompositeElement(
    const TSchedulerCompositeElement* element,
    TPrepareConditionalUsageDiscountsContext* context)
{
    TJobResources deltaConditionalDiscount;
    for (const auto& jobInfo : GetConditionallyPreemptableJobsInPool(element)) {
        deltaConditionalDiscount += jobInfo.Job->ResourceUsage();
    }

    context->CurrentConditionalDiscount += deltaConditionalDiscount;
    for (const auto& child : element->SchedulableChildren()) {
        PrepareConditionalUsageDiscounts(child.Get(), context);
    }
    context->CurrentConditionalDiscount -= deltaConditionalDiscount;
}

void TScheduleJobsContext::PrepareConditionalUsageDiscountsAtOperation(
    const TSchedulerOperationElement* element,
    TPrepareConditionalUsageDiscountsContext* context)
{
    if (GetOperationPreemptionPriority(element) != context->TargetOperationPreemptionPriority) {
        return;
    }

    SchedulingContext_->SetConditionalDiscountForOperation(element->GetOperationId(), context->CurrentConditionalDiscount);
}

void TScheduleJobsContext::InitializeChildHeap(const TSchedulerCompositeElement* element)
{
    if (std::ssize(element->SchedulableChildren()) < TreeSnapshot_->TreeConfig()->MinChildHeapSize) {
        return;
    }

    StageState_->TotalHeapElementCount += std::ssize(element->SchedulableChildren());

    ChildHeapMap_.emplace(element->GetTreeIndex(), TChildHeap(element, &DynamicAttributesList_));
}

std::optional<EDeactivationReason> TScheduleJobsContext::TryStartScheduleJob(
    TSchedulerOperationElement* element,
    TJobResources* precommittedResourcesOutput,
    TJobResources* availableResourcesOutput)
{
    const auto& minNeededResources = element->AggregatedMinNeededJobResources();

    // Do preliminary checks to avoid the overhead of updating and reverting precommit usage.
    if (!Dominates(GetHierarchicalAvailableResources(element), minNeededResources)) {
        return EDeactivationReason::ResourceLimitsExceeded;
    }
    if (!element->CheckAvailableDemand(minNeededResources)) {
        return EDeactivationReason::NoAvailableDemand;
    }

    TJobResources availableResourceLimits;
    auto increaseResult = element->TryIncreaseHierarchicalResourceUsagePrecommit(
        minNeededResources,
        &availableResourceLimits);

    if (increaseResult == EResourceTreeIncreaseResult::ResourceLimitExceeded) {
        return EDeactivationReason::ResourceLimitsExceeded;
    }
    if (increaseResult == EResourceTreeIncreaseResult::ElementIsNotAlive) {
        return EDeactivationReason::IsNotAlive;
    }

    element->IncreaseConcurrentScheduleJobCalls(SchedulingContext_->GetNodeShardId());
    element->IncreaseScheduleJobCallsSinceLastUpdate(SchedulingContext_->GetNodeShardId());

    *precommittedResourcesOutput = minNeededResources;
    *availableResourcesOutput = Min(
        availableResourceLimits,
        SchedulingContext_->GetNodeFreeResourcesWithDiscountForOperation(element->GetOperationId()));
    return {};
}

TControllerScheduleJobResultPtr TScheduleJobsContext::DoScheduleJob(
    TSchedulerOperationElement* element,
    const TJobResources& availableResources,
    TJobResources* precommittedResources)
{
    ++SchedulingStatistics_.ControllerScheduleJobCount;

    auto scheduleJobResult = element->ScheduleJob(
        SchedulingContext_,
        availableResources,
        TreeSnapshot_->ControllerConfig()->ScheduleJobTimeLimit,
        element->GetTreeId(),
        TreeSnapshot_->TreeConfig());

    MaybeDelay(
        element->Spec()->TestingOperationOptions->ScheduleJobDelay,
        element->Spec()->TestingOperationOptions->ScheduleJobDelayType);

    // Discard the job in case of resource overcommit.
    if (scheduleJobResult->StartDescriptor) {
        const auto& startDescriptor = *scheduleJobResult->StartDescriptor;
        // Note: |resourceDelta| might be negative.
        const auto resourceDelta = startDescriptor.ResourceLimits.ToJobResources() - *precommittedResources;
        // NB: If the element is disabled, we still choose the success branch. This is kind of a hotfix. See: YT-16070.
        auto increaseResult = EResourceTreeIncreaseResult::Success;
        if (IsOperationEnabled(element)) {
            increaseResult = element->TryIncreaseHierarchicalResourceUsagePrecommit(resourceDelta);
        }
        switch (increaseResult) {
            case EResourceTreeIncreaseResult::Success: {
                *precommittedResources += resourceDelta;
                break;
            }
            case EResourceTreeIncreaseResult::ResourceLimitExceeded: {
                auto jobId = scheduleJobResult->StartDescriptor->Id;
                // NB(eshcherbin): GetHierarchicalAvailableResource will never return infinite resources here,
                // because ResourceLimitExceeded could only be triggered if there's an ancestor with specified limits.
                auto availableDelta = GetHierarchicalAvailableResources(element);
                YT_LOG_DEBUG("Aborting job with resource overcommit (JobId: %v, Limits: %v, JobResources: %v)",
                    jobId,
                    FormatResources(*precommittedResources + availableDelta),
                    FormatResources(startDescriptor.ResourceLimits.ToJobResources()));

                element->AbortJob(
                    jobId,
                    EAbortReason::SchedulingResourceOvercommit,
                    element->GetTreeId(),
                    scheduleJobResult->ControllerEpoch);

                // Reset result.
                scheduleJobResult = New<TControllerScheduleJobResult>();
                scheduleJobResult->RecordFail(EScheduleJobFailReason::ResourceOvercommit);
                break;
            }
            case EResourceTreeIncreaseResult::ElementIsNotAlive: {
                auto jobId = scheduleJobResult->StartDescriptor->Id;
                YT_LOG_DEBUG("Aborting job as operation is not alive in tree anymore (JobId: %v)", jobId);

                element->AbortJob(
                    jobId,
                    EAbortReason::SchedulingOperationIsNotAlive,
                    element->GetTreeId(),
                    scheduleJobResult->ControllerEpoch);

                scheduleJobResult = New<TControllerScheduleJobResult>();
                scheduleJobResult->RecordFail(EScheduleJobFailReason::OperationIsNotAlive);
                break;
            }
            default:
                YT_ABORT();
        }
    } else if (scheduleJobResult->Failed[EScheduleJobFailReason::Timeout] > 0) {
        YT_LOG_WARNING("Job scheduling timed out");

        ++SchedulingStatistics_.ControllerScheduleJobTimedOutCount;

        StrategyHost_->SetOperationAlert(
            element->GetOperationId(),
            EOperationAlertType::ScheduleJobTimedOut,
            TError("Job scheduling timed out: either scheduler is under heavy load or operation is too heavy"),
            TreeSnapshot_->ControllerConfig()->ScheduleJobTimeoutAlertResetTime);
    }

    return scheduleJobResult;
}

void TScheduleJobsContext::FinishScheduleJob(TSchedulerOperationElement* element)
{
    element->DecreaseConcurrentScheduleJobCalls(SchedulingContext_->GetNodeShardId());
}

EOperationPreemptionPriority TScheduleJobsContext::GetOperationPreemptionPriority(
    const TSchedulerOperationElement* operationElement) const
{
    bool isEligibleForSsdPriorityPreemption = SsdPriorityPreemptionEnabled_ &&
        IsEligibleForSsdPriorityPreemption(operationElement->DiskRequestMedia());
    if (operationElement->GetLowestAggressivelyStarvingAncestor()) {
        return isEligibleForSsdPriorityPreemption
            ? EOperationPreemptionPriority::SsdAggressive
            : EOperationPreemptionPriority::Aggressive;
    }
    if (operationElement->GetLowestStarvingAncestor()) {
        return isEligibleForSsdPriorityPreemption
            ? EOperationPreemptionPriority::SsdRegular
            : EOperationPreemptionPriority::Regular;
    }
    return EOperationPreemptionPriority::None;
}

void TScheduleJobsContext::CheckForDeactivation(
    TSchedulerOperationElement* element,
    EOperationPreemptionPriority targetOperationPreemptionPriority)
{
    const auto& treeConfig = TreeSnapshot_->TreeConfig();
    auto& attributes = DynamicAttributesOf(element);

    // Reset operation element activeness (it can be active after scheduling without preemption).
    attributes.Active = false;

    if (attributes.IsNotAlive) {
        OnOperationDeactivated(element, EDeactivationReason::IsNotAlive);
        return;
    }

    if (targetOperationPreemptionPriority != EOperationPreemptionPriority::None &&
        targetOperationPreemptionPriority != GetOperationPreemptionPriority(element))
    {
        auto deactivationReason = [&] {
            YT_VERIFY(targetOperationPreemptionPriority != EOperationPreemptionPriority::None);

            // TODO(eshcherbin): Somehow get rid of these deactivation reasons.
            switch (targetOperationPreemptionPriority) {
                case EOperationPreemptionPriority::Regular:
                    return EDeactivationReason::IsNotEligibleForPreemptiveScheduling;
                case EOperationPreemptionPriority::SsdRegular:
                    return EDeactivationReason::IsNotEligibleForSsdPreemptiveScheduling;
                case EOperationPreemptionPriority::Aggressive:
                    return EDeactivationReason::IsNotEligibleForAggressivelyPreemptiveScheduling;
                case EOperationPreemptionPriority::SsdAggressive:
                    return EDeactivationReason::IsNotEligibleForSsdAggressivelyPreemptiveScheduling;
                default:
                    YT_ABORT();
            }
        }();
        OnOperationDeactivated(element, deactivationReason, /*considerInOperationCounter*/ false);
        return;
    }

    if (!element->IsAlive()) {
        OnOperationDeactivated(element, EDeactivationReason::IsNotAlive);
        return;
    }

    if (auto blockedReason = CheckBlocked(element)) {
        OnOperationDeactivated(element, *blockedReason);
        return;
    }

    if (element->Spec()->PreemptionMode == EPreemptionMode::Graceful &&
        element->GetStatus(/* atUpdate */ false) == ESchedulableStatus::Normal)
    {
        OnOperationDeactivated(element, EDeactivationReason::FairShareExceeded);
        return;
    }

    if (treeConfig->EnableSchedulingTags && !CanSchedule(StaticAttributesOf(element).SchedulingTagFilterIndex)) {
        OnOperationDeactivated(element, EDeactivationReason::UnmatchedSchedulingTag);
        return;
    }

    if (!IsSchedulingSegmentCompatibleWithNode(element)) {
        OnOperationDeactivated(element, EDeactivationReason::IncompatibleSchedulingSegment);
        return;
    }

    if (element->GetTentative() &&
        element->IsSaturatedInTentativeTree(
            SchedulingContext_->GetNow(),
            element->GetTreeId(),
            treeConfig->TentativeTreeSaturationDeactivationPeriod))
    {
        OnOperationDeactivated(element, EDeactivationReason::SaturatedInTentativeTree);
        return;
    }

    // NB: we explicitly set Active flag to avoid another call to IsAlive().
    attributes.Active = true;
}

void TScheduleJobsContext::ActivateOperation(TSchedulerOperationElement* element)
{
    auto& attributes = DynamicAttributesOf(element);
    YT_VERIFY(!attributes.Active);
    attributes.Active = true;
    ChildHeapMap_.UpdateChild(element->GetMutableParent(), element);
    UpdateAncestorsDynamicAttributes(element, /*deltaResourceUsage*/ TJobResources(), /*checkAncestorsActiveness*/ false);
}

void TScheduleJobsContext::DeactivateOperation(TSchedulerOperationElement* element, EDeactivationReason reason)
{
    auto& attributes = DynamicAttributesOf(element);
    YT_VERIFY(attributes.Active);
    attributes.Active = false;
    ChildHeapMap_.UpdateChild(element->GetMutableParent(), element);
    UpdateAncestorsDynamicAttributes(element, /*deltaResourceUsage*/ TJobResources());
    OnOperationDeactivated(element, reason, /*considerInOperationCounter*/ true);
}

void TScheduleJobsContext::OnOperationDeactivated(
    TSchedulerOperationElement* element,
    EDeactivationReason reason,
    bool considerInOperationCounter)
{
    ++StageState_->DeactivationReasons[reason];
    if (considerInOperationCounter) {
        TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->OnOperationDeactivated(SchedulingContext_, reason);
    }
}

std::optional<EDeactivationReason> TScheduleJobsContext::CheckBlocked(const TSchedulerOperationElement* element) const
{
    if (element->IsMaxConcurrentScheduleJobCallsPerNodeShardViolated(SchedulingContext_)) {
        return EDeactivationReason::MaxConcurrentScheduleJobCallsPerNodeShardViolated;
    }

    if (element->HasRecentScheduleJobFailure(SchedulingContext_->GetNow())) {
        return EDeactivationReason::RecentScheduleJobFailed;
    }

    return std::nullopt;
}

bool TScheduleJobsContext::IsSchedulingSegmentCompatibleWithNode(const TSchedulerOperationElement* element) const
{
    if (TreeSnapshot_->TreeConfig()->SchedulingSegments->Mode == ESegmentedSchedulingMode::Disabled) {
        return true;
    }

    if (!element->SchedulingSegment()) {
        return false;
    }

    auto nodeSegment = SchedulingContext_->GetSchedulingSegment();
    const auto& nodeModule = TNodeSchedulingSegmentManager::GetNodeModule(
        SchedulingContext_->GetNodeDescriptor(),
        TreeSnapshot_->TreeConfig()->SchedulingSegments->ModuleType);
    if (IsModuleAwareSchedulingSegment(*element->SchedulingSegment())) {
        if (!element->PersistentAttributes().SchedulingSegmentModule) {
            // We have not decided on the operation's module yet.
            return false;
        }

        return element->SchedulingSegment() == nodeSegment &&
            element->PersistentAttributes().SchedulingSegmentModule == nodeModule;
    }

    YT_VERIFY(!element->PersistentAttributes().SchedulingSegmentModule);

    return *element->SchedulingSegment() == nodeSegment;
}

bool TScheduleJobsContext::IsUsageOutdated(TSchedulerOperationElement* element) const
{
    auto now = SchedulingContext_->GetNow();
    auto updateTime = DynamicAttributesOf(element).ResourceUsageUpdateTime;
    return updateTime + DurationToCpuDuration(TreeSnapshot_->TreeConfig()->AllowedResourceUsageStaleness) < now;
}

void TScheduleJobsContext::UpdateCurrentResourceUsage(TSchedulerOperationElement* element)
{
    auto resourceUsageBeforeUpdate = GetCurrentResourceUsage(element);
    DynamicAttributesList_.ActualizeResourceUsageOfOperation(element, TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element));
    DynamicAttributesOf(element).ResourceUsageUpdateTime = SchedulingContext_->GetNow();

    UpdateDynamicAttributes(element, /*checkLiveness*/ true);
    auto resourceUsageAfterUpdate = GetCurrentResourceUsage(element);
    auto resourceUsageDelta = resourceUsageAfterUpdate - resourceUsageBeforeUpdate;

    ChildHeapMap_.UpdateChild(element->GetMutableParent(), element);
    UpdateAncestorsDynamicAttributes(element, resourceUsageDelta);
}

bool TScheduleJobsContext::HasJobsSatisfyingResourceLimits(const TSchedulerOperationElement* element) const
{
    for (const auto& jobResources : element->DetailedMinNeededJobResources()) {
        if (SchedulingContext_->CanStartJobForOperation(jobResources, element->GetOperationId())) {
            return true;
        }
    }
    return false;
}

void TScheduleJobsContext::UpdateAncestorsDynamicAttributes(
    TSchedulerOperationElement* element,
    const TJobResources& resourceUsageDelta,
    bool checkAncestorsActiveness)
{
    auto* ancestor = element->GetMutableParent();
    while (ancestor) {
        bool activeBefore = DynamicAttributesOf(ancestor).Active;
        if (checkAncestorsActiveness) {
            YT_VERIFY(activeBefore);
        }

        DynamicAttributesOf(ancestor).ResourceUsage += resourceUsageDelta;

        UpdateDynamicAttributes(ancestor, /*checkLiveness*/ true);

        bool activeAfter = DynamicAttributesOf(ancestor).Active;
        if (activeBefore && !activeAfter) {
            ++StageState_->DeactivationReasons[EDeactivationReason::NoBestLeafDescendant];
        }

        if (ancestor->GetMutableParent()) {
            ChildHeapMap_.UpdateChild(ancestor->GetMutableParent(), ancestor);
        }

        ancestor = ancestor->GetMutableParent();
    }
}

TFairShareStrategyPackingConfigPtr TScheduleJobsContext::GetPackingConfig() const
{
    return TreeSnapshot_->TreeConfig()->Packing;
}

bool TScheduleJobsContext::CheckPacking(const TSchedulerOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot) const
{
    // NB: We expect DetailedMinNeededResources_ to be of size 1 most of the time.
    TJobResourcesWithQuota packingJobResourcesWithQuota;
    if (element->DetailedMinNeededJobResources().empty()) {
        // Refuse packing if no information about resource requirements is provided.
        return false;
    } else if (element->DetailedMinNeededJobResources().size() == 1) {
        packingJobResourcesWithQuota = element->DetailedMinNeededJobResources()[0];
    } else {
        auto idx = RandomNumber<ui32>(static_cast<ui32>(element->DetailedMinNeededJobResources().size()));
        packingJobResourcesWithQuota = element->DetailedMinNeededJobResources()[idx];
    }

    return TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->CheckPacking(
        element,
        heartbeatSnapshot,
        packingJobResourcesWithQuota,
        TreeSnapshot_->RootElement()->GetTotalResourceLimits(),
        GetPackingConfig());
}

void TScheduleJobsContext::RecordPackingHeartbeat(const TSchedulerOperationElement* element, const TPackingHeartbeatSnapshot& heartbeatSnapshot)
{
    TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->RecordPackingHeartbeat(heartbeatSnapshot, GetPackingConfig());
}

bool TScheduleJobsContext::IsJobKnown(const TSchedulerOperationElement* element, TJobId jobId) const
{
    return TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->IsJobKnown(jobId);
}

bool TScheduleJobsContext::IsOperationEnabled(const TSchedulerOperationElement* element) const
{
    // NB(eshcherbin): Operation may have been disabled since last fair share update.
    return TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->IsEnabled();
}

void TScheduleJobsContext::OnMinNeededResourcesUnsatisfied(
    const TSchedulerOperationElement* element,
    const TJobResources& availableResources,
    const TJobResources& minNeededResources) const
{
    TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->OnMinNeededResourcesUnsatisfied(
        SchedulingContext_,
        availableResources,
        minNeededResources);
}

void TScheduleJobsContext::UpdateOperationPreemptionStatusStatistics(
    const TSchedulerOperationElement* element,
    EOperationPreemptionStatus status) const
{
    TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->UpdatePreemptionStatusStatistics(status);
}

int TScheduleJobsContext::GetOperationRunningJobCount(const TSchedulerOperationElement* element) const
{
    return TreeSnapshot_->SchedulingSnapshot()->GetEnabledOperationSharedState(element)->GetRunningJobCount();
}

bool TScheduleJobsContext::CanSchedule(int schedulingTagFilterIndex) const
{
    return schedulingTagFilterIndex == EmptySchedulingTagFilterIndex ||
        CanSchedule_[schedulingTagFilterIndex];
}

EJobSchedulingStage TScheduleJobsContext::GetStageType() const
{
    return StageState_->SchedulingStage->Type;
}

void TScheduleJobsContext::ProfileAndLogStatisticsOfStage()
{
    YT_VERIFY(StageState_);

    StageState_->TotalDuration = StageState_->Timer.GetElapsedTime();

    ProfileStageStatistics();

    if (StageState_->ScheduleJobAttemptCount > 0 && EnableSchedulingInfoLogging_) {
        LogStageStatistics();
    }
}

void TScheduleJobsContext::ProfileStageStatistics()
{
    if (!Initialized_) {
        return;
    }

    YT_VERIFY(StageState_);

    auto* profilingCounters = &StageState_->SchedulingStage->ProfilingCounters;

    profilingCounters->PrescheduleJobTime.Record(StageState_->PrescheduleDuration);
    profilingCounters->CumulativePrescheduleJobTime.Add(StageState_->PrescheduleDuration);

    if (StageState_->PrescheduleExecuted) {
        profilingCounters->PrescheduleJobCount.Increment();
        if (StageState_->ScheduleJobAttemptCount == 0) {
            profilingCounters->UselessPrescheduleJobCount.Increment();
        }
    }

    auto strategyScheduleJobDuration = StageState_->TotalDuration
        - StageState_->PrescheduleDuration
        - StageState_->TotalScheduleJobDuration;
    profilingCounters->StrategyScheduleJobTime.Record(strategyScheduleJobDuration);
    profilingCounters->CumulativeStrategyScheduleJobTime.Add(strategyScheduleJobDuration);

    profilingCounters->TotalControllerScheduleJobTime.Record(StageState_->TotalScheduleJobDuration);
    profilingCounters->CumulativeTotalControllerScheduleJobTime.Add(StageState_->TotalScheduleJobDuration);
    profilingCounters->ExecControllerScheduleJobTime.Record(StageState_->ExecScheduleJobDuration);
    profilingCounters->CumulativeExecControllerScheduleJobTime.Add(StageState_->ExecScheduleJobDuration);
    profilingCounters->PackingRecordHeartbeatTime.Record(StageState_->PackingRecordHeartbeatDuration);
    profilingCounters->PackingCheckTime.Record(StageState_->PackingCheckDuration);
    profilingCounters->AnalyzeJobsTime.Record(StageState_->AnalyzeJobsDuration);
    profilingCounters->CumulativeAnalyzeJobsTime.Add(StageState_->AnalyzeJobsDuration);

    profilingCounters->ScheduleJobAttemptCount.Increment(StageState_->ScheduleJobAttemptCount);
    profilingCounters->ScheduleJobFailureCount.Increment(StageState_->ScheduleJobFailureCount);
    profilingCounters->ControllerScheduleJobCount.Increment(SchedulingStatistics().ControllerScheduleJobCount);
    profilingCounters->ControllerScheduleJobTimedOutCount.Increment(SchedulingStatistics().ControllerScheduleJobTimedOutCount);

    for (auto reason : TEnumTraits<EScheduleJobFailReason>::GetDomainValues()) {
        profilingCounters->ControllerScheduleJobFail[reason].Increment(StageState_->FailedScheduleJob[reason]);
    }
    for (auto reason : TEnumTraits<EDeactivationReason>::GetDomainValues()) {
        profilingCounters->DeactivationCount[reason].Increment(StageState_->DeactivationReasons[reason]);
    }

    for (auto [schedulingIndex, count] : StageState_->SchedulingIndexToScheduleJobAttemptCount) {
        int rangeIndex = SchedulingIndexToProfilingRangeIndex(schedulingIndex);
        profilingCounters->SchedulingIndexCounters[rangeIndex].Increment(count);
    }
    if (StageState_->MaxSchedulingIndex >= 0) {
        profilingCounters->MaxSchedulingIndexCounters[SchedulingIndexToProfilingRangeIndex(StageState_->MaxSchedulingIndex)].Increment();
    }
}

void TScheduleJobsContext::LogStageStatistics()
{
    if (!Initialized_) {
        return;
    }

    YT_VERIFY(StageState_);

    YT_LOG_DEBUG(
        "Scheduling statistics (SchedulingStage: %v, ActiveTreeSize: %v, ActiveOperationCount: %v, TotalHeapElementCount: %v, "
        "DeactivationReasons: %v, CanStartMoreJobs: %v, Address: %v, SchedulingSegment: %v, MaxSchedulingIndex: %v)",
        StageState_->SchedulingStage->Type,
        StageState_->ActiveTreeSize,
        StageState_->ActiveOperationCount,
        StageState_->TotalHeapElementCount,
        StageState_->DeactivationReasons,
        SchedulingContext_->CanStartMoreJobs(),
        SchedulingContext_->GetNodeDescriptor().Address,
        SchedulingContext_->GetSchedulingSegment(),
        StageState_->MaxSchedulingIndex);
}

EJobPreemptionLevel TScheduleJobsContext::GetJobPreemptionLevel(const TJobWithPreemptionInfo& jobWithPreemptionInfo) const
{
    const auto& [job, preemptionStatus, operationElement] = jobWithPreemptionInfo;

    bool isEligibleForSsdPriorityPreemption = SsdPriorityPreemptionEnabled_ &&
        IsEligibleForSsdPriorityPreemption(GetDiskQuotaMedia(job->DiskQuota()));
    auto aggressivePreemptionAllowed = StaticAttributesOf(operationElement).EffectiveAggressivePreemptionAllowed;
    switch (preemptionStatus) {
        case EJobPreemptionStatus::NonPreemptable:
            return isEligibleForSsdPriorityPreemption
                ? EJobPreemptionLevel::SsdNonPreemptable
                : EJobPreemptionLevel::NonPreemptable;
        case EJobPreemptionStatus::AggressivelyPreemptable:
            if (aggressivePreemptionAllowed) {
                return isEligibleForSsdPriorityPreemption
                    ? EJobPreemptionLevel::SsdAggressivelyPreemptable
                    : EJobPreemptionLevel::AggressivelyPreemptable;
            } else {
                return isEligibleForSsdPriorityPreemption
                    ? EJobPreemptionLevel::SsdNonPreemptable
                    : EJobPreemptionLevel::NonPreemptable;
            }
        case EJobPreemptionStatus::Preemptable:
            return EJobPreemptionLevel::Preemptable;
        default:
            YT_ABORT();
    }
}

bool TScheduleJobsContext::IsEligibleForSsdPriorityPreemption(const THashSet<int>& diskRequestMedia) const
{
    for (const auto& medium : diskRequestMedia) {
        if (SsdPriorityPreemptionMedia_.contains(medium)) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TFairShareTreeJobScheduler::TFairShareTreeJobScheduler(
    TString treeId,
    NLogging::TLogger logger,
    ISchedulerStrategyHost* strategyHost,
    TFairShareStrategyTreeConfigPtr config,
    NProfiling::TProfiler profiler)
    : TreeId_(std::move(treeId))
    , Logger(std::move(logger))
    , StrategyHost_(strategyHost)
    , Config_(std::move(config))
    , Profiler_(std::move(profiler))
    , CumulativeScheduleJobsTime_(Profiler_.TimeCounter("/cumulative_schedule_jobs_time"))
    , ScheduleJobsDeadlineReachedCounter_(Profiler_.Counter("/schedule_jobs_deadline_reached"))
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

    TScheduleJobsContext context(
        schedulingContext,
        treeSnapshot,
        treeSnapshot->SchedulingSnapshot()->KnownSchedulingTagFilters(),
        enableSchedulingInfoLogging,
        StrategyHost_,
        Logger);

    context.SchedulingStatistics().ResourceUsage = schedulingContext->ResourceUsage();
    context.SchedulingStatistics().ResourceLimits = schedulingContext->ResourceLimits();

    if (config->EnableResourceUsageSnapshot) {
        if (auto snapshot = treeSnapshot->SchedulingSnapshot()->GetDynamicAttributesListSnapshot()) {
            YT_LOG_DEBUG_IF(enableSchedulingInfoLogging, "Using dynamic attributes snapshot for job scheduling");
            context.SetDynamicAttributesListSnapshot(std::move(snapshot));
        }
    }

    bool needPackingFallback;
    {
        context.StartStage(&SchedulingStages_[EJobSchedulingStage::NonPreemptive]);
        ScheduleJobsWithoutPreemption(treeSnapshot, &context, now);
        needPackingFallback = schedulingContext->StartedJobs().empty() && context.HasBadPackingOperations();
        context.ReactivateBadPackingOperations();
        context.SchedulingStatistics().MaxNonPreemptiveSchedulingIndex = context.GetStageMaxSchedulingIndex();
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
        context.SsdPriorityPreemptionMedia() = treeSnapshot->SchedulingSnapshot()->SsdPriorityPreemptionMedia();
        context.SchedulingStatistics().SsdPriorityPreemptionEnabled = context.GetSsdPriorityPreemptionEnabled();
        context.SchedulingStatistics().SsdPriorityPreemptionMedia = context.SsdPriorityPreemptionMedia();

        context.CountOperationsByPreemptionPriority();

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
        context.AbortJobsSinceResourcesOvercommit();
    }

    schedulingContext->SetSchedulingStatistics(context.SchedulingStatistics());

    CumulativeScheduleJobsTime_.Add(scheduleJobsTimer.GetElapsedTime());
}

void TFairShareTreeJobScheduler::PreemptJobsGracefully(
    const ISchedulingContextPtr& schedulingContext,
    const TFairShareTreeSnapshotPtr& treeSnapshot) const
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

void TFairShareTreeJobScheduler::RegisterOperation(const TSchedulerOperationElement* element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto operationId = element->GetOperationId();
    EmplaceOrCrash(
        OperationIdToSharedState_,
        operationId,
        New<TFairShareTreeJobSchedulerOperationSharedState>(
            StrategyHost_,
            element->Spec()->UpdatePreemptableJobsListLoggingPeriod,
            Logger.WithTag("OperationId: %v", operationId)));
}

void TFairShareTreeJobScheduler::UnregisterOperation(const TSchedulerOperationElement* element)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    EraseOrCrash(OperationIdToSharedState_, element->GetOperationId());
}

void TFairShareTreeJobScheduler::EnableOperation(const TSchedulerOperationElement* element) const
{
    return GetOperationSharedState(element->GetOperationId())->Enable();
}

void TFairShareTreeJobScheduler::DisableOperation(TSchedulerOperationElement* element, bool markAsNonAlive) const
{
    GetOperationSharedState(element->GetOperationId())->Disable();
    element->ReleaseResources(markAsNonAlive);
}

void TFairShareTreeJobScheduler::OnOperationStarvationStatusChanged(
    TOperationId operationId,
    EStarvationStatus oldStatus,
    EStarvationStatus newStatus) const
{
    if (oldStatus == EStarvationStatus::NonStarving && newStatus != EStarvationStatus::NonStarving) {
        GetOperationSharedState(operationId)->ResetDeactivationReasonsFromLastNonStarvingTime();
    }
}

void TFairShareTreeJobScheduler::RegisterJobsFromRevivedOperation(TSchedulerOperationElement* element, const std::vector<TJobPtr>& jobs) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto& operationSharedState = GetOperationSharedState(element->GetOperationId());
    for (const auto& job: jobs) {
        TJobResourcesWithQuota resourceUsageWithQuota = job->ResourceUsage();
        resourceUsageWithQuota.SetDiskQuota(job->DiskQuota());
        operationSharedState->OnJobStarted(
            element,
            job->GetId(),
            resourceUsageWithQuota,
            /*precommittedResources*/ {},
            // NB: |scheduleJobEpoch| is ignored in case |force| is true.
            /*scheduleJobEpoch*/ 0,
            /*force*/ true);
    }
}

void TFairShareTreeJobScheduler::ProcessUpdatedJob(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    TSchedulerOperationElement* element,
    TJobId jobId,
    const TJobResources& jobResources,
    const std::optional<TString>& jobDataCenter,
    const std::optional<TString>& jobInfinibandCluster,
    bool* shouldAbortJob) const
{
    const auto& operationSharedState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element);

    auto delta = operationSharedState->SetJobResourceUsage(jobId, jobResources);
    element->IncreaseHierarchicalResourceUsage(delta);
    operationSharedState->UpdatePreemptableJobsList(element);

    const auto& operationSchedulingSegment = element->SchedulingSegment();
    if (operationSchedulingSegment && IsModuleAwareSchedulingSegment(*operationSchedulingSegment)) {
        const auto& operationModule = element->PersistentAttributes().SchedulingSegmentModule;
        const auto& jobModule = TNodeSchedulingSegmentManager::GetNodeModule(
            jobDataCenter,
            jobInfinibandCluster,
            element->TreeConfig()->SchedulingSegments->ModuleType);
        bool jobIsRunningInTheRightModule = operationModule && (operationModule == jobModule);
        if (!jobIsRunningInTheRightModule) {
            *shouldAbortJob = true;

            YT_LOG_DEBUG(
                "Requested to abort job because it is running in a wrong module "
                "(OperationId: %v, JobId: %v, OperationModule: %v, JobModule: %v)",
                element->GetOperationId(),
                jobId,
                operationModule,
                jobModule);
        }
    }
}

void TFairShareTreeJobScheduler::ProcessFinishedJob(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    TSchedulerOperationElement* element,
    TJobId jobId) const
{
    const auto& operationSharedState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element);
    operationSharedState->OnJobFinished(element, jobId);
}

TError TFairShareTreeJobScheduler::CheckOperationIsHung(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TSchedulerOperationElement* element,
    TInstant now,
    TInstant activationTime,
    TDuration safeTimeout,
    int minScheduleJobCallAttempts,
    const THashSet<EDeactivationReason>& deactivationReasons)
{
    const auto& operationSharedState = treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element);

    int deactivationCount = 0;
    auto deactivationReasonToCount = operationSharedState->GetDeactivationReasonsFromLastNonStarvingTime();
    for (auto reason : deactivationReasons) {
        deactivationCount += deactivationReasonToCount[reason];
    }

    auto lastScheduleJobSuccessTime = operationSharedState->GetLastScheduleJobSuccessTime();
    if (activationTime + safeTimeout < now &&
        lastScheduleJobSuccessTime + safeTimeout < now &&
        element->GetLastNonStarvingTime() + safeTimeout < now &&
        operationSharedState->GetRunningJobCount() == 0 &&
        deactivationCount > minScheduleJobCallAttempts)
    {
        return TError("Operation has no successful scheduled jobs for a long period")
            << TErrorAttribute("period", safeTimeout)
            << TErrorAttribute("deactivation_count", deactivationCount)
            << TErrorAttribute("last_schedule_job_success_time", lastScheduleJobSuccessTime)
            << TErrorAttribute("last_non_starving_time", element->GetLastNonStarvingTime());
    }

    return TError();
}

void TFairShareTreeJobScheduler::BuildOperationProgress(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TSchedulerOperationElement* element,
    ISchedulerStrategyHost* const strategyHost,
    NYTree::TFluentMap fluent)
{
    bool isEnabled = treeSnapshot->IsElementEnabled(element);
    const auto& operationSharedState = isEnabled
        ? treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(element)
        : treeSnapshot->SchedulingSnapshot()->GetOperationSharedState(element);
    const auto& attributes = isEnabled
        ? treeSnapshot->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element)
        : TStaticAttributes{};
    fluent
        .Item("preemptable_job_count").Value(operationSharedState->GetPreemptableJobCount())
        .Item("aggressively_preemptable_job_count").Value(operationSharedState->GetAggressivelyPreemptableJobCount())
        .Item("scheduling_index").Value(attributes.SchedulingIndex)
        .Item("deactivation_reasons").Value(operationSharedState->GetDeactivationReasons())
        .Item("min_needed_resources_unsatisfied_count").Value(operationSharedState->GetMinNeededResourcesUnsatisfiedCount())
        .Item("disk_quota_usage").BeginMap()
            .Do([&] (TFluentMap fluent) {
                strategyHost->SerializeDiskQuota(operationSharedState->GetTotalDiskQuota(), fluent.GetConsumer());
            })
        .EndMap();
}

void TFairShareTreeJobScheduler::BuildElementYson(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TSchedulerElement* element,
    const TFieldsFilter& filter,
    TFluentMap fluent)
{
    const auto& attributes = treeSnapshot->IsElementEnabled(element)
        ? treeSnapshot->SchedulingSnapshot()->StaticAttributesList().AttributesOf(element)
        : TStaticAttributes{};
    fluent
        .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(filter, "aggressive_preemption_allowed", IsAggressivePreemptionAllowed(element))
        .ITEM_VALUE_IF_SUITABLE_FOR_FILTER(
            filter,
            "effective_aggressive_preemption_allowed",
            attributes.EffectiveAggressivePreemptionAllowed);
}

TJobSchedulerPostUpdateContext TFairShareTreeJobScheduler::CreatePostUpdateContext(TSchedulerRootElement* rootElement)
{
    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetControlInvoker(EControlQueue::FairShareStrategy));

    THashMap<TSchedulingSegmentModule, TJobResources> resourceLimitsPerModule;
    if (Config_->SchedulingSegments->Mode != ESegmentedSchedulingMode::Disabled) {
        for (const auto& schedulingSegmentModule : Config_->SchedulingSegments->GetModules()) {
            auto moduleTag = TNodeSchedulingSegmentManager::GetNodeTagFromModuleName(
                schedulingSegmentModule,
                Config_->SchedulingSegments->ModuleType);
            auto tagFilter = Config_->NodesFilter & TSchedulingTagFilter(MakeBooleanFormula(moduleTag));
            resourceLimitsPerModule[schedulingSegmentModule] = StrategyHost_->GetResourceLimits(tagFilter);
        }
    }

    return TJobSchedulerPostUpdateContext{
        .RootElement = rootElement,
        .ManageSchedulingSegmentsContext = TManageTreeSchedulingSegmentsContext{
            .TreeConfig = Config_,
            .TotalResourceLimits = rootElement->GetTotalResourceLimits(),
            .ResourceLimitsPerModule = std::move(resourceLimitsPerModule),
        },
        .OperationIdToSharedState = OperationIdToSharedState_,
    };
}

void TFairShareTreeJobScheduler::PostUpdate(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TJobSchedulerPostUpdateContext* postUpdateContext)
{
    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetFairShareUpdateInvoker());

    InitializeStaticAttributes(fairSharePostUpdateContext, postUpdateContext);

    PublishFairShareAndUpdatePreemptionAttributes(postUpdateContext->RootElement, postUpdateContext);

    auto cachedJobPreemptionStatusesUpdateDeadline =
        CachedJobPreemptionStatuses_.UpdateTime + fairSharePostUpdateContext->TreeConfig->CachedJobPreemptionStatusesUpdatePeriod;
    if (fairSharePostUpdateContext->Now > cachedJobPreemptionStatusesUpdateDeadline) {
        UpdateCachedJobPreemptionStatuses(fairSharePostUpdateContext, postUpdateContext);
    }

    TDynamicAttributesList dynamicAttributesList(postUpdateContext->RootElement->GetTreeSize());
    TChildHeapMap childHeapMap;
    ComputeDynamicAttributesAtUpdateRecursively(postUpdateContext->RootElement, &dynamicAttributesList, &childHeapMap);
    BuildSchedulableIndices(&dynamicAttributesList, &childHeapMap, postUpdateContext);

    ManageSchedulingSegments(fairSharePostUpdateContext, &postUpdateContext->ManageSchedulingSegmentsContext);

    CollectKnownSchedulingTagFilters(fairSharePostUpdateContext, postUpdateContext);
}

TFairShareTreeSchedulingSnapshotPtr TFairShareTreeJobScheduler::CreateSchedulingSnapshot(TJobSchedulerPostUpdateContext* postUpdateContext)
{
    VERIFY_INVOKER_AFFINITY(StrategyHost_->GetControlInvoker(EControlQueue::FairShareStrategy));

    // NB(eshcherbin): We cannot update SSD media in the constructor, because initial pool trees update
    // in the registration pipeline is done before medium directory sync. That's why we do the initial update
    // during the first fair share update.
    if (!SsdPriorityPreemptionMedia_) {
        UpdateSsdPriorityPreemptionMedia();
    }

    return New<TFairShareTreeSchedulingSnapshot>(
        std::move(postUpdateContext->StaticAttributesList),
        SsdPriorityPreemptionMedia_.value_or(THashSet<int>()),
        CachedJobPreemptionStatuses_,
        std::move(postUpdateContext->ManageSchedulingSegmentsContext.SchedulingSegmentsState),
        std::move(postUpdateContext->KnownSchedulingTagFilters),
        std::move(postUpdateContext->OperationIdToSharedState));
}

void TFairShareTreeJobScheduler::OnResourceUsageSnapshotUpdate(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TResourceUsageSnapshotPtr& resourceUsageSnapshot) const
{
    treeSnapshot->SchedulingSnapshot()->UpdateDynamicAttributesSnapshot(treeSnapshot, resourceUsageSnapshot);
}

void TFairShareTreeJobScheduler::UpdateConfig(TFairShareStrategyTreeConfigPtr config)
{
    Config_ = std::move(config);
    UpdateSsdPriorityPreemptionMedia();
}

void TFairShareTreeJobScheduler::BuildElementLoggingStringAttributes(
    const TFairShareTreeSnapshotPtr& treeSnapshot,
    const TSchedulerElement* element,
    TDelimitedStringBuilderWrapper& delimitedBuilder) const
{
    if (element->GetType() == ESchedulerElementType::Operation) {
        const auto* operationElement = static_cast<const TSchedulerOperationElement*>(element);
        const auto& operationSharedState = treeSnapshot->IsElementEnabled(operationElement)
            ? treeSnapshot->SchedulingSnapshot()->GetEnabledOperationSharedState(operationElement)
            : treeSnapshot->SchedulingSnapshot()->GetOperationSharedState(operationElement);
        delimitedBuilder->AppendFormat("PreemptableRunningJobs: %v", operationSharedState->GetPreemptableJobCount());
        delimitedBuilder->AppendFormat("AggressivelyPreemptableRunningJobs: %v", operationSharedState->GetAggressivelyPreemptableJobCount());
        delimitedBuilder->AppendFormat("PreemptionStatusStatistics: %v", operationSharedState->GetPreemptionStatusStatistics());
        delimitedBuilder->AppendFormat("DeactivationReasons: %v", operationSharedState->GetDeactivationReasons());
        delimitedBuilder->AppendFormat("MinNeededResourcesUnsatisfiedCount: %v", operationSharedState->GetMinNeededResourcesUnsatisfiedCount());
    }
}

void TFairShareTreeJobScheduler::OnJobStartedInTest(
    TSchedulerOperationElement* element,
    TJobId jobId,
    const TJobResourcesWithQuota& resourceUsage)
{
    const auto& operationSharedState = GetOperationSharedState(element->GetOperationId());
    operationSharedState->OnJobStarted(
        element,
        jobId,
        resourceUsage,
        /*precommitedResources*/ {},
        /*scheduleJobEpoch*/ 0);
}

void TFairShareTreeJobScheduler::ProcessUpdatedJobInTest(
    TSchedulerOperationElement* element,
    TJobId jobId,
    const TJobResources& jobResources)
{
    const auto& operationSharedState = GetOperationSharedState(element->GetOperationId());
    auto delta = operationSharedState->SetJobResourceUsage(jobId, jobResources);
    element->IncreaseHierarchicalResourceUsage(delta);
    operationSharedState->UpdatePreemptableJobsList(element);
}

EJobPreemptionStatus TFairShareTreeJobScheduler::GetJobPreemptionStatusInTest(const TSchedulerOperationElement* element, TJobId jobId) const
{
    const auto& operationSharedState = GetOperationSharedState(element->GetOperationId());
    return operationSharedState->GetJobPreemptionStatus(jobId);
}

void TFairShareTreeJobScheduler::InitSchedulingStages()
{
    for (auto stage : TEnumTraits<EJobSchedulingStage>::GetDomainValues()) {
        SchedulingStages_[stage] = TScheduleJobsStage{
            .Type = stage,
            .ProfilingCounters = TScheduleJobsProfilingCounters(Profiler_.WithTag("scheduling_stage", FormatEnum(stage))),
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
    const auto& controllerConfig = treeSnapshot->ControllerConfig();

    {
        TCpuInstant schedulingDeadline = startTime + DurationToCpuDuration(controllerConfig->ScheduleJobsTimeout);

        while (context->SchedulingContext()->CanStartMoreJobs() && context->SchedulingContext()->GetNow() < schedulingDeadline)
        {
            if (!context->GetStagePrescheduleExecuted()) {
                context->PrepareForScheduling();
                context->PrescheduleJob();
            }
            auto scheduleJobResult = context->ScheduleJob(ignorePacking);
            if (scheduleJobResult.Scheduled) {
                context->ReactivateBadPackingOperations();
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
    bool shouldAttemptScheduling = context->GetOperationWithPreemptionPriorityCount(targetOperationPreemptionPriority) > 0;
    bool shouldAttemptPreemption = forcePreemptionAttempt || shouldAttemptScheduling;
    if (!shouldAttemptPreemption) {
        return;
    }

    // NB: This method achieves 2 goals relevant for scheduling with preemption:
    // 1. Reset |Active| attribute after scheduling without preemption (this is necessary for PrescheduleJob correctness).
    // 2. Initialize dynamic attributes and calculate local resource usages if scheduling without preemption was skipped.
    context->PrepareForScheduling();

    std::vector<TJobWithPreemptionInfo> unconditionallyPreemptableJobs;
    TNonOwningJobSet forcefullyPreemptableJobs;
    context->AnalyzePreemptableJobs(
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

        const auto& controllerConfig = treeSnapshot->ControllerConfig();
        TCpuInstant schedulingDeadline = startTime + DurationToCpuDuration(controllerConfig->ScheduleJobsTimeout);

        while (context->SchedulingContext()->CanStartMoreJobs() && context->SchedulingContext()->GetNow() < schedulingDeadline)
        {
            if (!context->GetStagePrescheduleExecuted()) {
                context->PrescheduleJob(targetOperationPreemptionPriority);
            }

            auto scheduleJobResult = context->ScheduleJob(/*ignorePacking*/ true);
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

    context->PreemptJobsAfterScheduling(
        targetOperationPreemptionPriority,
        std::move(unconditionallyPreemptableJobs),
        forcefullyPreemptableJobs,
        jobStartedUsingPreemption);
}

const TFairShareTreeJobSchedulerOperationSharedStatePtr& TFairShareTreeJobScheduler::GetOperationSharedState(TOperationId operationId) const
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return GetOrCrash(OperationIdToSharedState_, operationId);
}

void TFairShareTreeJobScheduler::UpdateSsdPriorityPreemptionMedia()
{
    THashSet<int> media;
    std::vector<TString> unknownNames;
    for (const auto& mediumName : Config_->SsdPriorityPreemption->MediumNames) {
        if (auto mediumIndex = StrategyHost_->FindMediumIndexByName(mediumName)) {
            media.insert(*mediumIndex);
        } else {
            unknownNames.push_back(mediumName);
        }
    }

    if (unknownNames.empty()) {
        if (SsdPriorityPreemptionMedia_ != media) {
            YT_LOG_INFO("Updated SSD priority preemption media (OldSsdPriorityPreemptionMedia: %v, NewSsdPriorityPreemptionMedia: %v)",
                SsdPriorityPreemptionMedia_,
                media);

            SsdPriorityPreemptionMedia_.emplace(std::move(media));

            StrategyHost_->SetSchedulerAlert(ESchedulerAlertType::UpdateSsdPriorityPreemptionMedia, TError());
        }
    } else {
        auto error = TError("Config contains unknown SSD priority preemption media")
            << TErrorAttribute("unknown_medium_names", std::move(unknownNames));
        StrategyHost_->SetSchedulerAlert(ESchedulerAlertType::UpdateSsdPriorityPreemptionMedia, error);
    }
}

void TFairShareTreeJobScheduler::InitializeStaticAttributes(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    postUpdateContext->StaticAttributesList.resize(postUpdateContext->RootElement->GetTreeSize());

    for (const auto& [operationId, operationElement] : fairSharePostUpdateContext->EnabledOperationIdToElement) {
        auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(operationElement);
        attributes.OperationSharedState = GetOrCrash(postUpdateContext->OperationIdToSharedState, operationId);
    }
}

void TFairShareTreeJobScheduler::PublishFairShareAndUpdatePreemptionAttributes(
    TSchedulerElement* element,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    element->PublishFairShare(element->Attributes().FairShare.Total);

    auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(element);
    auto isAggressivePreemptionAllowed = IsAggressivePreemptionAllowed(element);
    if (element->IsRoot()) {
        YT_VERIFY(isAggressivePreemptionAllowed);
        attributes.EffectiveAggressivePreemptionAllowed = *isAggressivePreemptionAllowed;
    } else {
        const auto* parent = element->GetParent();
        YT_VERIFY(parent);
        const auto& parentAttributes = postUpdateContext->StaticAttributesList.AttributesOf(parent);

        attributes.EffectiveAggressivePreemptionAllowed = isAggressivePreemptionAllowed
            .value_or(parentAttributes.EffectiveAggressivePreemptionAllowed);
    }

    switch (element->GetType()) {
        case ESchedulerElementType::Pool:
        case ESchedulerElementType::Root:
            PublishFairShareAndUpdatePreemptionAttributesAtCompositeElement(static_cast<TSchedulerCompositeElement*>(element), postUpdateContext);
            break;
        case ESchedulerElementType::Operation:
            PublishFairShareAndUpdatePreemptionAttributesAtOperation(static_cast<TSchedulerOperationElement*>(element), postUpdateContext);
            break;
        default:
            YT_ABORT();
    }
}

void TFairShareTreeJobScheduler::PublishFairShareAndUpdatePreemptionAttributesAtCompositeElement(
    TSchedulerCompositeElement* element,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    for (const auto& child : element->EnabledChildren()) {
        PublishFairShareAndUpdatePreemptionAttributes(child.Get(), postUpdateContext);
    }
}

void TFairShareTreeJobScheduler::PublishFairShareAndUpdatePreemptionAttributesAtOperation(
    TSchedulerOperationElement* element,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    // If fair share ratio equals demand ratio then we want to explicitly disable preemption.
    // It is necessary since some job's resource usage may increase before the next fair share update,
    // and in this case we don't want any jobs to become preemptable
    bool isDominantFairShareEqualToDominantDemandShare =
        TResourceVector::Near(element->Attributes().FairShare.Total, element->Attributes().DemandShare, NVectorHdrf::RatioComparisonPrecision) &&
            !Dominates(TResourceVector::Epsilon(), element->Attributes().DemandShare);
    bool currentPreemptableValue = !isDominantFairShareEqualToDominantDemandShare;

    const auto& operationSharedState = postUpdateContext->StaticAttributesList.AttributesOf(element).OperationSharedState;
    operationSharedState->SetPreemptable(currentPreemptableValue);
    operationSharedState->UpdatePreemptableJobsList(element);
}

void TFairShareTreeJobScheduler::UpdateCachedJobPreemptionStatuses(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TJobSchedulerPostUpdateContext* postUpdateContext)
{
    auto jobPreemptionStatuses = New<TRefCountedJobPreemptionStatusMapPerOperation>();
    auto collectJobPreemptionStatuses = [&] (const auto& operationMap) {
        for (const auto& [operationId, operationElement] : operationMap) {
            // NB: We cannot use operation shared state from static attributes list, because disabled operations don't have a tree index.
            EmplaceOrCrash(
                *jobPreemptionStatuses,
                operationId,
                GetOrCrash(postUpdateContext->OperationIdToSharedState, operationId)->GetJobPreemptionStatusMap());
        }
    };

    collectJobPreemptionStatuses(fairSharePostUpdateContext->EnabledOperationIdToElement);
    collectJobPreemptionStatuses(fairSharePostUpdateContext->DisabledOperationIdToElement);

    CachedJobPreemptionStatuses_ = TCachedJobPreemptionStatuses{
        .Value = std::move(jobPreemptionStatuses),
        .UpdateTime = fairSharePostUpdateContext->Now,
    };
}

void TFairShareTreeJobScheduler::ComputeDynamicAttributesAtUpdateRecursively(
    TSchedulerElement* element,
    TDynamicAttributesList* dynamicAttributesList,
    TChildHeapMap* childHeapMap) const
{
    auto updateDynamicAttributes = [&] {
        dynamicAttributesList->UpdateAttributes(element, /*schedulingSnapshot*/ {}, /*checkLiveness*/ false, childHeapMap);
    };

    if (element->IsOperation()) {
        updateDynamicAttributes();
    } else {
        const auto* compositeElement = static_cast<const TSchedulerCompositeElement*>(element);
        for (const auto& child: compositeElement->SchedulableChildren()) {
            ComputeDynamicAttributesAtUpdateRecursively(
                child.Get(),
                dynamicAttributesList,
                childHeapMap);
        }

        updateDynamicAttributes();

        EmplaceOrCrash(
            *childHeapMap,
            element->GetTreeIndex(),
            TChildHeap(compositeElement, dynamicAttributesList));
    }
}

void TFairShareTreeJobScheduler::BuildSchedulableIndices(
    TDynamicAttributesList* dynamicAttributesList,
    TChildHeapMap* childHeapMap,
    TJobSchedulerPostUpdateContext* context) const
{
    auto& dynamicAttributes = dynamicAttributesList->AttributesOf(context->RootElement);

    int schedulingIndex = 0;
    TSchedulerOperationElement* bestLeafDescendant = nullptr;
    while (true) {
        if (!dynamicAttributes.Active) {
            break;
        }

        bestLeafDescendant = dynamicAttributes.BestLeafDescendant;
        auto& bestLeafDescendantAttributes = context->StaticAttributesList.AttributesOf(bestLeafDescendant);
        bestLeafDescendantAttributes.SchedulingIndex = schedulingIndex++;

        dynamicAttributesList->AttributesOf(bestLeafDescendant).Active = false;

        TSchedulerElement* current = bestLeafDescendant;
        while (auto* parent = current->GetMutableParent()) {
            childHeapMap->UpdateChild(parent, current);
            // NB(eshcherbin): Scheduling snapshot is needed only for liveness check.
            dynamicAttributesList->UpdateAttributes(parent, /*schedulingSnapshot*/ nullptr, /*checkLiveness*/ false, childHeapMap);
            current = parent;
        }
    }
}

void TFairShareTreeJobScheduler::ManageSchedulingSegments(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TManageTreeSchedulingSegmentsContext* manageSegmentsContext) const
{
    auto mode = manageSegmentsContext->TreeConfig->SchedulingSegments->Mode;
    if (mode != ESegmentedSchedulingMode::Disabled) {
        for (const auto& [_, operationElement] : fairSharePostUpdateContext->EnabledOperationIdToElement) {
            EmplaceOrCrash(
                manageSegmentsContext->Operations,
                operationElement->GetOperationId(),
                TOperationSchedulingSegmentContext{
                    .ResourceDemand = operationElement->ResourceDemand(),
                    .ResourceUsage = operationElement->ResourceUsageAtUpdate(),
                    .DemandShare = operationElement->Attributes().DemandShare,
                    .FairShare = operationElement->Attributes().FairShare.Total,
                    .Segment = operationElement->SchedulingSegment(),
                    .Module = operationElement->PersistentAttributes().SchedulingSegmentModule,
                    .SpecifiedModules = operationElement->SpecifiedSchedulingSegmentModules(),
                    .FailingToScheduleAtModuleSince = operationElement->PersistentAttributes().FailingToScheduleAtModuleSince,
                });
        }
    }

    TStrategySchedulingSegmentManager::ManageSegmentsInTree(manageSegmentsContext, TreeId_);

    if (mode != ESegmentedSchedulingMode::Disabled) {
        for (const auto& [_, operationElement] : fairSharePostUpdateContext->EnabledOperationIdToElement) {
            const auto& operationContext = GetOrCrash(manageSegmentsContext->Operations, operationElement->GetOperationId());
            operationElement->PersistentAttributes().SchedulingSegmentModule = operationContext.Module;
            operationElement->PersistentAttributes().FailingToScheduleAtModuleSince = operationContext.FailingToScheduleAtModuleSince;
        }
    }
}

void TFairShareTreeJobScheduler::CollectKnownSchedulingTagFilters(
    TFairSharePostUpdateContext* fairSharePostUpdateContext,
    TJobSchedulerPostUpdateContext* postUpdateContext) const
{
    THashMap<TSchedulingTagFilter, int> schedulingTagFilterToIndex;
    auto getTagFilterIndex = [&] (const TSchedulingTagFilter& filter) {
        if (filter.IsEmpty()) {
            return EmptySchedulingTagFilterIndex;
        }

        auto it = schedulingTagFilterToIndex.find(filter);
        if (it != schedulingTagFilterToIndex.end()) {
            return it->second;
        }

        int index = std::ssize(postUpdateContext->KnownSchedulingTagFilters);
        EmplaceOrCrash(schedulingTagFilterToIndex, filter, index);
        postUpdateContext->KnownSchedulingTagFilters.push_back(filter);
        return index;
    };

    for (const auto& [_, operationElement] : fairSharePostUpdateContext->EnabledOperationIdToElement) {
        auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(operationElement);
        attributes.SchedulingTagFilterIndex = getTagFilterIndex(operationElement->GetSchedulingTagFilter());
    }
    for (const auto& [_, poolElement] : fairSharePostUpdateContext->PoolNameToElement) {
        auto& attributes = postUpdateContext->StaticAttributesList.AttributesOf(poolElement);
        attributes.SchedulingTagFilterIndex = getTagFilterIndex(poolElement->GetSchedulingTagFilter());
    }
}

std::optional<bool> TFairShareTreeJobScheduler::IsAggressivePreemptionAllowed(const TSchedulerElement* element)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Root:
            return true;
        case ESchedulerElementType::Pool:
            return static_cast<const TSchedulerPoolElement*>(element)->GetConfig()->AllowAggressivePreemption;
        case ESchedulerElementType::Operation: {
            const auto* operationElement = static_cast<const TSchedulerOperationElement*>(element);
            if (operationElement->IsGang() && !operationElement->TreeConfig()->AllowAggressivePreemptionForGangOperations) {
                return false;
            }
            return {};
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NScheduler
