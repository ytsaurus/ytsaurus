#include "structs.h"

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NScheduler {

using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

TJobStartDescriptor::TJobStartDescriptor(
    TJobId id,
    const TJobResourcesWithQuota& resourceLimits,
    bool interruptible)
    : Id(id)
    , ResourceLimits(resourceLimits)
    , Interruptible(interruptible)
{ }

////////////////////////////////////////////////////////////////////////////////

void TControllerScheduleJobResult::RecordFail(EScheduleJobFailReason reason)
{
    ++Failed[reason];
}

bool TControllerScheduleJobResult::IsBackoffNeeded() const
{
    return
        !StartDescriptor &&
        Failed[EScheduleJobFailReason::NotEnoughResources] == 0 &&
        Failed[EScheduleJobFailReason::NoLocalJobs] == 0 &&
        Failed[EScheduleJobFailReason::NodeBanned] == 0 &&
        Failed[EScheduleJobFailReason::DataBalancingViolation] == 0;
}

bool TControllerScheduleJobResult::IsScheduleStopNeeded() const
{
    return
        Failed[EScheduleJobFailReason::NotEnoughChunkLists] > 0 ||
        Failed[EScheduleJobFailReason::JobSpecThrottling] > 0;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NControllerAgent::NProto::TPoolTreeControllerSettingsMap* protoPoolTreeControllerSettingsMap,
    const TPoolTreeControllerSettingsMap& poolTreeControllerSettingsMap)
{
    for (const auto& [treeName, settings] : poolTreeControllerSettingsMap) {
        auto* protoTreeSettings = protoPoolTreeControllerSettingsMap->add_tree_settings();
        protoTreeSettings->set_tree_name(treeName);
        ToProto(protoTreeSettings->mutable_scheduling_tag_filter(), settings.SchedulingTagFilter);
        protoTreeSettings->set_tentative(settings.Tentative);
        protoTreeSettings->set_probing(settings.Probing);
        protoTreeSettings->set_offloading(settings.Offloading);
        protoTreeSettings->set_main_resource(static_cast<int>(settings.MainResource));
    }
}

void FromProto(
    TPoolTreeControllerSettingsMap* poolTreeControllerSettingsMap,
    const NControllerAgent::NProto::TPoolTreeControllerSettingsMap& protoPoolTreeControllerSettingsMap)
{
    for (const auto& protoTreeSettings : protoPoolTreeControllerSettingsMap.tree_settings()) {
        TSchedulingTagFilter filter;
        FromProto(&filter, protoTreeSettings.scheduling_tag_filter());
        poolTreeControllerSettingsMap->emplace(
            protoTreeSettings.tree_name(),
            TPoolTreeControllerSettings{
                .SchedulingTagFilter = filter,
                .Tentative = protoTreeSettings.tentative(),
                .Probing = protoTreeSettings.probing(),
                .Offloading = protoTreeSettings.offloading(),
                .MainResource = protoTreeSettings.has_main_resource()
                    ? static_cast<EJobResourceType>(protoTreeSettings.main_resource())
                    : EJobResourceType::Cpu,
            });
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TPreemptedFor::operator == (const TPreemptedFor& other) const noexcept
{
    return JobId == other.JobId && OperationId == other.OperationId;
}

bool TPreemptedFor::operator != (const TPreemptedFor& other) const noexcept
{
    return !(*this == other);
}

TString ToString(const TPreemptedFor& preemptedFor)
{
    return Format(
        "{OperationId: %v, JobId: %v}",
        preemptedFor.OperationId,
        preemptedFor.JobId);
}

void ToProto(NControllerAgent::NProto::TPreemptedFor* proto, const TPreemptedFor& preemptedFor)
{
    ToProto(proto->mutable_job_id(), preemptedFor.JobId);
    ToProto(proto->mutable_operation_id(), preemptedFor.OperationId);
}

void FromProto(TPreemptedFor* preemptedFor, const NControllerAgent::NProto::TPreemptedFor& proto)
{
    FromProto(&preemptedFor->JobId, proto.job_id());
    FromProto(&preemptedFor->OperationId, proto.operation_id());
}

void Serialize(const TPreemptedFor& preemptedFor, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("job_id").Value(preemptedFor.JobId)
            .Item("operation_id").Value(preemptedFor.OperationId)
        .EndMap();
}

void Deserialize(TPreemptedFor& preemptedFor, const NYTree::INodePtr& node)
{
    Deserialize(preemptedFor.JobId, node->AsMap()->GetChildOrThrow("job_id"));
    Deserialize(preemptedFor.OperationId, node->AsMap()->GetChildOrThrow("operation_id"));
}

////////////////////////////////////////////////////////////////////////////////

bool TCompositePendingJobCount::IsZero() const
{
    if (DefaultCount != 0) {
        return false;
    }

    for (const auto& [_, count] : CountByPoolTree) {
        if (count != 0) {
            return false;
        }
    }

    return true;
}

int TCompositePendingJobCount::GetJobCountFor(const TString& tree) const
{
    auto it = CountByPoolTree.find(tree);
    return it != CountByPoolTree.end()
        ? it->second
        : DefaultCount;
}

void TCompositePendingJobCount::Persist(const TStreamPersistenceContext &context)
{
    using NYT::Persist;
    Persist(context, DefaultCount);
    Persist(context, CountByPoolTree);
}

void Serialize(const TCompositePendingJobCount& jobCount, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("absolute").Value(jobCount.DefaultCount)
            .Item("count_by_pool_tree").Value(jobCount.CountByPoolTree)
        .EndMap();
}

void FormatValue(TStringBuilderBase* builder, const TCompositePendingJobCount& jobCount, TStringBuf /* format */)
{
    if (jobCount.CountByPoolTree.empty()) {
        builder->AppendFormat("%v", jobCount.DefaultCount);
    } else {
        builder->AppendFormat("{DefaultCount: %v, CountByPoolTree: %v}",
            jobCount.DefaultCount,
            jobCount.CountByPoolTree);
    }
}

bool operator == (const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs)
{
    if (lhs.DefaultCount != rhs.DefaultCount) {
        return false;
    }

    if (lhs.CountByPoolTree.size() != lhs.CountByPoolTree.size()) {
        return false;
    }

    for (const auto& [tree, lhsCount] : lhs.CountByPoolTree) {
        auto rhsIt = rhs.CountByPoolTree.find(tree);
        if (rhsIt == rhs.CountByPoolTree.end()) {
            return false;
        }
        if (lhsCount != rhsIt->second) {
            return false;
        }
    }
    return true;
}

bool operator != (const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs)
{
    return !(lhs == rhs);
}

TCompositePendingJobCount operator + (const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs)
{
    TCompositePendingJobCount result;
    result.DefaultCount = lhs.DefaultCount + rhs.DefaultCount;
    for (const auto& [tree, lhsCount] : lhs.CountByPoolTree) {
        auto rhsIt = rhs.CountByPoolTree.find(tree);
        if (rhsIt == rhs.CountByPoolTree.end()) {
            result.CountByPoolTree[tree] = lhsCount;
        } else {
            result.CountByPoolTree[tree] = lhsCount + rhsIt->second;
        }
    }

    for (const auto& [tree, rhsCount] : rhs.CountByPoolTree) {
        if (result.CountByPoolTree.find(tree) == result.CountByPoolTree.end()) {
            result.CountByPoolTree[tree] = rhsCount;
        }
    }

    return result;
}

TCompositePendingJobCount operator - (const TCompositePendingJobCount& count)
{
    TCompositePendingJobCount result;
    result.DefaultCount = -count.DefaultCount;
    for (const auto& [tree, countPerTree] : count.CountByPoolTree) {
        result.CountByPoolTree[tree] = -countPerTree;
    }

    return result;
}

TCompositePendingJobCount operator - (const TCompositePendingJobCount& lhs, const TCompositePendingJobCount& rhs)
{
    return lhs + (-rhs);
}

////////////////////////////////////////////////////////////////////////////////

const TJobResources& TCompositeNeededResources::GetNeededResourcesForTree(const TString& tree) const
{
    auto it = ResourcesByPoolTree.find(tree);
    return it != ResourcesByPoolTree.end()
        ? it->second
        : DefaultResources;
}

void TCompositeNeededResources::Persist(const TStreamPersistenceContext &context)
{
    using NYT::Persist;
    Persist(context, DefaultResources);
    Persist(context, ResourcesByPoolTree);
}

void FormatValue(TStringBuilderBase* builder, const TCompositeNeededResources& neededResources, TStringBuf /* format */)
{
    builder->AppendFormat("{DefaultResources: %v, ResourcesByPoolTree: %v}",
        neededResources.DefaultResources,
        neededResources.ResourcesByPoolTree);
}

TCompositeNeededResources operator - (const TCompositeNeededResources& lhs, const TCompositeNeededResources& rhs)
{
    return lhs + (-rhs);
}

TCompositeNeededResources operator + (const TCompositeNeededResources& lhs, const TCompositeNeededResources& rhs)
{
    TCompositeNeededResources result;
    result.DefaultResources = lhs.DefaultResources + rhs.DefaultResources;
    for (const auto& [tree, lhsResources] : lhs.ResourcesByPoolTree) {
        auto rhsIt = rhs.ResourcesByPoolTree.find(tree);
        if (rhsIt == rhs.ResourcesByPoolTree.end()) {
            result.ResourcesByPoolTree[tree] = lhsResources;
        } else {
            result.ResourcesByPoolTree[tree] = lhsResources + rhsIt->second;
        }
    }

    for (const auto& [tree, rhsResources] : rhs.ResourcesByPoolTree) {
        if (result.ResourcesByPoolTree.find(tree) == result.ResourcesByPoolTree.end()) {
            result.ResourcesByPoolTree[tree] = rhsResources;
        }
    }

    return result;
}

TCompositeNeededResources operator - (const TCompositeNeededResources& resources)
{
    TCompositeNeededResources result;
    result.DefaultResources = -resources.DefaultResources;
    for (const auto& [tree, resourcesPerTree] : resources.ResourcesByPoolTree) {
        result.ResourcesByPoolTree[tree] = -resourcesPerTree;
    }

    return result;
}

TString FormatResources(const TCompositeNeededResources& resources)
{
    return Format("{DefaultResources: %v, ResourcesByPoolTree: %v}", resources.DefaultResources, resources.ResourcesByPoolTree);
}

void ToProto(NControllerAgent::NProto::TCompositeNeededResources* protoNeededResources, const TCompositeNeededResources& neededResources)
{
    using namespace NProto;
    ToProto(protoNeededResources->mutable_default_resources(), neededResources.DefaultResources);

    auto protoMap = protoNeededResources->mutable_resources_per_pool_tree();
    for (const auto& [tree, resources] : neededResources.ResourcesByPoolTree) {
        ToProto(&(*protoMap)[tree], resources);
    }
}

void FromProto(TCompositeNeededResources* neededResources, const NControllerAgent::NProto::TCompositeNeededResources& protoNeededResources)
{
    FromProto(&neededResources->DefaultResources, protoNeededResources.default_resources());

    for (const auto& [tree, resources] : protoNeededResources.resources_per_pool_tree()) {
        FromProto(&neededResources->ResourcesByPoolTree[tree], resources);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
