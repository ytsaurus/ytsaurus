#include "structs.h"

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NScheduler {

using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

TAllocationStartDescriptor::TAllocationStartDescriptor(
    TAllocationId id,
    const TJobResourcesWithQuota& resourceLimits)
    : Id(id)
    , ResourceLimits(resourceLimits)
{ }

////////////////////////////////////////////////////////////////////////////////

void TControllerScheduleAllocationResult::RecordFail(EScheduleAllocationFailReason reason)
{
    ++Failed[reason];
}

bool TControllerScheduleAllocationResult::IsBackoffNeeded() const
{
    return
        !StartDescriptor &&
        Failed[EScheduleAllocationFailReason::NotEnoughResources] == 0 &&
        Failed[EScheduleAllocationFailReason::NoLocalJobs] == 0 &&
        Failed[EScheduleAllocationFailReason::NodeBanned] == 0 &&
        Failed[EScheduleAllocationFailReason::DataBalancingViolation] == 0;
}

bool TControllerScheduleAllocationResult::IsScheduleStopNeeded() const
{
    return
        Failed[EScheduleAllocationFailReason::NotEnoughChunkLists] > 0 ||
        Failed[EScheduleAllocationFailReason::JobSpecThrottling] > 0;
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
        protoTreeSettings->set_allow_idle_cpu_policy(settings.AllowIdleCpuPolicy);
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
                .AllowIdleCpuPolicy = protoTreeSettings.allow_idle_cpu_policy(),
            });
    }
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TPreemptedFor& preemptedFor)
{
    return Format(
        "{OperationId: %v, AllocationId: %v}",
        preemptedFor.OperationId,
        preemptedFor.AllocationId);
}

void ToProto(NProto::TPreemptedFor* proto, const TPreemptedFor& preemptedFor)
{
    ToProto(proto->mutable_allocation_id(), preemptedFor.AllocationId);
    ToProto(proto->mutable_operation_id(), preemptedFor.OperationId);
}

void FromProto(TPreemptedFor* preemptedFor, const NProto::TPreemptedFor& proto)
{
    FromProto(&preemptedFor->AllocationId, proto.allocation_id());
    FromProto(&preemptedFor->OperationId, proto.operation_id());
}

void Serialize(const TPreemptedFor& preemptedFor, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("job_id").Value(preemptedFor.AllocationId)
            .Item("operation_id").Value(preemptedFor.OperationId)
        .EndMap();
}

// TODO(pogorelov): Use allocation_id here
void Deserialize(TPreemptedFor& preemptedFor, const NYTree::INodePtr& node)
{
    Deserialize(preemptedFor.AllocationId, node->AsMap()->GetChildOrThrow("job_id"));
    Deserialize(preemptedFor.OperationId, node->AsMap()->GetChildOrThrow("operation_id"));
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

void FormatValue(TStringBuilderBase* builder, const TCompositeNeededResources& neededResources, TStringBuf /*format*/)
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
