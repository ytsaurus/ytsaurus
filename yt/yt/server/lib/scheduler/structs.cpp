#include "structs.h"

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NScheduler {

using namespace NControllerAgent;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TAllocationAttributes* protoAttributes,
    const TAllocationAttributes& attributes)
{
    if (auto timeout = attributes.WaitingForResourcesOnNodeTimeout) {
        protoAttributes->set_waiting_for_resources_on_node_timeout(ToProto<i64>(*timeout));
    }
    if (const auto& cudaToolkitVersion = attributes.CudaToolkitVersion) {
        protoAttributes->set_cuda_toolkit_version(*cudaToolkitVersion);
    }
    {
        auto& diskRequest =
            (*protoAttributes->mutable_disk_request() = {});
        if (auto& diskSpace = attributes.DiskRequest.DiskSpace) {
            diskRequest.set_disk_space(ToProto<i64>(*diskSpace));
        }
        if (auto& mediumIndex = attributes.DiskRequest.MediumIndex) {
            diskRequest.set_medium_index(ToProto<i32>(*mediumIndex));
        }
        if (auto& inodeCount = attributes.DiskRequest.InodeCount) {
            diskRequest.set_inode_count(ToProto<i64>(*inodeCount));
        }
    }

    protoAttributes->set_allow_idle_cpu_policy(attributes.AllowIdleCpuPolicy);
    protoAttributes->set_port_count(ToProto<i64>(attributes.PortCount));
}

void FromProto(
    TAllocationAttributes* attributes,
    const NProto::TAllocationAttributes& protoAttributes)
{
    if (protoAttributes.has_waiting_for_resources_on_node_timeout()) {
        attributes->WaitingForResourcesOnNodeTimeout
            = FromProto<TDuration>(protoAttributes.waiting_for_resources_on_node_timeout());
    }
    if (protoAttributes.has_cuda_toolkit_version()) {
        attributes->CudaToolkitVersion
            = FromProto<TString>(protoAttributes.cuda_toolkit_version());
    }
    {
        auto& diskRequest =
            (attributes->DiskRequest = {});
        if (protoAttributes.disk_request().has_disk_space()) {
            diskRequest.DiskSpace = FromProto<i64>(protoAttributes.disk_request().disk_space());
        }
        if (protoAttributes.disk_request().has_medium_index()) {
            diskRequest.MediumIndex = FromProto<i32>(protoAttributes.disk_request().medium_index());
        }
        if (protoAttributes.disk_request().has_inode_count()) {
            diskRequest.InodeCount = FromProto<i64>(protoAttributes.disk_request().inode_count());
        }
    }
    attributes->AllowIdleCpuPolicy = protoAttributes.allow_idle_cpu_policy();
    attributes->PortCount = protoAttributes.port_count();
}

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

void ToProto(
    NScheduler::NProto::TScheduleAllocationResponse* protoResponse,
    const TControllerScheduleAllocationResult& scheduleJobResult)
{
    protoResponse->set_controller_epoch(scheduleJobResult.ControllerEpoch.Underlying());
    protoResponse->set_success(static_cast<bool>(scheduleJobResult.StartDescriptor));

    if (scheduleJobResult.StartDescriptor) {
        const auto& startDescriptor = *scheduleJobResult.StartDescriptor;
        ToProto(protoResponse->mutable_resource_limits(), startDescriptor.ResourceLimits);

        ToProto(
            protoResponse->mutable_allocation_attributes(),
            startDescriptor.AllocationAttributes);
    }

    protoResponse->set_duration(ToProto<i64>(scheduleJobResult.Duration));
    if (scheduleJobResult.NextDurationEstimate) {
        protoResponse->set_next_duration_estimate(ToProto<i64>(*scheduleJobResult.NextDurationEstimate));
    }
    for (auto reason : TEnumTraits<EScheduleAllocationFailReason>::GetDomainValues()) {
        if (scheduleJobResult.Failed[reason] > 0) {
            auto* protoCounter = protoResponse->add_failed();
            protoCounter->set_reason(static_cast<int>(reason));
            protoCounter->set_value(scheduleJobResult.Failed[reason]);
        }
    }
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

void FormatValue(TStringBuilderBase* builder, const TPreemptedFor& preemptedFor, TStringBuf /*spec*/)
{
    Format(
        builder,
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
            // COMPAT(pogorelov)
            .Item("job_id").Value(preemptedFor.AllocationId)
            .Item("allocation_id").Value(preemptedFor.AllocationId)
            .Item("operation_id").Value(preemptedFor.OperationId)
        .EndMap();
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
