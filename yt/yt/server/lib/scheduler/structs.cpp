#include "structs.h"

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NScheduler {

using namespace NControllerAgent;
using namespace NYson;
using namespace NYTree;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TAllocationAttributes* protoAttributes,
    const TAllocationAttributes& attributes)
{
    if (auto timeout = attributes.WaitingForResourcesOnNodeTimeout) {
        protoAttributes->set_waiting_for_resources_on_node_timeout(ToProto(*timeout));
    }
    if (const auto& cudaToolkitVersion = attributes.CudaToolkitVersion) {
        protoAttributes->set_cuda_toolkit_version(*cudaToolkitVersion);
    }
    {
        auto& diskRequest = (*protoAttributes->mutable_disk_request() = {});
        if (auto diskSpace = attributes.DiskRequest.DiskSpace) {
            diskRequest.set_disk_space(*diskSpace);
        }
        if (auto mediumIndex = attributes.DiskRequest.MediumIndex) {
            diskRequest.set_medium_index(*mediumIndex);
        }
        if (auto& inodeCount = attributes.DiskRequest.InodeCount) {
            diskRequest.set_inode_count(*inodeCount);
        }
    }

    protoAttributes->set_allow_idle_cpu_policy(attributes.AllowIdleCpuPolicy);
    protoAttributes->set_port_count(attributes.PortCount);
    protoAttributes->set_enable_multiple_jobs(attributes.EnableMultipleJobs);
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
    if (protoAttributes.has_enable_multiple_jobs()) {
        attributes->EnableMultipleJobs = protoAttributes.enable_multiple_jobs();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TControllerScheduleAllocationResult::RecordFail(EScheduleFailReason reason)
{
    ++Failed[reason];
}

bool TControllerScheduleAllocationResult::IsBackoffNeeded() const
{
    return
        !StartDescriptor &&
        Failed[EScheduleFailReason::NotEnoughResources] == 0 &&
        Failed[EScheduleFailReason::NoLocalJobs] == 0 &&
        Failed[EScheduleFailReason::NodeBanned] == 0 &&
        Failed[EScheduleFailReason::DataBalancingViolation] == 0;
}

bool TControllerScheduleAllocationResult::IsScheduleStopNeeded() const
{
    return
        Failed[EScheduleFailReason::NotEnoughChunkLists] > 0 ||
        Failed[EScheduleFailReason::JobSpecThrottling] > 0;
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

    protoResponse->set_duration(ToProto(scheduleJobResult.Duration));
    if (scheduleJobResult.NextDurationEstimate) {
        protoResponse->set_next_duration_estimate(ToProto(*scheduleJobResult.NextDurationEstimate));
    }
    for (auto reason : TEnumTraits<EScheduleFailReason>::GetDomainValues()) {
        if (scheduleJobResult.Failed[reason] > 0) {
            auto* protoCounter = protoResponse->add_failed();
            protoCounter->set_reason(ToProto(reason));
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
        protoTreeSettings->set_main_resource(ToProto(settings.MainResource));
        protoTreeSettings->set_allow_idle_cpu_policy(settings.AllowIdleCpuPolicy);
    }
}

void FromProto(
    TPoolTreeControllerSettingsMap* poolTreeControllerSettingsMap,
    const NControllerAgent::NProto::TPoolTreeControllerSettingsMap& protoPoolTreeControllerSettingsMap)
{
    for (const auto& protoTreeSettings : protoPoolTreeControllerSettingsMap.tree_settings()) {
        poolTreeControllerSettingsMap->emplace(
            protoTreeSettings.tree_name(),
            TPoolTreeControllerSettings{
                .SchedulingTagFilter = FromProto<TSchedulingTagFilter>(protoTreeSettings.scheduling_tag_filter()),
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

void Serialize(const TPreemptedFor& preemptedFor, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
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
    builder->AppendFormat(
        "{DefaultResources: %v, ResourcesByPoolTree: %v}",
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

void TAllocationGroupResources::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, MinNeededResources);
    Persist(context, AllocationCount);
}

void FormatValue(TStringBuilderBase* builder, const TAllocationGroupResources& allocationGroupResources, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "{MinNeededResources: %v, AllocationCount: %v}",
        allocationGroupResources.MinNeededResources,
        allocationGroupResources.AllocationCount);
}

void Serialize(const TAllocationGroupResources& allocationGroupResources, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            // TODO(eshcherbin): Refactor job resources with disk quota serialization.
            .Item("min_needed_resources").Value(allocationGroupResources.MinNeededResources.ToJobResources())
            .Item("allocation_count").Value(allocationGroupResources.AllocationCount)
        .EndMap();
}

void ToProto(
    NControllerAgent::NProto::TAllocationGroupResources* protoAllocationGroupResources,
    const TAllocationGroupResources& allocationGroupResources)
{
    ToProto(protoAllocationGroupResources->mutable_min_needed_resources(), allocationGroupResources.MinNeededResources);
    protoAllocationGroupResources->set_allocation_count(allocationGroupResources.AllocationCount);
}

void FromProto(
    TAllocationGroupResources* allocationGroupResources,
    const NControllerAgent::NProto::TAllocationGroupResources& protoAllocationGroupResources)
{
    FromProto(&allocationGroupResources->MinNeededResources, protoAllocationGroupResources.min_needed_resources());
    allocationGroupResources->AllocationCount = protoAllocationGroupResources.allocation_count();
}

void ToProto(
    ::google::protobuf::Map<TProtoStringType, NControllerAgent::NProto::TAllocationGroupResources>* protoAllocationGroupResourcesMap,
    const TAllocationGroupResourcesMap& allocationGroupResourcesMap)
{
    protoAllocationGroupResourcesMap->clear();
    for (const auto& [name, resources] : allocationGroupResourcesMap) {
        ToProto(&(*protoAllocationGroupResourcesMap)[name], resources);
    }
}

void FromProto(
    TAllocationGroupResourcesMap* allocationGroupResourcesMap,
    const ::google::protobuf::Map<TProtoStringType, NControllerAgent::NProto::TAllocationGroupResources>& protoAllocationGroupResourcesMap)
{
    allocationGroupResourcesMap->clear();
    allocationGroupResourcesMap->reserve(protoAllocationGroupResourcesMap.size());
    for (const auto& [name, resources] : protoAllocationGroupResourcesMap) {
        FromProto(&(*allocationGroupResourcesMap)[name], resources);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
