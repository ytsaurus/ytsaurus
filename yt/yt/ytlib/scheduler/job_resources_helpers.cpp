#include "job_resources_helpers.h"
#include "config.h"

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>

#include <yt/yt/core/ytree/fluent.h>

#include <functional>

namespace NYT {

using namespace NYson;
using namespace NYTree;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NProfiling;

namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

TJobResources ToJobResources(const NNodeTrackerClient::NProto::TNodeResources& nodeResources)
{
    TJobResources result;
    #define XX(name, Name) result.Set##Name(nodeResources.name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

TNodeResources ToNodeResources(const TJobResources& jobResources)
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(static_cast<decltype(result.name())>(jobResources.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TJobResources ToJobResources(const TJobResourcesConfigPtr& config, TJobResources defaultValue)
{
    return NVectorHdrf::ToJobResources(*config, defaultValue);
}

////////////////////////////////////////////////////////////////////////////////

void SerializeDiskQuotaImpl(
    const TDiskQuota& quota,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
    TFluentMap fluent)
{
    fluent
        .Item("disk_space_per_medium")
            .DoMapFor(quota.DiskSpacePerMedium, [&] (TFluentMap fluent, const std::pair<int, i64>& pair) {
                auto [mediumIndex, diskSpace] = pair;
                fluent.Item(mediumDirectory->FindByIndex(mediumIndex)->Name).Value(diskSpace);
            })
        .Item("disk_space_without_medium").Value(quota.DiskSpaceWithoutMedium);
}

void SerializeDiskQuota(
    const TDiskQuota& quota,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
    IYsonConsumer* consumer)
{
    SerializeDiskQuotaImpl(quota, mediumDirectory, BuildYsonMapFragmentFluently(consumer));
}

void SerializeJobResourcesWithQuota(
    const TJobResourcesWithQuota& resources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory,
    IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            #define XX(name, Name) .Item(#name).Value(resources.Get##Name())
            ITERATE_JOB_RESOURCES(XX)
            #undef XX
            .Do(std::bind(SerializeDiskQuotaImpl, resources.DiskQuota(), mediumDirectory, std::placeholders::_1))
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TString FormatResource(
    const TJobResources& usage,
    const TJobResources& limits)
{
    return Format(
        "UserSlots: %v/%v, Cpu: %v/%v, Gpu: %v/%v, Memory: %v/%v, Network: %v/%v",
        // User slots
        usage.GetUserSlots(),
        limits.GetUserSlots(),
        // Cpu
        usage.GetCpu(),
        limits.GetCpu(),
        // Gpu
        usage.GetGpu(),
        limits.GetGpu(),
        // Memory (in MB)
        usage.GetMemory() / 1_MB,
        limits.GetMemory() / 1_MB,
        // Network
        usage.GetNetwork(),
        limits.GetNetwork());
}

TString FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits)
{
    return Format("{%v}", FormatResource(usage, limits));
}

TString FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits,
    const NNodeTrackerClient::NProto::TDiskResources& diskResources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
{
    return Format("{%v, DiskResources: %v}",
        FormatResource(usage, limits),
        NNodeTrackerClient::ToString(diskResources, mediumDirectory));
}

TString FormatResources(const TJobResources& resources)
{
    return Format(
        "{UserSlots: %v, Cpu: %v, Gpu: %v, Memory: %vMB, Network: %v}",
        resources.GetUserSlots(),
        resources.GetCpu(),
        resources.GetGpu(),
        resources.GetMemory() / 1_MB,
        resources.GetNetwork());
}

TString FormatResources(const TJobResourcesWithQuota& resources)
{
    return Format(
        "{UserSlots: %v, Cpu: %v, Gpu: %v, Memory: %vMB, Network: %v, DiskQuota: %v}",
        resources.GetUserSlots(),
        resources.GetCpu(),
        resources.GetGpu(),
        resources.GetMemory() / 1_MB,
        resources.GetNetwork(),
        resources.DiskQuota()
    );

}

TString FormatResourcesConfig(const TJobResourcesConfigPtr& config)
{
    TStringBuilder builder;
    builder.AppendChar('{');

    TDelimitedStringBuilderWrapper delimitedBuilder(&builder);
    config->ForEachResource([&] (auto NVectorHdrf::TJobResourcesConfig::* resourceDataMember, EJobResourceType resourceType) {
        if (auto value = config.Get()->*resourceDataMember) {
            delimitedBuilder->AppendFormat(
                "%v: %v",
                TEnumTraits<EJobResourceType>::ToString(resourceType),
                *value);
        }
    });

    builder.AppendChar('}');

    return builder.Flush();
}

void FormatValue(TStringBuilderBase* builder, const TDiskQuota& diskQuota, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "%v {DiskSpaceWithoutMedium: %v}",
        MakeFormattableView(diskQuota.DiskSpacePerMedium, [] (TStringBuilderBase* builder, const std::pair<int, i64>& pair) {
            auto [mediumIndex, diskSpace] = pair;
            builder->AppendFormat("{MediumIndex: %v, DiskSpace: %v}", mediumIndex, diskSpace);
        }),
        diskQuota.DiskSpaceWithoutMedium);
}

////////////////////////////////////////////////////////////////////////////////

void TJobResourcesProfiler::Init(const NProfiling::TProfiler& profiler)
{
    #define XX(name, Name) Name = profiler.Gauge("/" #name);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

void TJobResourcesProfiler::Reset()
{
    #define XX(name, Name) Name = {};
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

void TJobResourcesProfiler::Update(const TJobResources& resources)
{
    #define XX(name, Name) Name.Update(static_cast<double>(resources.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

void ProfileResources(
    NProfiling::ISensorWriter* writer,
    const TJobResources& resources,
    const TString& prefix,
    NProfiling::EMetricType metricType)
{
    switch (metricType) {
        case NProfiling::EMetricType::Gauge:
            #define XX(name, Name) writer->AddGauge(prefix + "/" #name, static_cast<double>(resources.Get##Name()));
            ITERATE_JOB_RESOURCES(XX)
            #undef XX
            break;
        case NProfiling::EMetricType::Counter:
            // NB: CPU value will be rounded down.
            #define XX(name, Name) writer->AddCounter(prefix + "/" #name, static_cast<i64>(resources.Get##Name()));
            ITERATE_JOB_RESOURCES(XX)
            #undef XX
            break;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NScheduler::NProto::TDiskQuota* protoDiskQuota, const NScheduler::TDiskQuota& diskQuota)
{
    for (auto [mediumIndex, diskSpace] : diskQuota.DiskSpacePerMedium) {
        auto* protoDiskLocationQuotaProto = protoDiskQuota->add_disk_location_quota();
        protoDiskLocationQuotaProto->set_medium_index(mediumIndex);
        protoDiskLocationQuotaProto->set_disk_space(diskSpace);
    }
    if (diskQuota.DiskSpaceWithoutMedium) {
        auto* protoDiskLocationQuotaProto = protoDiskQuota->add_disk_location_quota();
        protoDiskLocationQuotaProto->set_disk_space(*diskQuota.DiskSpaceWithoutMedium);
    }
}

void FromProto(NScheduler::TDiskQuota* diskQuota, const NScheduler::NProto::TDiskQuota& protoDiskQuota)
{
    for (const auto& protoDiskLocationQuota : protoDiskQuota.disk_location_quota()) {
        if (protoDiskLocationQuota.has_medium_index()) {
            diskQuota->DiskSpacePerMedium.emplace(protoDiskLocationQuota.medium_index(), protoDiskLocationQuota.disk_space());
        } else {
            diskQuota->DiskSpaceWithoutMedium = protoDiskLocationQuota.disk_space();
        }
    }
}

void ToProto(NScheduler::NProto::TJobResources* protoResources, const NScheduler::TJobResources& resources)
{
    protoResources->set_cpu(static_cast<double>(resources.GetCpu()));
    protoResources->set_gpu(resources.GetGpu());
    protoResources->set_user_slots(resources.GetUserSlots());
    protoResources->set_memory(resources.GetMemory());
    protoResources->set_network(resources.GetNetwork());
}

void FromProto(NScheduler::TJobResources* resources, const NScheduler::NProto::TJobResources& protoResources)
{
    resources->SetCpu(TCpuResource(protoResources.cpu()));
    resources->SetGpu(protoResources.gpu());
    resources->SetUserSlots(protoResources.user_slots());
    resources->SetMemory(protoResources.memory());
    resources->SetNetwork(protoResources.network());
}

void ToProto(NScheduler::NProto::TJobResourcesWithQuota* protoResources, const NScheduler::TJobResourcesWithQuota& resources)
{
    protoResources->set_cpu(static_cast<double>(resources.GetCpu()));
    protoResources->set_gpu(resources.GetGpu());
    protoResources->set_user_slots(resources.GetUserSlots());
    protoResources->set_memory(resources.GetMemory());
    protoResources->set_network(resources.GetNetwork());

    auto diskQuota = resources.DiskQuota();
    ToProto(protoResources->mutable_disk_quota(), diskQuota);
}

void FromProto(NScheduler::TJobResourcesWithQuota* resources, const NScheduler::NProto::TJobResourcesWithQuota& protoResources)
{
    resources->SetCpu(TCpuResource(protoResources.cpu()));
    resources->SetGpu(protoResources.gpu());
    resources->SetUserSlots(protoResources.user_slots());
    resources->SetMemory(protoResources.memory());
    resources->SetNetwork(protoResources.network());

    NScheduler::TDiskQuota diskQuota;
    FromProto(&diskQuota, protoResources.disk_quota());
    resources->DiskQuota() = diskQuota;
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler

} // namespace NYT
