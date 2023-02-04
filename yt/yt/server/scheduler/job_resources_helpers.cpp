#include "job_resources_helpers.h"

#include <yt/yt/ytlib/scheduler/config.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TJobResources GetAdjustedResourceLimits(
    const TJobResources& demand,
    const TJobResources& limits,
    const TMemoryDistribution& execNodeMemoryDistribution)
{
    auto adjustedLimits = limits;

    // Take memory granularity into account.
    if (demand.GetUserSlots() > 0 && !execNodeMemoryDistribution.empty()) {
        i64 memoryDemandPerJob = demand.GetMemory() / demand.GetUserSlots();
        if (memoryDemandPerJob != 0) {
            i64 newMemoryLimit = 0;
            for (const auto& [memoryLimitPerNode, nodeCount] : execNodeMemoryDistribution) {
                i64 slotsPerNode = memoryLimitPerNode / memoryDemandPerJob;
                i64 adjustedMemoryLimit = slotsPerNode * memoryDemandPerJob * nodeCount;
                newMemoryLimit += adjustedMemoryLimit;
            }
            adjustedLimits.SetMemory(newMemoryLimit);
        }
    }

    return adjustedLimits;
}

////////////////////////////////////////////////////////////////////////////////

void ProfileResourceVector(
    NProfiling::ISensorWriter* writer,
    const THashSet<EJobResourceType>& resourceTypes,
    const TResourceVector& resourceVector,
    const TString& prefix)
{
    for (auto resourceType : resourceTypes) {
        writer->AddGauge(
            prefix + "/" + FormatEnum(resourceType),
            resourceVector[resourceType]);
    }
}

void ProfileResourceVolume(
    NProfiling::ISensorWriter* writer,
    const TResourceVolume& volume,
    const TString& prefix,
    NProfiling::EMetricType metricType)
{
    switch (metricType) {
        case NProfiling::EMetricType::Gauge:
            #define XX(name, Name) writer->AddGauge(prefix + "/" #name, static_cast<double>(volume.Get##Name()));
            ITERATE_JOB_RESOURCES(XX)
            #undef XX
            break;
        case NProfiling::EMetricType::Counter:
            // NB: CPU value will be rounded down.
            #define XX(name, Name) writer->AddCounter(prefix + "/" #name, static_cast<i64>(volume.Get##Name()));
            ITERATE_JOB_RESOURCES(XX)
            #undef XX
            break;
        default:
            YT_ABORT();
    }
}

void ProfileResourcesConfig(
    NProfiling::ISensorWriter* writer,
    const TJobResourcesConfigPtr& resourcesConfig,
    const TString& prefix)
{
    if (!resourcesConfig) {
        return;
    }

    resourcesConfig->ForEachResource([&] (auto NVectorHdrf::TJobResourcesConfig::* resourceDataMember, EJobResourceType resourceType) {
        if (auto value = resourcesConfig.Get()->*resourceDataMember) {
            writer->AddGauge(prefix + "/" + FormatEnum(resourceType), static_cast<double>(*value));
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler


