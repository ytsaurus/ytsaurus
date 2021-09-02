#include "resource_helpers.h"

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

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

void FormatValue(TStringBuilderBase* builder, const TResourceVector& resourceVector, TStringBuf format)
{
    auto getResourceSuffix = [] (EJobResourceType resourceType) {
        const auto& resourceNames = TEnumTraits<EJobResourceType>::GetDomainNames();
        switch (resourceType) {
            case EJobResourceType::UserSlots:
                // S is for Slots.
                return 'S';

            default:
                return resourceNames[ToUnderlying(resourceType)][0];
        }
    };

    builder->AppendChar('[');
    bool isFirst = true;
    for (auto resourceType : TEnumTraits<EJobResourceType>::GetDomainValues()) {
        if (!isFirst) {
            builder->AppendChar(' ');
        }
        isFirst = false;

        FormatValue(builder, resourceVector[resourceType], format);
        builder->AppendChar(getResourceSuffix(resourceType));
    }
    builder->AppendChar(']');
}

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

////////////////////////////////////////////////////////////////////////////////

void ProfileResourceVolume(
    NProfiling::ISensorWriter* writer,
    const TResourceVolume& volume,
    const TString& prefix)
{
    #define XX(name, Name) writer->AddGauge(prefix + "/" #name, static_cast<double>(volume.Get##Name()));
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

void Serialize(const TResourceVolume& volume, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            #define XX(name, Name) .Item(#name).Value(volume.Get##Name())
            ITERATE_JOB_RESOURCES(XX)
            #undef XX
        .EndMap();
}

void Deserialize(TResourceVolume& volume, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();
    #define XX(name, Name) \
        if (auto child = mapNode->FindChild(#name)) { \
            auto value = volume.Get##Name(); \
            Deserialize(value, child); \
            volume.Set##Name(value); \
        }
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

void FormatValue(TStringBuilderBase* builder, const TResourceVolume& volume, TStringBuf /* format */)
{
    builder->AppendFormat(
        "{UserSlots: %.2f, Cpu: %v, Gpu: %.2f, Memory: %.2fMBs, Network: %.2f}",
        volume.GetUserSlots(),
        volume.GetCpu(),
        volume.GetGpu(),
        volume.GetMemory() / 1_MB,
        volume.GetNetwork());
}

TString ToString(const TResourceVolume& volume)
{
    return ToStringViaBuilder(volume);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

