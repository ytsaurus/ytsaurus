#include "resource_vector.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////
    
TResourceVector TResourceVector::FromJobResources(
    const TJobResources& resources,
    const TJobResources& totalLimits,
    double zeroDivByZero,
    double oneDivByZero)
{
    TResourceVector result = {};
    int resourceId = 0;
    auto update = [&](auto resourceValue, auto resourceLimit) {
        if (static_cast<double>(resourceLimit) == 0.0) {
            if (static_cast<double>(resourceValue) == 0.0) {
                result[resourceId] = zeroDivByZero;
            } else {
                result[resourceId] = oneDivByZero;
            }
        } else {
            result[resourceId] = static_cast<double>(resourceValue) / static_cast<double>(resourceLimit);
        }
        ++resourceId;
    };
    #define XX(name, Name) update(resources.Get##Name(), totalLimits.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return result;
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
    const TString& prefix,
    bool profilingCompatibilityEnabled)
{
    if (profilingCompatibilityEnabled) {
        for (auto resourceType : resourceTypes) {
            writer->AddGauge(
                prefix + "_x100000/" + FormatEnum(resourceType),
                static_cast<i64>(resourceVector[resourceType] * 1e5));
        }
    } else {
        for (auto resourceType : resourceTypes) {
            writer->AddGauge(
                prefix + "/" + FormatEnum(resourceType),
                resourceVector[resourceType]);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

