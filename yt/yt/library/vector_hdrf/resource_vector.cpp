#include "resource_vector.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NYTree;

TResourceVector TResourceVector::FromJobResources(
    const TJobResources& resources,
    const TJobResources& totalLimits,
    double zeroDivByZero,
    double oneDivByZero)
{
    auto computeResult = [&] (auto resourceValue, auto resourceLimit, double& result) {
        if (static_cast<double>(resourceLimit) == 0.0) {
            if (static_cast<double>(resourceValue) == 0.0) {
                result = zeroDivByZero;
            } else {
                result = oneDivByZero;
            }
        } else {
            result = static_cast<double>(resourceValue) / static_cast<double>(resourceLimit);
        }
    };

    TResourceVector resultVector;
    #define XX(name, Name) computeResult(resources.Get##Name(), totalLimits.Get##Name(), resultVector[EJobResourceType::Name]);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return resultVector;
}

void Serialize(const TResourceVector& resourceVector, IYsonConsumer* consumer)
{
    auto fluent = NYTree::BuildYsonFluently(consumer).BeginMap();
    for (int index = 0; index < ResourceCount; ++index) {
        fluent
            .Item(FormatEnum(TResourceVector::GetResourceTypeById(index)))
            .Value(resourceVector[index]);
    }
    fluent.EndMap();
}

void FormatValue(TStringBuilderBase* builder, const TResourceVector& resourceVector, TStringBuf spec)
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

        FormatValue(builder, resourceVector[resourceType], spec);
        builder->AppendChar(getResourceSuffix(resourceType));
    }
    builder->AppendChar(']');
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
