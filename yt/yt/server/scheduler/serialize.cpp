#include "serialize.h"

#include <yt/yt/ytlib/scheduler/job_resources_serialize.h>

namespace NYT {

namespace NFairShare {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TDetailedFairShare& detailedFairShare, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("strong_guarantee").Value(detailedFairShare.StrongGuarantee)
            .Item("integral_guarantee").Value(detailedFairShare.IntegralGuarantee)
            .Item("weight_proportional").Value(detailedFairShare.WeightProportional)
            .Item("total").Value(detailedFairShare.Total)
        .EndMap();
}

void SerializeDominant(const TDetailedFairShare& detailedFairShare, NYTree::TFluentAny fluent)
{
    fluent
        .BeginMap()
            .Item("strong_guarantee").Value(MaxComponent(detailedFairShare.StrongGuarantee))
            .Item("integral_guarantee").Value(MaxComponent(detailedFairShare.IntegralGuarantee))
            .Item("weight_proportional").Value(MaxComponent(detailedFairShare.WeightProportional))
            .Item("total").Value(MaxComponent(detailedFairShare.Total))
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFairShare

namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TResourceVector& resourceVector, NYson::IYsonConsumer* consumer)
{
    auto fluent = NYTree::BuildYsonFluently(consumer).BeginMap();
    for (int index = 0; index < ResourceCount; ++index) {
        fluent
            .Item(FormatEnum(TResourceVector::GetResourceTypeById(index)))
            .Value(resourceVector[index]);
    }
    fluent.EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler

} // namespace NYT
