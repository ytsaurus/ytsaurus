#include "serialize.h"

#include <yt/yt/ytlib/scheduler/job_resources_helpers.h>

#include <yt/yt/library/vector_hdrf/resource_helpers.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TDetailedFairShare& detailedFairShare, NYson::IYsonConsumer* consumer)
{
    using NVectorHdrf::Serialize;

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

void Serialize(const TFairShareFunctionsStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("fair_share_by_suggestion_size").Value(statistics.FairShareBySuggestionSize)
            .Item("fair_share_by_fit_factor_size").Value(statistics.FairShareByFitFactorSize)
            .Item("max_fit_factor_by_suggestion_size").Value(statistics.MaxFitFactorBySuggestionSize)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
