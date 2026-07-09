#include "describe_computations.h"

namespace NYT::NFlow::NDescribe {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

void TComputationsDescription::Register(TRegistrar registrar)
{
    registrar.Parameter("computations", &TThis::Computations)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TComputationsDescription DescribeComputations(const TFlowViewPtr& flowView)
{
    TComputationsDescription computations;
    auto intermediateDescriptions = GetComputationPartitionIntermediateDescriptions(flowView);
    for (const auto& [computationId, computation] : MakeComputationDescriptions(flowView, intermediateDescriptions)) {
        computations.Computations.push_back(computation);
    }
    SortBy(computations.Computations, [] (const auto& computation) -> auto& {
        return computation.Name;
    });
    return computations;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
