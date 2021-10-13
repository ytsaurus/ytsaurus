#include "pool_resources.h"

namespace NYT::NSchedulerPoolServer {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TPoolResources::TPoolResources()
{
    RegisterParameter("strong_guarantee_resources", StrongGuaranteeResources)
        .DefaultNew();
    RegisterParameter("burst_guarantee_resources", BurstGuaranteeResources)
        .DefaultNew();
    RegisterParameter("resource_flow", ResourceFlow)
        .DefaultNew();
    RegisterParameter("max_operation_count", MaxOperationCount)
        .Default();
    RegisterParameter("max_running_operation_count", MaxRunningOperationCount)
        .Default();
}

bool TPoolResources::IsNonNegative()
{
    // NB: fields of type TJobResourcesConfig cannot be negative.
    return (!MaxOperationCount.has_value() || MaxOperationCount >= 0) &&
        (!MaxRunningOperationCount.has_value() || MaxRunningOperationCount >= 0);
}

TPoolResourcesPtr TPoolResources::operator-()
{
    auto result = New<TPoolResources>();
    result->StrongGuaranteeResources = -*StrongGuaranteeResources;
    result->BurstGuaranteeResources = -*BurstGuaranteeResources;
    result->ResourceFlow = -*ResourceFlow;
    if (MaxOperationCount.has_value()) {
        result->MaxOperationCount = -*MaxOperationCount;
    }
    if (MaxRunningOperationCount.has_value()) {
        result->MaxRunningOperationCount = -*MaxRunningOperationCount;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
