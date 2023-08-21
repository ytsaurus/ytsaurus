#include "pool_resources.h"

#include <yt/yt/ytlib/scheduler/config.h>

namespace NYT::NSchedulerPoolServer {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TPoolResources::Register(TRegistrar registrar)
{
    registrar.Parameter("strong_guarantee_resources", &TThis::StrongGuaranteeResources)
        .DefaultNew();
    registrar.Parameter("burst_guarantee_resources", &TThis::BurstGuaranteeResources)
        .DefaultNew();
    registrar.Parameter("resource_flow", &TThis::ResourceFlow)
        .DefaultNew();
    registrar.Parameter("max_operation_count", &TThis::MaxOperationCount)
        .Default();
    registrar.Parameter("max_running_operation_count", &TThis::MaxRunningOperationCount)
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
