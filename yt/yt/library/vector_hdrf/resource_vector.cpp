#include "resource_vector.h"

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////
    
TResourceVector TResourceVector::FromJobResources(
    const TJobResources& resources,
    const TJobResources& totalLimits,
    double zeroDivByZero,
    double oneDivByZero)
{
    TResourceVector resultVector = {};
    auto update = [&] (auto resourceValue, auto resourceLimit, double& result) {
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
    #define XX(name, Name, resourceIndex) update(resources.Get##Name(), totalLimits.Get##Name(), resultVector[resourceIndex]);
    ITERATE_JOB_RESOURCES_WITH_INDEX(XX)
    #undef XX
    return resultVector;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

