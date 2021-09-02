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

} // namespace NYT::NScheduler

