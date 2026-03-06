#ifndef SPARE_INSTANCES_INL_H_
#error "Direct inclusion of this file is not allowed, include spare_instances.h"
// For the sake of sane code completion.
#include "spare_instances.h"
#endif

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

template <class TSpareInstance>
TSpareInstanceAllocator<TSpareInstance>::TSpareInstanceAllocator(
    TZoneToDataCenterToInfo& spareInstances)
    : SpareInstances(spareInstances)
{ }

template <class TSpareInstances>
bool TSpareInstanceAllocator<TSpareInstances>::HasInstances(const std::string& zoneName, const std::string& dataCenterName) const
{
    if (!SpareInstances.contains(zoneName)) {
        return false;
    }
    auto& dcToInfo = GetOrCrash(SpareInstances, zoneName);
    if (!dcToInfo.contains(dataCenterName)) {
        return false;
    }
    auto& info = GetOrCrash(dcToInfo, dataCenterName);
    return !info.FreeInstances().empty();
}

template <class TSpareInstances>
std::string TSpareInstanceAllocator<TSpareInstances>::Allocate(const std::string& zoneName, const std::string& dataCenterName, const std::string& bundleName)
{
    YT_VERIFY(HasInstances(zoneName, dataCenterName));

    auto& info = GetOrCrash(GetOrCrash(SpareInstances, zoneName), dataCenterName);
    auto spareInstanceName = info.FreeInstances().back();
    info.MutableFreeInstances().pop_back();
    info.UsedByBundle[bundleName].push_back(spareInstanceName);

    return spareInstanceName;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
