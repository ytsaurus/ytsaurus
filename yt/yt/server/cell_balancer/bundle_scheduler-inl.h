#ifndef BUNDLE_SCHEDULER_INL_H_
#error "Direct inclusion of this file is not allowed, include bundle_scheduler.h"
// For the sake of sane code completion.
#include "bundle_scheduler.h"
#endif

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

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

template <typename TSpareInstances>
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

template <class T>
TBundleMutation<T> TSchedulerMutations::WrapMutation(T mutation)
{
    YT_VERIFY(!BundleNameContext_.empty());
    return TBundleMutation<T>{BundleNameContext_, std::move(mutation)};
}

template <class T, class... Args>
    requires std::derived_from<T, TBundleNameMixin>
TIntrusivePtr<T> TSchedulerMutations::NewMutation(Args&&... args)
{
    auto value = New<T>(std::forward<Args>(args)...);
    YT_VERIFY(!BundleNameContext_.empty());
    value->BundleName = BundleNameContext_;
    return value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
