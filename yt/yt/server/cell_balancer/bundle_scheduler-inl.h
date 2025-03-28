#ifndef BUNDLE_SCHEDULER_INL_H_
#error "Direct inclusion of this file is not allowed, include bundle_scheduler.h"
// For the sake of sane code completion.
#include "bundle_scheduler.h"
#endif

namespace NYT::NCellBalancer {

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
