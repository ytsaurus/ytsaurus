#ifndef SCHEDULER_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include action.h"
// For the sake of sane code completion.
#include "scheduler_pool.h"
#endif

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

template<class TResource>
void TSchedulerPool::ValidateChildrenGuaranteeSum(
    const char* guaranteeName,
    NScheduler::EJobResourceType resourceType,
    std::function<std::optional<TResource>(const NScheduler::TPoolConfigPtr&)> getResource)
{
    auto parentResource = getResource(this->FullConfig());
    if (!parentResource) {
        for (const auto& [_, child] : KeyToChild_) {
            if (auto childResource = getResource(child->FullConfig())) {
                THROW_ERROR_EXCEPTION(
                    "%v is explicitly configured at child pool %Qv but is not configured at parent %Qv",
                    guaranteeName,
                    child->GetName(),
                    GetName())
                    << TErrorAttribute("pool_name", child->GetName())
                    << TErrorAttribute("parent_name", GetName())
                    << TErrorAttribute("resource_type", resourceType)
                    << TErrorAttribute("resource_guarantee", *childResource);
            }
        }
        return;
    }

    TResource childrenResourceSum = 0;
    for (const auto& [_, child] : KeyToChild_) {
        childrenResourceSum += getResource(child->FullConfig()).value_or(0);
    }

    if (*parentResource < childrenResourceSum) {
        THROW_ERROR_EXCEPTION("%v of resource for pool %Qv is less than the sum of children guarantees", guaranteeName, GetName())
            << TErrorAttribute("resource_type", resourceType)
            << TErrorAttribute("pool_name", GetName())
            << TErrorAttribute("parent_resource", *parentResource)
            << TErrorAttribute("children_resource_sum", childrenResourceSum);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
