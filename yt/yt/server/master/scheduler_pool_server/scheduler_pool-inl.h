#ifndef SCHEDULER_POOL_INL_H_
#error "Direct inclusion of this file is not allowed, include action.h"
// For the sake of sane code completion.
#include "scheduler_pool.h"
#endif

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

template <class TResource>
void TSchedulerPool::ValidateChildrenGuaranteeSum(
    const char* guaranteeName,
    NScheduler::EJobResourceType resourceType,
    std::function<std::optional<TResource>(const NScheduler::TPoolConfigPtr&)> getResource)
{
    auto parentResource = getResource(this->FullConfig());
    if (!parentResource || !this->FullConfig()->AllowChildrenGuarantees) {
        for (auto it : GetSortedIterators(KeyToChild_)) {
            auto child = it->second;
            if (auto childResource = getResource(child->FullConfig())) {
                if (!parentResource) {
                    THROW_ERROR_EXCEPTION(
                        "%v is explicitly configured at child pool %Qv but is not configured at parent %Qv",
                        guaranteeName,
                        child->GetName(),
                        GetName())
                        .With("pool_name", child->GetName())
                        .With("parent_name", GetName())
                        .With("resource_type", resourceType)
                        .With("resource_guarantee", *childResource);
                } else {
                    THROW_ERROR_EXCEPTION(
                        "%v is explicitly configured at child pool %Qv but parent %Qv does not allow children guarantees",
                        guaranteeName,
                        child->GetName(),
                        GetName())
                        .With("pool_name", child->GetName())
                        .With("parent_name", GetName())
                        .With("resource_type", resourceType)
                        .With("resource_guarantee", *childResource);
                }
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
            .With("resource_type", resourceType)
            .With("pool_name", GetName())
            .With("parent_resource", *parentResource)
            .With("children_resource_sum", childrenResourceSum);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
