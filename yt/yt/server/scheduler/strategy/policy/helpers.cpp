#include "helpers.h"

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

THashSet<int> GetDiskQuotaMedia(const TDiskQuota& diskQuota)
{
    THashSet<int> media;
    for (const auto& [index, _] : diskQuota.DiskSpacePerMedium) {
        media.insert(index);
    }
    return media;
}

std::optional<bool> IsAggressivePreemptionAllowed(const TPoolTreeElement* element)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Root:
            return true;
        case ESchedulerElementType::Pool:
            return static_cast<const TPoolTreePoolElement*>(element)->GetConfig()->AllowAggressivePreemption;
        case ESchedulerElementType::Operation: {
            const auto* operationElement = static_cast<const TPoolTreeOperationElement*>(element);
            if (operationElement->IsGang() && !operationElement->TreeConfig()->AllowAggressivePreemptionForGangOperations) {
                return false;
            }
            return {};
        }
    }
}

std::optional<bool> IsPrioritySchedulingSegmentModuleAssignmentEnabled(const TPoolTreeElement* element)
{
    switch (element->GetType()) {
        case ESchedulerElementType::Root:
            return false;
        case ESchedulerElementType::Pool:
            return static_cast<const TPoolTreePoolElement*>(element)->GetConfig()->EnablePrioritySchedulingSegmentModuleAssignment;
        case ESchedulerElementType::Operation:
            YT_UNIMPLEMENTED();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
