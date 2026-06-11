#pragma once

#include <yt/yt/server/scheduler/strategy/pool_tree_element.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

namespace NYT::NScheduler::NStrategy::NPolicy {

////////////////////////////////////////////////////////////////////////////////

THashSet<int> GetDiskQuotaMedia(const TDiskQuota& diskQuota);

std::optional<bool> IsAggressivePreemptionAllowed(const TPoolTreeElement* element);

std::optional<bool> IsPrioritySchedulingSegmentModuleAssignmentEnabled(const TPoolTreeElement* element);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy
