#pragma once

#include "public.h"

#include <util/generic/hash_set.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

void EnsureResourceInstances(const TFlowViewPtr& flowView);
void ProjectResourceInstanceStatuses(const TFlowViewPtr& flowView);

THashSet<std::string> CollectWorkersWaitingForStartupResources(const TFlowViewPtr& flowView);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
