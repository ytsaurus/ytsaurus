#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/public.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! Patch #spec inplace and merge the controller options patches into *optionsPatch.
void ApplyExperiments(
    const NYTree::IMapNodePtr& spec,
    EOperationType type,
    const std::vector<NScheduler::TExperimentAssignmentPtr>& experimentAssignments,
    NYTree::INodePtr* optionsPatch);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
