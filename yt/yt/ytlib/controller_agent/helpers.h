#pragma once

#include "public.h"

#include <yt/core/yson/string.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

bool IsFinishedState(EControllerState state);

NYson::TYsonString BuildBriefStatistics(const NYTree::INodePtr& statistics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
