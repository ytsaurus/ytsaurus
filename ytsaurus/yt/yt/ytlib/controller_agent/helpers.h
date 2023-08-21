#pragma once

#include "public.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

bool IsFinishedState(EControllerState state);

// Used in node and client.
NYson::TYsonString BuildBriefStatistics(const NYTree::INodePtr& statistics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
