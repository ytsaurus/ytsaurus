#pragma once

#include "private.h"
#include "profiler.h"

#include <yt/yt/client/api/public.h>


namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateSpytEngine(NApi::IClientPtr stateClient, NYPath::TYPath stateRoot, const TStateTimeProfilingCountersMapPtr& stateTimeProfilingCountersMap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
