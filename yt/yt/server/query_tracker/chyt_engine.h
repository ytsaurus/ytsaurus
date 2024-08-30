#pragma once

#include "private.h"
#include "profiler.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateChytEngine(const NApi::IClientPtr& stateClient, const NYPath::TYPath& stateRoot, const TStateTimeProfilingCountersMapPtr& stateTimeProfilingCountersMap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
