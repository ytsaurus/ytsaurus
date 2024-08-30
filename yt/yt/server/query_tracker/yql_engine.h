#pragma once

#include "private.h"
#include "profiler.h"

#include "engine.h"

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateYqlEngine(const NApi::IClientPtr& stateClient, const NYPath::TYPath& stateRoot, const TStateTimeProfilingCountersMapPtr& stateTimeProfilingCountersMap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
