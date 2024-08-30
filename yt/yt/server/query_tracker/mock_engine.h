#pragma once

#include "engine.h"
#include "profiler.h"

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateMockEngine(const NApi::IClientPtr& stateClient, const NYPath::TYPath& stateRoot, const TStateTimeProfilingCountersMapPtr& stateTimeProfilingCountersMap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
