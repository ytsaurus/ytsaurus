#pragma once

#include <core/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class ECategory>
class TMemoryUsageTracker;

template <class ECategory>
class TMemoryUsageTrackerGuard;

class TServerConfig;
typedef TIntrusivePtr<TServerConfig> TServerConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
