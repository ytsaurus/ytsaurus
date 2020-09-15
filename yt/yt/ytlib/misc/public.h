#pragma once

#include <yt/client/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class ECategory, class TPoolTag = TString>
class TMemoryUsageTracker;

template <class ECategory, class TPoolTag = TString>
class TMemoryUsageTrackerGuard;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
