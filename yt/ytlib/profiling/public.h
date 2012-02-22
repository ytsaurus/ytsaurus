#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TProfiler;
class TProfilingManager;

struct TQueuedSample;

//! Generic value for samples. 
typedef i64 TValue;

//! Number of CPU cycles as returned by, e.g., #GetCycleCount.
typedef ui64 TCpuClock;

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT