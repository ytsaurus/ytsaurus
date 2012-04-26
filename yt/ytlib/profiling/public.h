#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TProfiler;
class TProfilingManager;

class TResourceTracker;

struct TTimer;
struct TQueuedSample;

//! Generic value for samples. 
typedef i64 TValue;

typedef ui64 TCpuInstant;
typedef i64  TCpuDuration;

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
