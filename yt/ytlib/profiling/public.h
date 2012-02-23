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

struct TTimer;

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT