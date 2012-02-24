#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TProfiler;
class TProfilingManager;

struct TTimer;
struct TQueuedSample;

//! Generic value for samples. 
typedef i64 TValue;

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT