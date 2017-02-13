#pragma once

#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TProfiler;
class TProfileManager;

class TResourceTracker;

struct TTimer;
struct TQueuedSample;

//! Generic value for samples.
typedef i64 TValue;

typedef ui64 TCpuInstant;
typedef i64  TCpuDuration;

typedef int TTagId;

const int TypicalTagCount = 8;
typedef SmallVector<TTagId, TypicalTagCount> TTagIdList;

extern const TTagIdList EmptyTagIds;

//! Enumeration of metric types.
/*
 *  - Counter: A counter is a cumulative metric that represents a single numerical
 *  value that only ever goes up. A counter is typically used to count requests served,
 *  tasks completed, errors occurred, etc.. Counters should not be used to expose current
 *  counts of items whose number can also go down, e.g. the number of currently running
 *  goroutines. Use gauges for this use case.
 *
 *  - Gauge: A gauge is a metric that represents a single numerical value that can
 *  arbitrarily go up and down. Gauges are typically used for measured values like
 *  temperatures or current memory usage, but also "counts" that can go up and down,
 *  like the number of running goroutines.
 */
DEFINE_ENUM(EMetricType,
    (Counter)
    (Gauge)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
