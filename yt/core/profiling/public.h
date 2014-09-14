#pragma once

#include <core/misc/common.h>
#include <core/misc/small_vector.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
