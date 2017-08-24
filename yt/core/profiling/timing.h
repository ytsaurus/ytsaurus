#pragma once

#include "public.h"

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! Returns the current processor clock (rdtsc).
TCpuInstant GetCpuInstant();

//! Returns the current time (obtained via #GetCpuInstant).
TInstant GetInstant();

//! Converts a number of processor ticks into a regular duration.
TDuration CpuDurationToDuration(TCpuDuration duration);

//! Converts a regular duration into the number of processor ticks.
TCpuDuration DurationToCpuDuration(TDuration duration);

//! Converts a processor clock into the regular time instant.
TInstant CpuInstantToInstant(TCpuInstant instant);

//! Converts a regular time instant into the processor clock.
TCpuInstant InstantToCpuInstant(TInstant instant);

//! Converts a duration to TValue suitable for profiling.
/*!
 *  The current implementation just returns microseconds.
 */
TValue DurationToValue(TDuration duration);

//! Converts a TValue to duration.
/*!
 *  The current implementation assumes that #value is given in microseconds.
 */
TDuration ValueToDuration(TValue value);

//! Converts a CPU duration into TValue suitable for profiling.
TValue CpuDurationToValue(TCpuDuration duration);

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT

#define TIMING_INL_H_
#include "timing-inl.h"
#undef TIMING_INL_H_
