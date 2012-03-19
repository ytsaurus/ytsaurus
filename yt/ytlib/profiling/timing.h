#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! Returns the processor clock (rdtsc).
TCpuInstant GetCpuInstant();

//! Converts a number of processor ticks into a regular duration.
TDuration CpuDurationToDuration(TCpuDuration duration);

//! Converts a regular duration into the number of processor ticks.
TCpuDuration DurationToCpuDuration(TDuration duration);

//! Converts a processor clock into the regular time instant.
TInstant CpuInstantToInstant(TCpuInstant instant);

//! Converts a duration to TValue suitable for profiling.
/*!
 *  The current implementation just returns microseconds.
 */
TValue DurationToValue(TDuration duration);

//! Converts a CPU duration into TValue suitable for profiling.
TValue CpuDurationToValue(TCpuDuration duration);

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT

