#pragma once

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using TCpuInstant = i64;
using TCpuDuration = i64;

//! Returns the current processor clock (rdtsc).
TCpuInstant GetCpuInstant();

//! Returns the current time (obtained via #GetCpuInstant).
TInstant GetInstant();

//! Converts a number of processor ticks into a regular duration.
TDuration CpuDurationToDuration(TCpuDuration cpuDuration);

//! Converts a regular duration into the number of processor ticks.
TCpuDuration DurationToCpuDuration(TDuration duration);

//! Converts a processor clock into the regular time instant.
TInstant CpuInstantToInstant(TCpuInstant cpuInstant);

//! Converts a regular time instant into the processor clock.
TCpuInstant InstantToCpuInstant(TInstant instant);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
