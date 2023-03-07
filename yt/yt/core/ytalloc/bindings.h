#pragma once

#include <util/generic/string.h>

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

// Installs YTAlloc log handler that pushes event to YT logging infrastructure.
void EnableYTLogging();

// Enables peroidic push of YTAlloc statistics to YT profiling infrastructure.
void EnableYTProfiling();

// Installs backtrace provider that invokes libunwind.
void SetLibunwindBacktraceProvider();

// Configures YTAlloc from YT_ALLOC_CONFIG environment variable.
void ConfigureFromEnv();

// Builds a string containing some brief allocation statistics.
TString FormatAllocationCounters();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc
