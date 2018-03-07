#pragma once

#include "public.h"

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

extern const char* const SystemLoggingCategoryName;
extern const char* const DefaultStderrWriterName;
extern const ELogLevel DefaultStderrMinLevel;
extern const ELogLevel DefaultStderrQuietLevel;
extern const NProfiling::TProfiler LoggingProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
