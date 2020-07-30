#pragma once

#include "public.h"

#include <yt/core/profiling/profiler.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf SystemLoggingCategoryName = AsStringBuf("Logging");
constexpr TStringBuf DefaultStderrWriterName = AsStringBuf("Stderr");
constexpr ELogLevel DefaultStderrMinLevel = ELogLevel::Info;
constexpr ELogLevel DefaultStderrQuietLevel = ELogLevel::Error;

extern const NProfiling::TProfiler LoggingProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
