#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ILogWriter)

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf SystemLoggingCategoryName = "Logging";
constexpr TStringBuf DefaultStderrWriterName = "Stderr";
constexpr ELogLevel DefaultStderrMinLevel = ELogLevel::Info;
constexpr ELogLevel DefaultStderrQuietLevel = ELogLevel::Error;

extern const NProfiling::TRegistry LoggingProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
