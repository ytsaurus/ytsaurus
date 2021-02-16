#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf SystemLoggingCategoryName = "Logging";
constexpr TStringBuf DefaultStderrWriterName = "Stderr";
constexpr ELogLevel DefaultStderrMinLevel = ELogLevel::Info;
constexpr ELogLevel DefaultStderrQuietLevel = ELogLevel::Error;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
