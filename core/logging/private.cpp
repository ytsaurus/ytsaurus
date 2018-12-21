#include "private.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

const char* const SystemLoggingCategoryName = "Logging";
const char* const DefaultStderrWriterName = "Stderr";
const ELogLevel DefaultStderrMinLevel = ELogLevel::Info;
const ELogLevel DefaultStderrQuietLevel = ELogLevel::Error;
const NProfiling::TProfiler LoggingProfiler("/logging");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
