#include "private.h"

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

const char* const SystemLoggingCategoryName = "Logging";
const char* const DefaultStderrWriterName = "Stderr";
const ELogLevel DefaultStderrMinLevel = ELogLevel::Info;
const ELogLevel DefaultStderrQuietLevel = ELogLevel::Error;
const NProfiling::TProfiler LoggingProfiler("/logging");

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
