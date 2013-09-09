#pragma once

#include <core/logging/log.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger TableReaderLogger;
extern NLog::TLogger TableWriterLogger;

extern const int FormatVersion;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

