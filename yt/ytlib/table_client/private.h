#pragma once

#include <ytlib/logging/log.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger TableReaderLogger;
extern NLog::TLogger TableWriterLogger;

extern int DefaultPartitionTag;
extern int FormatVersion;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

