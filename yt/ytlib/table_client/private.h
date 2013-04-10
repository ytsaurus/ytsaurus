#pragma once

#include <ytlib/logging/log.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger TableReaderLogger;
extern NLog::TLogger TableWriterLogger;

extern const int DefaultPartitionTag;
extern const int FormatVersion;

extern const int MaxPrefetchWindow;

//! Estimated memory overhead per chunk reader.
extern const i64 ChunkReaderMemorySize;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

