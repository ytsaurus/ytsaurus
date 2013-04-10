#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger TableReaderLogger("TableReader");
NLog::TLogger TableWriterLogger("TableWriter");

const int DefaultPartitionTag = -1;
const int FormatVersion = 1;

const int MaxPrefetchWindow = 250;

const i64 ChunkReaderMemorySize = (i64) 16 * 1024;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

