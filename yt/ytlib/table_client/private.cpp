#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger TableReaderLogger("TableReader");
NLog::TLogger TableWriterLogger("TableWriter");

int DefaultPartitionTag = -1;
int FormatVersion = 1;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

