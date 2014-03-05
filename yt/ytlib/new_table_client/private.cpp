#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

const int FormatVersion = 2;

NLog::TLogger TableReaderLogger("TableReader");
NLog::TLogger TableWriterLogger("TableWriter");

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
