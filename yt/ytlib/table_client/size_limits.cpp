#include "stdafx.h"
#include "size_limits.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

const size_t MaxKeySize = 4 * 1024;
const size_t MaxColumnNameSize = 256;
const int MaxColumnCount = 1024;
const i64 MaxRowWeight = 16 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
