#pragma once

#include <yt/server/clickhouse_server/native/public.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctionsExt(NNative::IStoragePtr storage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
