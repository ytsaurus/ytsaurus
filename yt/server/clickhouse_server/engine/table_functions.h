#pragma once

#include "clickhouse.h"

#include <yt/server/clickhouse_server/native/public.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctionsExt(NNative::IStoragePtr storage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
