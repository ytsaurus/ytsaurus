#pragma once

#include "clickhouse.h"

#include <yt/server/clickhouse_server/native/public.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

void RegisterTableDictionarySource(
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr authToken);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
