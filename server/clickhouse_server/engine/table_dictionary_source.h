#pragma once

#include <yt/server/clickhouse_server/native/public.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

void RegisterTableDictionarySource(
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr authToken);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
