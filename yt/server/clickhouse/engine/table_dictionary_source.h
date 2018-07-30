#pragma once

#include <yt/server/clickhouse/interop/api.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

void RegisterTableDictionarySource(
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr authToken);

} // namespace NClickHouse
} // namespace NYT
