#pragma once

#include "public.h"

#include <yt/server/clickhouse_server/interop/api.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

NInterop::IStoragePtr CreateStorage(
    NApi::NNative::IConnectionPtr connection,
    INativeClientCachePtr clientCache,
    NConcurrency::IThroughputThrottlerPtr scanThrottler);

}   // namespace NClickHouse
}   // namespace NYT
