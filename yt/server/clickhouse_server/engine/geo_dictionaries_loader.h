#pragma once

#include <yt/server/clickhouse_server/native/public.h>

#include <Dictionaries/Embedded/IGeoDictionariesLoader.h>

#include <memory>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IGeoDictionariesLoader> CreateGeoDictionariesLoader(
    NNative::IStoragePtr storage,
    NNative::IAuthorizationTokenPtr authToken,
    const std::string& geodataPath);

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
