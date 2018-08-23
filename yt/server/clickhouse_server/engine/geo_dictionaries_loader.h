#pragma once

#include <yt/server/clickhouse_server/interop/api.h>

#include <Dictionaries/Embedded/IGeoDictionariesLoader.h>

#include <memory>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IGeoDictionariesLoader> CreateGeoDictionariesLoader(
    NInterop::IStoragePtr storage,
    NInterop::IAuthorizationTokenPtr authToken,
    const std::string& geodataPath);

} // namespace NClickHouse
} // namespace NYT
