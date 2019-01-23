#pragma once

#include <yt/server/clickhouse_server/public.h>

#include <Dictionaries/Embedded/IGeoDictionariesLoader.h>

#include <memory>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IGeoDictionariesLoader> CreateGeoDictionariesLoader(
    IStoragePtr storage,
    IAuthorizationTokenPtr authToken,
    const std::string& geodataPath);

} // namespace NYT::NClickHouseServer
